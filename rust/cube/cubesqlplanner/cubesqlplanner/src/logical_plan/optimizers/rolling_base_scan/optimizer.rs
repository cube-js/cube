use super::same_rows::reads_same_rows;
use crate::logical_plan::*;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

/// Merges the base scans of rolling windows that read the same rows.
///
/// A rolling window's base CTE aggregates one measure over the rows its frame
/// can reach, and which rows those are is decided by the frame and the query's
/// filters — not by the measure. Windows differing only in the column they
/// aggregate therefore scan the fact table once each where one scan would do,
/// and the cost of that shows up as (entities × window × anchors) once a
/// high-cardinality dimension is grouped by.
///
/// Runs after pre-aggregations have been matched, which is what makes the
/// merge safe to take unconditionally: a rollup answers only for a query whose
/// every measure it carries, so a scan carrying two measures could not be
/// served by a rollup holding one of them. By this point each base scan either
/// already reads a rollup — a source this declines to merge — or was left on
/// the fact table, where nothing is lost by sharing it.
pub struct RollingBaseScanOptimizer;

/// One base scan and the measures that will ride on it.
struct MergeGroup {
    /// Position of the kept CTE in the plan's CTE list.
    position: usize,
    leaf: Rc<MultiStageLeafMeasure>,
    measures: Vec<Rc<MemberSymbol>>,
    /// Names of the CTEs folded into this one.
    absorbed: Vec<String>,
}

impl RollingBaseScanOptimizer {
    pub fn new() -> Self {
        Self
    }

    pub fn optimize(&self, root: Rc<RootQuery>) -> Result<Rc<RootQuery>, CubeError> {
        let groups = self.group_shareable_scans(&root);
        if groups.iter().all(|group| group.absorbed.is_empty()) {
            return Ok(root);
        }

        let mut merged_at: HashMap<usize, &MergeGroup> = HashMap::new();
        let mut renames: HashMap<String, String> = HashMap::new();
        for group in groups.iter() {
            if group.absorbed.is_empty() {
                continue;
            }
            let kept = root.ctes()[group.position].name.clone();
            merged_at.insert(group.position, group);
            for absorbed in group.absorbed.iter() {
                renames.insert(absorbed.clone(), kept.clone());
            }
        }

        let ctes = root
            .ctes()
            .iter()
            .enumerate()
            .filter(|(_, cte)| !renames.contains_key(&cte.name))
            .map(|(position, cte)| match merged_at.get(&position) {
                Some(group) => Self::widen_leaf(cte, group),
                None => Ok(Self::rename_measure_input(cte, &renames)),
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Rc::new(
            RootQuery::builder()
                .ctes(ctes)
                .query(root.query().clone())
                .build(),
        ))
    }

    /// Base scans that may share a CTE, each with the measures it will carry.
    /// Every group holds at least its own measure; the ones that absorbed none
    /// are left alone.
    fn group_shareable_scans(&self, root: &Rc<RootQuery>) -> Vec<MergeGroup> {
        let consumers = CteConsumers::collect(root);
        let mut groups: Vec<MergeGroup> = Vec::new();

        for (position, cte) in root.ctes().iter().enumerate() {
            let MultiStageMemberLogicalType::LeafMeasure(leaf) = &cte.member_type else {
                continue;
            };
            if !consumers.is_sole_input_of_a_window(&cte.name) {
                continue;
            }
            match groups
                .iter_mut()
                .find(|group| reads_same_rows(&group.leaf, leaf))
            {
                Some(group) => {
                    // A measure two scans have in common — a switch dispatching
                    // several stages onto one — is one column of the merged
                    // scan, not two under the same alias.
                    for measure in leaf.measures.iter() {
                        if !group
                            .measures
                            .iter()
                            .any(|carried| carried.full_name() == measure.full_name())
                        {
                            group.measures.push(measure.clone());
                        }
                    }
                    group.absorbed.push(cte.name.clone());
                }
                None => groups.push(MergeGroup {
                    position,
                    leaf: leaf.clone(),
                    measures: leaf.measures.clone(),
                    absorbed: Vec::new(),
                }),
            }
        }

        groups
    }

    /// The kept CTE, projecting every measure of its group. Each consumer
    /// still reads only the column its own reference names.
    fn widen_leaf(
        cte: &Rc<LogicalMultiStageMember>,
        group: &MergeGroup,
    ) -> Result<Rc<LogicalMultiStageMember>, CubeError> {
        let query = &group.leaf.query;
        let schema = LogicalSchema {
            time_dimensions: query.schema().time_dimensions.clone(),
            dimensions: query.schema().dimensions.clone(),
            measures: group.measures.clone(),
        };
        let widened = Query::builder()
            .schema(schema.into_rc())
            .filter(query.filter().clone())
            .modifers(query.modifers().clone())
            .source(query.source().clone())
            .build();

        Ok(Rc::new(LogicalMultiStageMember {
            name: cte.name.clone(),
            member_type: MultiStageMemberLogicalType::LeafMeasure(Rc::new(MultiStageLeafMeasure {
                measures: group.measures.clone(),
                evaluation_context: group.leaf.evaluation_context.clone(),
                query: Rc::new(widened),
            })),
        }))
    }

    /// Points a rolling window at the scan its own was folded into.
    fn rename_measure_input(
        cte: &Rc<LogicalMultiStageMember>,
        renames: &HashMap<String, String>,
    ) -> Rc<LogicalMultiStageMember> {
        let MultiStageMemberLogicalType::RollingWindow(window) = &cte.member_type else {
            return cte.clone();
        };
        let Some(kept) = renames.get(window.measure_input.name()) else {
            return cte.clone();
        };

        let measure_input = MultiStageSubqueryRef::builder()
            .name(kept.clone())
            .symbols(window.measure_input.symbols().clone())
            .schema(window.measure_input.schema().clone())
            .build();

        Rc::new(LogicalMultiStageMember {
            name: cte.name.clone(),
            member_type: MultiStageMemberLogicalType::RollingWindow(Rc::new(
                MultiStageRollingWindow {
                    schema: window.schema.clone(),
                    is_ungrouped: window.is_ungrouped,
                    rolling_time_dimension: window.rolling_time_dimension.clone(),
                    rolling_window: window.rolling_window.clone(),
                    order_by: window.order_by.clone(),
                    time_series_input: window.time_series_input.clone(),
                    measure_input,
                    time_dimension_in_measure_input: window.time_dimension_in_measure_input.clone(),
                },
            )),
        })
    }
}

/// Who reads each CTE of a plan.
struct CteConsumers {
    /// How many references the whole plan makes to each CTE name.
    reference_counts: HashMap<String, usize>,
    /// Names a rolling window reads its measure off.
    window_measure_inputs: Vec<String>,
}

impl CteConsumers {
    fn collect(root: &Rc<RootQuery>) -> Self {
        let mut result = Self {
            reference_counts: HashMap::new(),
            window_measure_inputs: Vec::new(),
        };
        result.walk(&root.as_plan_node());
        result
    }

    fn walk(&mut self, node: &PlanNode) {
        for name in node.referenced_cte_names() {
            *self.reference_counts.entry(name).or_insert(0) += 1;
        }
        if let PlanNode::MultiStageRollingWindow(window) = node {
            self.window_measure_inputs
                .push(window.measure_input.name().clone());
        }
        for input in node.inputs() {
            self.walk(&input);
        }
    }

    /// Whether the only thing reading this CTE is one rolling window, taking
    /// it as the measure side of its frame.
    ///
    /// This is what makes dropping the absorbed CTE safe. Only a rolling
    /// window's `measure_input` is repointed at the scan it was folded into —
    /// the root query and every other member are carried through verbatim — so
    /// a reference of any other kind would survive into the SQL naming a CTE
    /// that is no longer in the `WITH` list.
    ///
    /// Counting those references relies on `referenced_cte_names`, which every
    /// node linking a CTE by name has to declare. A member joined in by name
    /// through the builder context instead would not be counted; today that
    /// channel carries dimension-calculation CTEs only, and those are never
    /// candidates here.
    fn is_sole_input_of_a_window(&self, name: &str) -> bool {
        self.reference_counts.get(name) == Some(&1)
            && self.window_measure_inputs.iter().any(|input| input == name)
    }
}
