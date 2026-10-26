use super::same_rows::reads_same_rows;
use crate::logical_plan::*;
use crate::planner::query_properties::member_chain_eq;
use crate::planner::{InputId, MemberSymbol};
use std::collections::HashMap;
use std::rc::Rc;

/// Merges the base scans of rolling windows that read the same rows, so that
/// windows differing only in the column they aggregate share one scan of the
/// fact table and each reads its own column off it.
///
/// Must run after pre-aggregations are matched — see the call site, where that
/// ordering is what someone editing could move.
pub struct RollingBaseScanOptimizer;

/// One base scan and the measures that will ride on it.
struct MergeGroup {
    /// Position of the kept subquery in the plan's subquery list.
    position: usize,
    leaf: Rc<Query>,
    measures: Vec<Rc<MemberSymbol>>,
    /// Subqueries folded into this one.
    absorbed: Vec<InputId>,
}

impl RollingBaseScanOptimizer {
    pub fn new() -> Self {
        Self
    }

    pub fn optimize(&self, root: Rc<RootQuery>) -> Rc<RootQuery> {
        let groups = self.group_shareable_scans(&root);
        if groups.iter().all(|group| group.absorbed.is_empty()) {
            return root;
        }

        let mut merged_at: HashMap<usize, &MergeGroup> = HashMap::new();
        let mut renames: HashMap<InputId, &Rc<Subquery>> = HashMap::new();
        for group in groups.iter() {
            if group.absorbed.is_empty() {
                continue;
            }
            let kept = &root.subqueries()[group.position];
            merged_at.insert(group.position, group);
            for absorbed in group.absorbed.iter() {
                renames.insert(*absorbed, kept);
            }
        }

        let subqueries = root
            .subqueries()
            .iter()
            .enumerate()
            .filter(|(_, subquery)| !renames.contains_key(&subquery.id))
            .map(|(position, subquery)| match merged_at.get(&position) {
                Some(group) => Self::widen_leaf(subquery, group),
                None => Self::rename_measure_input(subquery, &renames),
            })
            .collect::<Vec<_>>();

        Rc::new(
            RootQuery::builder()
                .subqueries(subqueries)
                .query(root.query().clone())
                .build(),
        )
    }

    /// Base scans that may share a subquery, each with the measures it will carry.
    /// Every group holds at least its own measure; the ones that absorbed none
    /// are left alone.
    fn group_shareable_scans(&self, root: &Rc<RootQuery>) -> Vec<MergeGroup> {
        let consumers = SubqueryConsumers::collect(root);
        let mut groups: Vec<MergeGroup> = Vec::new();

        for (position, subquery) in root.subqueries().iter().enumerate() {
            let SubqueryBody::LeafMeasure(leaf) = &subquery.body else {
                continue;
            };
            if !consumers.is_sole_input_of_a_window(subquery.id) {
                continue;
            }
            // A leaf selecting a bare dimension grid — its member rendered by
            // the stage above rather than here — has no measure to share.
            if leaf.schema().measures.is_empty() {
                continue;
            }
            match groups
                .iter_mut()
                .find(|group| reads_same_rows(&group.leaf, leaf))
            {
                Some(group) => {
                    // A measure two scans have in common — a switch dispatching
                    // several stages onto one — is one column of the merged
                    // scan, not two under the same alias. Identity is the
                    // resolved reference chain, which is what the physical side
                    // matches a column by: two symbols resolving to it render
                    // the same column, and projecting both would leave each
                    // window reading whichever came first.
                    for measure in leaf.schema().measures.iter() {
                        if !group
                            .measures
                            .iter()
                            .any(|carried| member_chain_eq(carried, measure))
                        {
                            group.measures.push(measure.clone());
                        }
                    }
                    group.absorbed.push(subquery.id);
                }
                None => groups.push(MergeGroup {
                    position,
                    leaf: leaf.clone(),
                    measures: leaf.schema().measures.clone(),
                    absorbed: Vec::new(),
                }),
            }
        }

        groups
    }

    /// The kept subquery, projecting every measure of its group. Each consumer
    /// still reads only the column its own reference names.
    fn widen_leaf(subquery: &Rc<Subquery>, group: &MergeGroup) -> Rc<Subquery> {
        let query = &group.leaf;
        let schema = LogicalSchema {
            time_dimensions: query.schema().time_dimensions.clone(),
            dimensions: query.schema().dimensions.clone(),
            measures: group.measures.clone(),
        };
        let widened = query.with_schema_filter_and_modifers(
            schema.into_rc(),
            query.filter().clone(),
            query.modifers().clone(),
        );

        subquery.with_body(SubqueryBody::LeafMeasure(widened))
    }

    /// Points a rolling window at the scan its own was folded into.
    fn rename_measure_input(
        subquery: &Rc<Subquery>,
        renames: &HashMap<InputId, &Rc<Subquery>>,
    ) -> Rc<Subquery> {
        let SubqueryBody::RollingWindow(window) = &subquery.body else {
            return subquery.clone();
        };
        let Some(kept) = renames.get(&window.measure_input.id()) else {
            return subquery.clone();
        };

        let measure_input = SubqueryRef::builder()
            .id(kept.id)
            .name(kept.name.clone())
            .schema(window.measure_input.schema().clone())
            .build();

        subquery.with_body(SubqueryBody::RollingWindow(Rc::new(
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
        )))
    }
}

/// Who reads each subquery of a plan.
struct SubqueryConsumers {
    /// How many references the whole plan makes to each subquery.
    reference_counts: HashMap<InputId, usize>,
    /// Subqueries a rolling window reads its measure off.
    window_measure_inputs: Vec<InputId>,
}

impl SubqueryConsumers {
    fn collect(root: &Rc<RootQuery>) -> Self {
        let mut result = Self {
            reference_counts: HashMap::new(),
            window_measure_inputs: Vec::new(),
        };
        result.walk(&root.as_plan_node());
        result
    }

    fn walk(&mut self, node: &PlanNode) {
        for id in node.referenced_inputs() {
            *self.reference_counts.entry(id).or_insert(0) += 1;
        }
        if let PlanNode::MultiStageRollingWindow(window) = node {
            self.window_measure_inputs.push(window.measure_input.id());
        }
        for input in node.inputs() {
            self.walk(&input);
        }
    }

    /// Whether the only thing reading this subquery is one rolling window, taking
    /// it as the measure side of its frame.
    ///
    /// This is what makes dropping the absorbed subquery safe. Only a rolling
    /// window's `measure_input` is repointed at the scan it was folded into —
    /// the root query and every other member are carried through verbatim — so
    /// a reference of any other kind would survive into the SQL naming a subquery
    /// that is no longer in the `WITH` list.
    ///
    /// Counting those references relies on `referenced_inputs`, which every
    /// node linking a subquery has to declare.
    fn is_sole_input_of_a_window(&self, id: InputId) -> bool {
        self.reference_counts.get(&id) == Some(&1) && self.window_measure_inputs.contains(&id)
    }
}
