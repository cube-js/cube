use super::super::rolling_base_scan::same_members;
use crate::logical_plan::*;
use crate::planner::query_properties::member_chain_eq;
use crate::planner::{InputId, MemberSymbol};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// Fuses multi-stage measure calculations that compute over the same inputs at
/// the same grain into one calculation of all their measures. A calculation
/// read by several others is otherwise evaluated once per reader by engines
/// that inline CTEs.
pub struct StageCalculationFusionOptimizer;

struct FusionGroup {
    position: usize,
    calculation: Rc<MultiStageMeasureCalculation>,
    measures: Vec<Rc<MemberSymbol>>,
    absorbed: Vec<InputId>,
}

impl StageCalculationFusionOptimizer {
    pub fn new() -> Self {
        Self
    }

    pub fn optimize(&self, root: Rc<RootQuery>) -> Result<Rc<RootQuery>, CubeError> {
        let mut root = root;
        // Fusing one level gives the calculations reading it identical inputs.
        while let Some(fused) = Self::fuse_once(&root)? {
            root = fused;
        }
        Ok(root)
    }

    fn fuse_once(root: &Rc<RootQuery>) -> Result<Option<Rc<RootQuery>>, CubeError> {
        let readers = MemberReaders::collect(root);
        let grids: HashMap<InputId, Rc<KeysGrid>> = root
            .subqueries()
            .iter()
            .filter(|subquery| subquery.inline)
            .filter_map(|subquery| match &subquery.body {
                SubqueryBody::KeysGrid(grid) => Some((subquery.id, grid.clone())),
                _ => None,
            })
            .collect();

        let mut groups: Vec<FusionGroup> = Vec::new();
        for (position, subquery) in root.subqueries().iter().enumerate() {
            let SubqueryBody::MultiStageMeasureCalculation(calculation) = &subquery.body else {
                continue;
            };
            if subquery.inline
                || !readers.read_only_by_member(subquery.id)
                || !is_fusable(calculation)
            {
                continue;
            }
            match groups
                .iter_mut()
                .find(|group| same_calculation(&group.calculation, calculation, &grids))
            {
                Some(group) => {
                    for measure in calculation.schema().measures.iter() {
                        if !group.measures.iter().any(|m| member_chain_eq(m, measure)) {
                            group.measures.push(measure.clone());
                        }
                    }
                    group.absorbed.push(subquery.id);
                }
                None => groups.push(FusionGroup {
                    position,
                    calculation: calculation.clone(),
                    measures: calculation.schema().measures.clone(),
                    absorbed: Vec::new(),
                }),
            }
        }
        if groups.iter().all(|group| group.absorbed.is_empty()) {
            return Ok(None);
        }

        let mut fused_at: HashMap<usize, Rc<Subquery>> = HashMap::new();
        let mut forwards = InputForwards::default();
        let mut absorbed: HashSet<InputId> = HashSet::new();
        for group in groups.iter().filter(|group| !group.absorbed.is_empty()) {
            let kept = &root.subqueries()[group.position];
            let fused = fused_calculation(group);
            let to = Rc::new(
                SubqueryRef::builder()
                    .id(kept.id)
                    .name(kept.name.clone())
                    .schema(fused.schema().clone())
                    .build(),
            );
            for id in group.absorbed.iter() {
                forwards.add(*id, &to);
                absorbed.insert(*id);
            }
            fused_at.insert(
                group.position,
                kept.with_body(SubqueryBody::MultiStageMeasureCalculation(fused)),
            );
        }

        let subqueries = root
            .subqueries()
            .iter()
            .enumerate()
            .filter(|(_, subquery)| !absorbed.contains(&subquery.id))
            .map(|(position, subquery)| {
                fused_at
                    .get(&position)
                    .cloned()
                    .unwrap_or_else(|| subquery.clone())
            })
            .collect_vec();
        let result = Rc::new(
            RootQuery::builder()
                .subqueries(subqueries)
                .query(root.query().clone())
                .build(),
        );
        Ok(Some(result.forward_all(&forwards)?))
    }
}

// A window's partition and a rank's order are taken per calculation, so only
// plain calculations evaluate each measure on its own.
fn is_fusable(calculation: &MultiStageMeasureCalculation) -> bool {
    *calculation.calculation_type() != MultiStageCalculationType::Rank
        && *calculation.window_function_to_use() == MultiStageCalculationWindowFunction::None
}

fn same_calculation(
    a: &MultiStageMeasureCalculation,
    b: &MultiStageMeasureCalculation,
    grids: &HashMap<InputId, Rc<KeysGrid>>,
) -> bool {
    a.calculation_type() == b.calculation_type()
        && a.window_function_to_use() == b.window_function_to_use()
        && a.is_ungrouped() == b.is_ungrouped()
        && same_members(a.partition_by(), b.partition_by())
        && same_members(&a.schema().dimensions, &b.schema().dimensions)
        && same_members(&a.schema().time_dimensions, &b.schema().time_dimensions)
        && same_source(a.source(), b.source(), grids)
}

fn same_source(
    a: &CalculationSource,
    b: &CalculationSource,
    grids: &HashMap<InputId, Rc<KeysGrid>>,
) -> bool {
    match (a, b) {
        (CalculationSource::Merge(a), CalculationSource::Merge(b)) => {
            same_members(&a.schema().dimensions, &b.schema().dimensions)
                && same_members(&a.schema().time_dimensions, &b.schema().time_dimensions)
                && sorted_ids(a.subqueries()) == sorted_ids(b.subqueries())
        }
        (CalculationSource::GridLookup(a), CalculationSource::GridLookup(b)) => {
            same_grid(a.grid(), b.grid(), grids) && same_legs(a.legs(), b.legs())
        }
        _ => false,
    }
}

// Each calculation plans an inline grid of its own, so grids are compared by
// what they enumerate.
fn same_grid(
    a: &Rc<SubqueryRef>,
    b: &Rc<SubqueryRef>,
    grids: &HashMap<InputId, Rc<KeysGrid>>,
) -> bool {
    if a.id() == b.id() {
        return true;
    }
    let (Some(a_grid), Some(b_grid)) = (grids.get(&a.id()), grids.get(&b.id())) else {
        return false;
    };
    same_members(&a_grid.schema().dimensions, &b_grid.schema().dimensions)
        && same_members(
            &a_grid.schema().time_dimensions,
            &b_grid.schema().time_dimensions,
        )
        && sorted_ids(a_grid.sources()) == sorted_ids(b_grid.sources())
}

fn same_legs(a: &[LookupLeg], b: &[LookupLeg]) -> bool {
    let sorted = |legs: &[LookupLeg]| {
        legs.iter()
            .sorted_by_key(|leg| leg.subquery().id())
            .cloned()
            .collect_vec()
    };
    let (a, b) = (sorted(a), sorted(b));
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .all(|(x, y)| x.subquery().id() == y.subquery().id() && same_members(x.on(), y.on()))
}

fn sorted_ids(refs: &[Rc<SubqueryRef>]) -> Vec<InputId> {
    refs.iter().map(|r| r.id()).sorted().dedup().collect()
}

fn fused_calculation(group: &FusionGroup) -> Rc<MultiStageMeasureCalculation> {
    let kept = &group.calculation;
    let schema = LogicalSchema {
        time_dimensions: kept.schema().time_dimensions.clone(),
        dimensions: kept.schema().dimensions.clone(),
        measures: group.measures.clone(),
    }
    .into_rc();
    Rc::new(
        MultiStageMeasureCalculation::builder()
            .schema(schema)
            .is_ungrouped(kept.is_ungrouped())
            .calculation_type(kept.calculation_type().clone())
            .partition_by(kept.partition_by().clone())
            .window_function_to_use(kept.window_function_to_use().clone())
            .order_by(kept.order_by().clone())
            .source(kept.source().clone())
            .build(),
    )
}

/// Which subqueries are read only by nodes that pick their columns by member,
/// so a wider schema leaves what they read unchanged.
struct MemberReaders {
    all: HashMap<InputId, usize>,
    by_member: HashMap<InputId, usize>,
}

impl MemberReaders {
    fn collect(root: &Rc<RootQuery>) -> Self {
        let mut result = Self {
            all: HashMap::new(),
            by_member: HashMap::new(),
        };
        result.walk(&root.as_plan_node());
        result
    }

    fn walk(&mut self, node: &PlanNode) {
        let by_member = matches!(
            node,
            PlanNode::Merge(_) | PlanNode::GridLookup(_) | PlanNode::KeysGrid(_)
        );
        for id in node.referenced_inputs() {
            *self.all.entry(id).or_insert(0) += 1;
            if by_member {
                *self.by_member.entry(id).or_insert(0) += 1;
            }
        }
        for input in node.inputs() {
            self.walk(&input);
        }
    }

    fn read_only_by_member(&self, id: InputId) -> bool {
        self.all.get(&id).is_some_and(|all| *all > 0)
            && self.all.get(&id) == self.by_member.get(&id)
    }
}
