use crate::logical_plan::*;
use crate::planner::query_properties::member_chain_eq;
use crate::planner::{InputId, MemberId};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// How each subquery of a plan is read.
pub struct InputReaders {
    all: HashMap<InputId, usize>,
    by_stages: HashMap<InputId, usize>,
    by_member: HashMap<InputId, usize>,
}

impl InputReaders {
    pub fn collect(root: &Rc<RootQuery>) -> Self {
        let mut result = Self {
            all: HashMap::new(),
            by_stages: HashMap::new(),
            by_member: HashMap::new(),
        };
        result.walk(&root.as_plan_node(), false);
        result
    }

    fn walk(&mut self, node: &PlanNode, in_stage: bool) {
        let in_stage = in_stage
            || matches!(
                node,
                PlanNode::MultiStageMeasureCalculation(_) | PlanNode::KeysGrid(_)
            );
        let by_member = matches!(
            node,
            PlanNode::Merge(_) | PlanNode::GridLookup(_) | PlanNode::KeysGrid(_)
        );
        for id in node.referenced_inputs() {
            *self.all.entry(id).or_insert(0) += 1;
            if in_stage {
                *self.by_stages.entry(id).or_insert(0) += 1;
            }
            if by_member {
                *self.by_member.entry(id).or_insert(0) += 1;
            }
        }
        for input in node.inputs() {
            self.walk(&input, in_stage);
        }
    }

    /// Read, and only by multi-stage calculations and their grids.
    pub fn read_only_by_stages(&self, id: InputId) -> bool {
        self.is_read(id) && self.all.get(&id) == self.by_stages.get(&id)
    }

    /// Read, and only by nodes that pick their columns by member, so a wider
    /// schema leaves what they read unchanged.
    pub fn read_only_by_member(&self, id: InputId) -> bool {
        self.is_read(id) && self.all.get(&id) == self.by_member.get(&id)
    }

    fn is_read(&self, id: InputId) -> bool {
        self.all.get(&id).is_some_and(|all| *all > 0)
    }
}

/// A subquery taking over the readers of `absorbed` with a body that also
/// computes what they did.
pub struct SubqueryCollapse {
    pub position: usize,
    pub body: SubqueryBody,
    pub schema: Rc<LogicalSchema>,
    pub absorbed: Vec<InputId>,
}

/// The plan with every collapse applied: absorbed subqueries removed and their
/// readers forwarded to the subquery that took them over.
pub fn collapse_subqueries(
    root: &Rc<RootQuery>,
    collapses: Vec<SubqueryCollapse>,
) -> Result<Rc<RootQuery>, CubeError> {
    let mut replaced_at: HashMap<usize, Rc<Subquery>> = HashMap::new();
    let mut forwards = InputForwards::default();
    let mut absorbed: HashSet<InputId> = HashSet::new();
    for collapse in collapses {
        let kept = &root.subqueries()[collapse.position];
        let to = Rc::new(
            SubqueryRef::builder()
                .id(kept.id)
                .name(kept.name.clone())
                .schema(collapse.schema)
                .build(),
        );
        // Its own readers may now read the measures it took over, so they get
        // its new schema too.
        forwards.add(kept.id, &to);
        for id in collapse.absorbed {
            forwards.add(id, &to);
            absorbed.insert(id);
        }
        replaced_at.insert(collapse.position, kept.with_body(collapse.body));
    }
    let subqueries = root
        .subqueries()
        .iter()
        .enumerate()
        .filter(|(_, subquery)| !absorbed.contains(&subquery.id))
        .map(|(position, subquery)| {
            replaced_at
                .get(&position)
                .cloned()
                .unwrap_or_else(|| subquery.clone())
        })
        .collect_vec();
    Rc::new(
        RootQuery::builder()
            .subqueries(subqueries)
            .query(root.query().clone())
            .build(),
    )
    .forward_all(&forwards)
}

/// The measures merges and lookups read from two of their inputs, each with the
/// subquery reading it (`None` for the root query). A widened subquery must add
/// none: the reader would take either value.
pub fn measures_read_twice(root: &Rc<RootQuery>) -> HashSet<(Option<InputId>, MemberId)> {
    fn walk(
        node: &PlanNode,
        owner: Option<InputId>,
        result: &mut HashSet<(Option<InputId>, MemberId)>,
    ) {
        let owner = match node {
            PlanNode::Subquery(subquery) => Some(subquery.id),
            _ => owner,
        };
        let inputs = match node {
            PlanNode::Merge(merge) => merge.subqueries().clone(),
            PlanNode::GridLookup(lookup) => lookup
                .legs()
                .iter()
                .map(|leg| leg.subquery().clone())
                .collect_vec(),
            _ => vec![],
        };
        let inputs = inputs
            .into_iter()
            .unique_by(|input| input.id())
            .collect_vec();
        for (a, b) in inputs.iter().tuple_combinations() {
            for x in a.schema().measures.iter() {
                if b.schema().measures.iter().any(|y| member_chain_eq(x, y)) {
                    result.insert((owner, x.clone().resolve_reference_chain().id().clone()));
                }
            }
        }
        for input in node.inputs() {
            walk(&input, owner, result);
        }
    }
    let mut result = HashSet::new();
    walk(&root.as_plan_node(), None, &mut result);
    result
}

/// Whether `after` reads some measure twice where `before` did not.
pub fn reads_a_measure_twice_anew(before: &Rc<RootQuery>, after: &Rc<RootQuery>) -> bool {
    !measures_read_twice(after).is_subset(&measures_read_twice(before))
}
