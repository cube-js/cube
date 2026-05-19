use crate::logical_plan::{LogicalMultiStageMember, MultiStageSubqueryRef};
use std::rc::Rc;

/// Accumulator the multi-stage / multiplied planners write CTEs
/// into during planning: a monotonic counter for generated CTE
/// names (`cte_0`, `cte_1`, ...), the list of
/// `LogicalMultiStageMember`s, and the subquery refs that the
/// outer `FullKeyAggregate` will join over.
pub struct CteState {
    counter: usize,
    members: Vec<Rc<LogicalMultiStageMember>>,
    subquery_refs: Vec<Rc<MultiStageSubqueryRef>>,
}

impl CteState {
    pub fn new() -> Self {
        Self {
            counter: 0,
            members: Vec::new(),
            subquery_refs: Vec::new(),
        }
    }

    /// Generates the next unique CTE name (`cte_0`, `cte_1`, ...).
    pub fn next_cte_name(&mut self) -> String {
        let name = format!("cte_{}", self.counter);
        self.counter += 1;
        name
    }

    pub fn add_member(&mut self, member: Rc<LogicalMultiStageMember>) {
        if self.members.iter().any(|m| m.name == member.name) {
            // Same-named bodies (e.g. the same DSQ referenced from
            // multiple consumers) are deduplicated here so each CTE is
            // rendered once.
            return;
        }
        self.members.push(member);
    }

    pub fn add_subquery_ref(&mut self, subquery_ref: Rc<MultiStageSubqueryRef>) {
        self.subquery_refs.push(subquery_ref);
    }

    /// Consumes the state, returning the accumulated CTE members
    /// and subquery refs.
    pub fn into_results(
        self,
    ) -> (
        Vec<Rc<LogicalMultiStageMember>>,
        Vec<Rc<MultiStageSubqueryRef>>,
    ) {
        (self.members, self.subquery_refs)
    }

    /// Drain refs accumulated since `baseline` — caller uses them as the
    /// FK data inputs of its root Query. Members stay in this `CteState`
    /// and are read off `into_results` to populate the `LogicalPlan`
    /// CTE pool.
    pub fn drain_subquery_refs_from(&mut self, baseline: usize) -> Vec<Rc<MultiStageSubqueryRef>> {
        self.subquery_refs.split_off(baseline)
    }
}
