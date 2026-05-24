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
}
