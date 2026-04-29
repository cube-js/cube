use crate::logical_plan::{LogicalMultiStageMember, MultiStageSubqueryRef};
use std::rc::Rc;

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

    pub fn into_results(
        self,
    ) -> (
        Vec<Rc<LogicalMultiStageMember>>,
        Vec<Rc<MultiStageSubqueryRef>>,
    ) {
        (self.members, self.subquery_refs)
    }
}
