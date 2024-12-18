use super::{MultiStageAppliedState, MultiStageMember};
use crate::planner::sql_evaluator::MemberSymbol;
use std::fmt::Debug;
use std::rc::Rc;

pub struct MultiStageQueryDescription {
    member: Rc<MultiStageMember>,
    state: Rc<MultiStageAppliedState>,
    input: Vec<Rc<MultiStageQueryDescription>>,
    alias: String,
}

impl Debug for MultiStageQueryDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiStageQueryDescription")
            .field(
                "member_node",
                &format!("node with path {}", self.member_node().full_name()),
            )
            .field("state", &self.state)
            .field("input", &self.input)
            .field("alias", &self.alias)
            .finish()
    }
}

impl MultiStageQueryDescription {
    pub fn new(
        member: Rc<MultiStageMember>,
        state: Rc<MultiStageAppliedState>,
        input: Vec<Rc<MultiStageQueryDescription>>,
        alias: String,
    ) -> Rc<Self> {
        Rc::new(Self {
            member,
            state,
            input,
            alias,
        })
    }

    pub fn member_node(&self) -> &Rc<MemberSymbol> {
        &self.member.evaluation_node()
    }

    pub fn member(&self) -> &Rc<MultiStageMember> {
        &self.member
    }

    pub fn state(&self) -> Rc<MultiStageAppliedState> {
        self.state.clone()
    }

    pub fn member_name(&self) -> String {
        self.member_node().full_name()
    }

    pub fn alias(&self) -> &String {
        &self.alias
    }

    pub fn input(&self) -> &Vec<Rc<MultiStageQueryDescription>> {
        &self.input
    }

    pub fn is_leaf(&self) -> bool {
        self.input.is_empty()
    }

    pub fn is_match_member_and_state(
        &self,
        member_node: &Rc<MemberSymbol>,
        state: &Rc<MultiStageAppliedState>,
    ) -> bool {
        member_node.full_name() == self.member_name() && state == &self.state
    }
}
