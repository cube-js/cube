use super::MultiStageAppliedState;
use crate::planner::sql_evaluator::EvaluationNode;
use std::fmt::Debug;
use std::rc::Rc;

pub struct MultiStageQueryDescription {
    member_node: Rc<EvaluationNode>,
    state: Rc<MultiStageAppliedState>,
    input: Vec<Rc<MultiStageQueryDescription>>,
    alias: String,
}

impl Debug for MultiStageQueryDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiStageQueryDescription")
            .field(
                "member_node",
                &format!("node with path {}", self.member_node.full_name()),
            )
            .field("state", &self.state)
            .field("input", &self.input)
            .field("alias", &self.alias)
            .finish()
    }
}

impl MultiStageQueryDescription {
    pub fn new(
        member_node: Rc<EvaluationNode>,
        state: Rc<MultiStageAppliedState>,
        input: Vec<Rc<MultiStageQueryDescription>>,
        alias: String,
    ) -> Rc<Self> {
        Rc::new(Self {
            member_node,
            state,
            input,
            alias,
        })
    }

    pub fn member_node(&self) -> &Rc<EvaluationNode> {
        &self.member_node
    }

    pub fn state(&self) -> Rc<MultiStageAppliedState> {
        self.state.clone()
    }

    pub fn member_name(&self) -> String {
        self.member_node.full_name()
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
        member_node: &Rc<EvaluationNode>,
        state: &Rc<MultiStageAppliedState>,
    ) -> bool {
        member_node.full_name() == self.member_name() && state == &self.state
    }
}
