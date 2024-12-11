use crate::planner::sql_evaluator::{EvaluationNode, MemberSymbolType, TraversalVisitor};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct MemberChildsCollector {
    pub childs: Vec<Rc<EvaluationNode>>,
}

#[derive(Clone)]
pub struct MemberChildsCollectorState {
    pub is_root: bool,
}

impl MemberChildsCollectorState {
    pub fn new(is_root: bool) -> Self {
        Self { is_root }
    }
}

impl MemberChildsCollector {
    pub fn new() -> Self {
        Self { childs: vec![] }
    }

    pub fn extract_result(self) -> Vec<Rc<EvaluationNode>> {
        self.childs
    }
}

impl TraversalVisitor for MemberChildsCollector {
    type State = MemberChildsCollectorState;
    fn on_node_traverse(
        &mut self,
        node: &Rc<EvaluationNode>,
        state: &Self::State,
    ) -> Result<Option<Self::State>, CubeError> {
        if state.is_root {
            let new_state = MemberChildsCollectorState::new(false);
            match node.symbol() {
                MemberSymbolType::Measure(s) => {
                    for filter_node in s.measure_filters() {
                        self.apply(filter_node, &new_state)?
                    }
                    for order_by in s.measure_order_by() {
                        self.apply(order_by.evaluation_node(), &new_state)?
                    }
                    Ok(Some(new_state))
                }
                MemberSymbolType::Dimension(_) => Ok(Some(new_state)),
                _ => Ok(None),
            }
        } else {
            match node.symbol() {
                MemberSymbolType::Measure(_) | MemberSymbolType::Dimension(_) => {
                    self.childs.push(node.clone());
                    Ok(None)
                }
                _ => Ok(Some(state.clone())),
            }
        }
    }
}

pub fn member_childs(node: &Rc<EvaluationNode>) -> Result<Vec<Rc<EvaluationNode>>, CubeError> {
    let mut visitor = MemberChildsCollector::new();
    visitor.apply(node, &MemberChildsCollectorState::new(true))?;
    Ok(visitor.extract_result())
}
