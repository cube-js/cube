use crate::planner::sql_evaluator::{MemberSymbol, TraversalVisitor};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct MemberChildsCollector {
    pub childs: Vec<Rc<MemberSymbol>>,
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

    pub fn extract_result(self) -> Vec<Rc<MemberSymbol>> {
        self.childs
    }
}

impl TraversalVisitor for MemberChildsCollector {
    type State = MemberChildsCollectorState;
    fn on_node_traverse(
        &mut self,
        node: &Rc<MemberSymbol>,
        state: &Self::State,
    ) -> Result<Option<Self::State>, CubeError> {
        if state.is_root {
            let new_state = MemberChildsCollectorState::new(false);
            match node.as_ref() {
                MemberSymbol::Measure(_) => Ok(Some(new_state)),
                MemberSymbol::Dimension(_) => Ok(Some(new_state)),
                _ => Ok(None),
            }
        } else {
            match node.as_ref() {
                MemberSymbol::Measure(_) | MemberSymbol::Dimension(_) => {
                    self.childs.push(node.clone());
                    Ok(None)
                }
                _ => Ok(Some(state.clone())),
            }
        }
    }
}

pub fn member_childs(node: &Rc<MemberSymbol>) -> Result<Vec<Rc<MemberSymbol>>, CubeError> {
    let mut visitor = MemberChildsCollector::new();
    visitor.apply(node, &MemberChildsCollectorState::new(true))?;
    Ok(visitor.extract_result())
}
