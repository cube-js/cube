use super::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub trait TraversalVisitor {
    type State;
    fn on_node_traverse(
        &mut self,
        node: &Rc<MemberSymbol>,
        state: &Self::State,
    ) -> Result<Option<Self::State>, CubeError>;

    fn apply(&mut self, node: &Rc<MemberSymbol>, state: &Self::State) -> Result<(), CubeError> {
        if let Some(state) = self.on_node_traverse(node, state)? {
            for dep in node.get_dependencies() {
                self.apply(&dep, &state)?
            }
        }
        Ok(())
    }
}
