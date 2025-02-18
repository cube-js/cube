use super::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub trait TraversalVisitor {
    type State;
    fn on_node_traverse(
        &mut self,
        node: &Rc<MemberSymbol>,
        path: &Vec<String>,
        state: &Self::State,
    ) -> Result<Option<Self::State>, CubeError>;

    fn apply(&mut self, node: &Rc<MemberSymbol>, state: &Self::State) -> Result<(), CubeError> {
        self.apply_with_path(node, &vec![], state)
    }

    fn apply_with_path(
        &mut self,
        node: &Rc<MemberSymbol>,
        path: &Vec<String>,
        state: &Self::State,
    ) -> Result<(), CubeError> {
        if let Some(state) = self.on_node_traverse(node, path, state)? {
            for (dep, dep_path) in node.get_dependencies_with_path() {
                self.apply_with_path(&dep, &dep_path, &state)?
            }
        }
        Ok(())
    }
}
