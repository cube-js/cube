use crate::cube_bridge::join_definition::JoinDefinition;
use crate::cube_bridge::join_graph::JoinGraph;
use crate::cube_bridge::join_hints::JoinHintItem;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

/// Mock implementation of JoinGraph for testing
///
/// This mock provides a placeholder implementation.
/// The build_join method is not implemented and will panic with todo!().
///
/// # Example
///
/// ```
/// use cubesqlplanner::test_fixtures::cube_bridge::MockJoinGraph;
///
/// let join_graph = MockJoinGraph;
/// // Note: calling build_join will panic with todo!()
/// ```
pub struct MockJoinGraph;

impl JoinGraph for MockJoinGraph {
    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }

    fn build_join(
        &self,
        _cubes_to_join: Vec<JoinHintItem>,
    ) -> Result<Rc<dyn JoinDefinition>, CubeError> {
        todo!("build_join not implemented in MockJoinGraph")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_create() {
        let _join_graph = MockJoinGraph;
        // Just verify we can create the mock
    }
}