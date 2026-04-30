use crate::cube_bridge::join_definition::{JoinDefinition, JoinDefinitionStatic};
use crate::cube_bridge::join_item::JoinItem;
use crate::impl_static_data;
use crate::test_fixtures::cube_bridge::MockJoinItem;
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(Debug, TypedBuilder)]
pub struct MockJoinDefinition {
    root: String,
    #[builder(default)]
    multiplication_factor: HashMap<String, bool>,

    joins: Vec<Rc<MockJoinItem>>,
}

impl_static_data!(
    MockJoinDefinition,
    JoinDefinitionStatic,
    root,
    multiplication_factor
);

impl JoinDefinition for MockJoinDefinition {
    crate::impl_static_data_method!(JoinDefinitionStatic);

    fn joins(&self) -> Result<Vec<Rc<dyn JoinItem>>, CubeError> {
        Ok(self
            .joins
            .iter()
            .map(|j| j.clone() as Rc<dyn JoinItem>)
            .collect())
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}
