use crate::cube_bridge::join_item::{JoinItem, JoinItemStatic};
use crate::cube_bridge::join_item_definition::JoinItemDefinition;
use crate::impl_static_data;
use crate::test_fixtures::cube_bridge::MockJoinItemDefinition;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Mock implementation of JoinItem for testing
#[derive(Debug, TypedBuilder)]
pub struct MockJoinItem {
    // Fields from JoinItemStatic
    from: String,
    to: String,
    original_from: String,
    original_to: String,

    // Trait field
    join: Rc<MockJoinItemDefinition>,
}

impl_static_data!(
    MockJoinItem,
    JoinItemStatic,
    from,
    to,
    original_from,
    original_to
);

impl JoinItem for MockJoinItem {
    crate::impl_static_data_method!(JoinItemStatic);

    fn join(&self) -> Result<Rc<dyn JoinItemDefinition>, CubeError> {
        Ok(self.join.clone() as Rc<dyn JoinItemDefinition>)
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_join_item() {
        let join_def = Rc::new(
            MockJoinItemDefinition::builder()
                .relationship("many_to_one".to_string())
                .sql("{orders.user_id} = {users.id}".to_string())
                .build(),
        );

        let join_item = MockJoinItem::builder()
            .from("orders".to_string())
            .to("users".to_string())
            .original_from("Orders".to_string())
            .original_to("Users".to_string())
            .join(join_def)
            .build();

        let static_data = join_item.static_data();
        assert_eq!(static_data.from, "orders");
        assert_eq!(static_data.to, "users");
        assert_eq!(static_data.original_from, "Orders");
        assert_eq!(static_data.original_to, "Users");

        let join = join_item.join().unwrap();
        assert_eq!(join.static_data().relationship, "many_to_one");
    }

    #[test]
    fn test_join_item_with_aliases() {
        let join_def = Rc::new(
            MockJoinItemDefinition::builder()
                .relationship("one_to_many".to_string())
                .sql("{u.id} = {o.user_id}".to_string())
                .build(),
        );

        let join_item = MockJoinItem::builder()
            .from("u".to_string())
            .to("o".to_string())
            .original_from("users".to_string())
            .original_to("orders".to_string())
            .join(join_def)
            .build();

        assert_eq!(join_item.static_data().from, "u");
        assert_eq!(join_item.static_data().original_from, "users");
    }
}
