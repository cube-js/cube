use crate::cube_bridge::join_definition::{JoinDefinition, JoinDefinitionStatic};
use crate::cube_bridge::join_item::JoinItem;
use crate::impl_static_data;
use crate::test_fixtures::cube_bridge::MockJoinItem;
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Mock implementation of JoinDefinition for testing
#[derive(Debug, TypedBuilder)]
pub struct MockJoinDefinition {
    // Fields from JoinDefinitionStatic
    root: String,
    #[builder(default)]
    multiplication_factor: HashMap<String, bool>,

    // Trait field
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::cube_bridge::MockJoinItemDefinition;

    #[test]
    fn test_basic_join_definition() {
        let join_item = Rc::new(
            MockJoinItem::builder()
                .from("orders".to_string())
                .to("users".to_string())
                .original_from("Orders".to_string())
                .original_to("Users".to_string())
                .join(Rc::new(
                    MockJoinItemDefinition::builder()
                        .relationship("many_to_one".to_string())
                        .sql("{orders.user_id} = {users.id}".to_string())
                        .build(),
                ))
                .build(),
        );

        let join_def = MockJoinDefinition::builder()
            .root("orders".to_string())
            .joins(vec![join_item])
            .build();

        assert_eq!(join_def.static_data().root, "orders");
        let joins = join_def.joins().unwrap();
        assert_eq!(joins.len(), 1);
    }

    #[test]
    fn test_join_definition_with_multiplication_factor() {
        let mut mult_factor = HashMap::new();
        mult_factor.insert("orders".to_string(), true);
        mult_factor.insert("users".to_string(), false);

        let join_def = MockJoinDefinition::builder()
            .root("orders".to_string())
            .multiplication_factor(mult_factor.clone())
            .joins(vec![])
            .build();

        assert_eq!(join_def.static_data().multiplication_factor, mult_factor);
    }

    #[test]
    fn test_join_definition_with_multiple_joins() {
        let join_to_users = Rc::new(
            MockJoinItem::builder()
                .from("orders".to_string())
                .to("users".to_string())
                .original_from("Orders".to_string())
                .original_to("Users".to_string())
                .join(Rc::new(
                    MockJoinItemDefinition::builder()
                        .relationship("many_to_one".to_string())
                        .sql("{orders.user_id} = {users.id}".to_string())
                        .build(),
                ))
                .build(),
        );

        let join_to_products = Rc::new(
            MockJoinItem::builder()
                .from("orders".to_string())
                .to("products".to_string())
                .original_from("Orders".to_string())
                .original_to("Products".to_string())
                .join(Rc::new(
                    MockJoinItemDefinition::builder()
                        .relationship("many_to_one".to_string())
                        .sql("{orders.product_id} = {products.id}".to_string())
                        .build(),
                ))
                .build(),
        );

        let join_def = MockJoinDefinition::builder()
            .root("orders".to_string())
            .joins(vec![join_to_users, join_to_products])
            .build();

        let joins = join_def.joins().unwrap();
        assert_eq!(joins.len(), 2);
        assert_eq!(joins[0].static_data().to, "users");
        assert_eq!(joins[1].static_data().to, "products");
    }

    #[test]
    fn test_complex_join_graph() {
        // Orders -> Users
        let join_orders_users = Rc::new(
            MockJoinItem::builder()
                .from("orders".to_string())
                .to("users".to_string())
                .original_from("Orders".to_string())
                .original_to("Users".to_string())
                .join(Rc::new(
                    MockJoinItemDefinition::builder()
                        .relationship("many_to_one".to_string())
                        .sql("{orders.user_id} = {users.id}".to_string())
                        .build(),
                ))
                .build(),
        );

        // Users -> Countries
        let join_users_countries = Rc::new(
            MockJoinItem::builder()
                .from("users".to_string())
                .to("countries".to_string())
                .original_from("Users".to_string())
                .original_to("Countries".to_string())
                .join(Rc::new(
                    MockJoinItemDefinition::builder()
                        .relationship("many_to_one".to_string())
                        .sql("{users.country_id} = {countries.id}".to_string())
                        .build(),
                ))
                .build(),
        );

        // Orders -> Products
        let join_orders_products = Rc::new(
            MockJoinItem::builder()
                .from("orders".to_string())
                .to("products".to_string())
                .original_from("Orders".to_string())
                .original_to("Products".to_string())
                .join(Rc::new(
                    MockJoinItemDefinition::builder()
                        .relationship("many_to_many".to_string())
                        .sql("{orders.id} = {order_items.order_id} AND {order_items.product_id} = {products.id}".to_string())
                        .build(),
                ))
                .build(),
        );

        let mut mult_factor = HashMap::new();
        mult_factor.insert("products".to_string(), true);

        let join_def = MockJoinDefinition::builder()
            .root("orders".to_string())
            .joins(vec![
                join_orders_users,
                join_users_countries,
                join_orders_products,
            ])
            .multiplication_factor(mult_factor)
            .build();

        let static_data = join_def.static_data();
        assert_eq!(static_data.root, "orders");
        assert_eq!(
            static_data.multiplication_factor.get("products"),
            Some(&true)
        );

        let joins = join_def.joins().unwrap();
        assert_eq!(joins.len(), 3);
    }
}
