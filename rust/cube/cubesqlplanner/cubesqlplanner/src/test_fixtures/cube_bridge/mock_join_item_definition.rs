use crate::cube_bridge::join_item_definition::{JoinItemDefinition, JoinItemDefinitionStatic};
use crate::cube_bridge::member_sql::MemberSql;
use crate::test_fixtures::cube_bridge::MockMemberSql;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Mock implementation of JoinItemDefinition for testing
#[derive(Debug, Clone, TypedBuilder)]
pub struct MockJoinItemDefinition {
    // Fields from JoinItemDefinitionStatic
    name: String,
    #[builder(default, setter(strip_option(fallback = alias_opt)))]
    alias: Option<String>,
    relationship: String,

    // Trait field
    sql: String,
}

impl_static_data!(
    MockJoinItemDefinition,
    JoinItemDefinitionStatic,
    name,
    alias,
    relationship
);

impl JoinItemDefinition for MockJoinItemDefinition {
    crate::impl_static_data_method!(JoinItemDefinitionStatic);

    fn sql(&self) -> Result<Rc<dyn MemberSql>, CubeError> {
        Ok(Rc::new(MockMemberSql::new(&self.sql)?))
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_many_to_one_join() {
        let join_def = MockJoinItemDefinition::builder()
            .name("users".to_string())
            .relationship("many_to_one".to_string())
            .sql("{CUBE.user_id} = {users.id}".to_string())
            .build();

        assert_eq!(join_def.static_data().relationship, "many_to_one");
        let sql = join_def.sql().unwrap();
        assert_eq!(sql.args_names(), &vec!["CUBE", "users"]);
    }

    #[test]
    fn test_one_to_many_join() {
        let join_def = MockJoinItemDefinition::builder()
            .name("orders".to_string())
            .relationship("one_to_many".to_string())
            .sql("{CUBE.id} = {orders.user_id}".to_string())
            .build();

        assert_eq!(join_def.static_data().relationship, "one_to_many");
    }

    #[test]
    fn test_one_to_one_join() {
        let join_def = MockJoinItemDefinition::builder()
            .name("profile".to_string())
            .relationship("one_to_one".to_string())
            .sql("{CUBE.id} = {profile.user_id}".to_string())
            .build();

        assert_eq!(join_def.static_data().relationship, "one_to_one");
    }

    #[test]
    fn test_unaliased_join_is_named_by_joined_cube() {
        let join_def = MockJoinItemDefinition::builder()
            .name("users".to_string())
            .relationship("many_to_one".to_string())
            .sql("{CUBE.user_id} = {users.id}".to_string())
            .build();

        let static_data = join_def.static_data();
        assert_eq!(static_data.alias, None);
        assert_eq!(static_data.effective_name(), "users");
    }

    #[test]
    fn test_aliased_join() {
        let join_def = MockJoinItemDefinition::builder()
            .name("users".to_string())
            .alias("manager".to_string())
            .relationship("many_to_one".to_string())
            .sql("{CUBE.manager_id} = {users.id}".to_string())
            .build();

        let static_data = join_def.static_data();
        assert_eq!(static_data.name, "users");
        assert_eq!(static_data.alias.as_deref(), Some("manager"));
        assert_eq!(static_data.effective_name(), "manager");
    }
}
