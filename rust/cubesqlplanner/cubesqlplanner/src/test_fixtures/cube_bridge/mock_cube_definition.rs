use crate::cube_bridge::cube_definition::{CubeDefinition, CubeDefinitionStatic};
use crate::cube_bridge::member_sql::MemberSql;
use crate::impl_static_data;
use crate::test_fixtures::cube_bridge::{MockJoinItemDefinition, MockMemberSql};
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(Clone, TypedBuilder)]
pub struct MockCubeDefinition {
    name: String,
    #[builder(default)]
    sql_alias: Option<String>,
    #[builder(default)]
    is_view: Option<bool>,
    #[builder(default)]
    is_calendar: Option<bool>,
    #[builder(default)]
    join_map: Option<Vec<Vec<String>>>,

    #[builder(default, setter(strip_option(fallback = sql_table_opt)))]
    sql_table: Option<String>,
    #[builder(default, setter(strip_option(fallback = sql_opt)))]
    sql: Option<String>,

    #[builder(default)]
    joins: HashMap<String, MockJoinItemDefinition>,
}

impl_static_data!(
    MockCubeDefinition,
    CubeDefinitionStatic,
    name,
    sql_alias,
    is_view,
    is_calendar,
    join_map
);

impl CubeDefinition for MockCubeDefinition {
    crate::impl_static_data_method!(CubeDefinitionStatic);

    fn has_sql_table(&self) -> Result<bool, CubeError> {
        Ok(self.sql_table.is_some())
    }

    fn sql_table(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError> {
        match &self.sql_table {
            Some(sql_str) => Ok(Some(Rc::new(MockMemberSql::new(sql_str)?))),
            None => Ok(None),
        }
    }

    fn has_sql(&self) -> Result<bool, CubeError> {
        Ok(self.sql.is_some())
    }

    fn sql(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError> {
        match &self.sql {
            Some(sql_str) => Ok(Some(Rc::new(MockMemberSql::new(sql_str)?))),
            None => Ok(None),
        }
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}

impl MockCubeDefinition {
    pub fn joins(&self) -> &HashMap<String, MockJoinItemDefinition> {
        &self.joins
    }

    pub fn get_join(&self, name: &str) -> Option<&MockJoinItemDefinition> {
        self.joins.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cube_bridge::join_item_definition::JoinItemDefinition;
    use std::collections::HashMap;

    #[test]
    fn test_basic_cube() {
        let cube = MockCubeDefinition::builder()
            .name("users".to_string())
            .sql_table("public.users".to_string())
            .build();

        assert_eq!(cube.static_data().name, "users");
        assert!(cube.has_sql_table().unwrap());
        assert!(cube.sql_table().unwrap().is_some());
    }

    #[test]
    fn test_cube_with_alias() {
        let cube = MockCubeDefinition::builder()
            .name("users".to_string())
            .sql_alias(Some("u".to_string()))
            .sql_table("public.users".to_string())
            .build();

        let static_data = cube.static_data();
        assert_eq!(static_data.name, "users");
        assert_eq!(static_data.sql_alias, Some("u".to_string()));
        assert_eq!(static_data.resolved_alias(), "u");
    }

    #[test]
    fn test_cube_without_alias_uses_name() {
        let cube = MockCubeDefinition::builder()
            .name("users".to_string())
            .sql_table("public.users".to_string())
            .build();

        let static_data = cube.static_data();
        assert_eq!(static_data.resolved_alias(), "users");
    }

    #[test]
    fn test_view_cube() {
        let cube = MockCubeDefinition::builder()
            .name("active_users".to_string())
            .is_view(Some(true))
            .sql("SELECT * FROM users WHERE status = 'active'".to_string())
            .build();

        assert_eq!(cube.static_data().is_view, Some(true));
        assert!(cube.has_sql().unwrap());
        assert!(!cube.has_sql_table().unwrap());
    }

    #[test]
    fn test_calendar_cube() {
        let cube = MockCubeDefinition::builder()
            .name("calendar".to_string())
            .is_calendar(Some(true))
            .sql_table("public.calendar".to_string())
            .build();

        assert_eq!(cube.static_data().is_calendar, Some(true));
    }

    #[test]
    fn test_cube_with_join_map() {
        let join_map = vec![
            vec!["users".to_string(), "orders".to_string()],
            vec!["orders".to_string(), "products".to_string()],
        ];

        let cube = MockCubeDefinition::builder()
            .name("users".to_string())
            .sql_table("public.users".to_string())
            .join_map(Some(join_map.clone()))
            .build();

        assert_eq!(cube.static_data().join_map, Some(join_map));
    }

    #[test]
    fn test_cube_sql_parsing() {
        let cube = MockCubeDefinition::builder()
            .name("derived_cube".to_string())
            .sql("SELECT {other_cube.id}, COUNT(*) FROM {other_cube} GROUP BY 1".to_string())
            .build();

        let sql = cube.sql().unwrap().unwrap();
        assert_eq!(sql.args_names(), &vec!["other_cube"]);
    }

    #[test]
    fn test_cube_with_sql_table_reference() {
        let cube = MockCubeDefinition::builder()
            .name("users".to_string())
            .sql_table("{database.schema.users}".to_string())
            .build();

        let sql_table = cube.sql_table().unwrap().unwrap();
        assert_eq!(sql_table.args_names(), &vec!["database"]);
    }

    #[test]
    fn test_cube_with_single_join() {
        let mut joins = HashMap::new();
        joins.insert(
            "users".to_string(),
            MockJoinItemDefinition::builder()
                .relationship("many_to_one".to_string())
                .sql("{CUBE}.user_id = {users.id}".to_string())
                .build(),
        );

        let cube = MockCubeDefinition::builder()
            .name("orders".to_string())
            .sql_table("public.orders".to_string())
            .joins(joins)
            .build();

        assert_eq!(cube.joins().len(), 1);
        assert!(cube.get_join("users").is_some());

        let users_join = cube.get_join("users").unwrap();
        assert_eq!(users_join.static_data().relationship, "many_to_one");
    }

    #[test]
    fn test_cube_with_multiple_joins() {
        let mut joins = HashMap::new();
        joins.insert(
            "users".to_string(),
            MockJoinItemDefinition::builder()
                .relationship("many_to_one".to_string())
                .sql("{CUBE}.user_id = {users.id}".to_string())
                .build(),
        );
        joins.insert(
            "products".to_string(),
            MockJoinItemDefinition::builder()
                .relationship("many_to_one".to_string())
                .sql("{CUBE}.product_id = {products.id}".to_string())
                .build(),
        );

        let cube = MockCubeDefinition::builder()
            .name("orders".to_string())
            .sql_table("public.orders".to_string())
            .joins(joins)
            .build();

        assert_eq!(cube.joins().len(), 2);
        assert!(cube.get_join("users").is_some());
        assert!(cube.get_join("products").is_some());
        assert!(cube.get_join("nonexistent").is_none());
    }

    #[test]
    fn test_join_accessor_methods() {
        let mut joins = HashMap::new();
        joins.insert(
            "countries".to_string(),
            MockJoinItemDefinition::builder()
                .relationship("many_to_one".to_string())
                .sql("{CUBE}.country_id = {countries.id}".to_string())
                .build(),
        );

        let cube = MockCubeDefinition::builder()
            .name("users".to_string())
            .sql_table("public.users".to_string())
            .joins(joins)
            .build();

        let all_joins = cube.joins();
        assert_eq!(all_joins.len(), 1);
        assert!(all_joins.contains_key("countries"));

        let country_join = cube.get_join("countries").unwrap();
        let sql = country_join.sql().unwrap();
        assert_eq!(sql.args_names(), &vec!["CUBE", "countries"]);

        assert!(cube.get_join("nonexistent").is_none());
    }

    #[test]
    fn test_cube_without_joins() {
        let cube = MockCubeDefinition::builder()
            .name("users".to_string())
            .sql_table("public.users".to_string())
            .build();

        assert_eq!(cube.joins().len(), 0);
        assert!(cube.get_join("any").is_none());
    }
}
