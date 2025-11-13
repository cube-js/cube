use crate::cube_bridge::cube_definition::{CubeDefinition, CubeDefinitionStatic};
use crate::cube_bridge::member_sql::MemberSql;
use crate::impl_static_data;
use crate::test_fixtures::cube_bridge::MockMemberSql;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Mock implementation of CubeDefinition for testing
#[derive(Clone, TypedBuilder)]
pub struct MockCubeDefinition {
    // Fields from CubeDefinitionStatic
    name: String,
    #[builder(default)]
    sql_alias: Option<String>,
    #[builder(default)]
    is_view: Option<bool>,
    #[builder(default)]
    is_calendar: Option<bool>,
    #[builder(default)]
    join_map: Option<Vec<Vec<String>>>,

    // Optional trait fields
    #[builder(default, setter(strip_option))]
    sql_table: Option<String>,
    #[builder(default, setter(strip_option))]
    sql: Option<String>,
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

#[cfg(test)]
mod tests {
    use super::*;

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
}