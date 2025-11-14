use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::timeshift_definition::{TimeShiftDefinition, TimeShiftDefinitionStatic};
use crate::impl_static_data;
use crate::test_fixtures::cube_bridge::MockMemberSql;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Mock implementation of TimeShiftDefinition for testing
#[derive(Debug, Clone, TypedBuilder)]
pub struct MockTimeShiftDefinition {
    #[builder(default)]
    interval: Option<String>,
    #[builder(default)]
    timeshift_type: Option<String>,
    #[builder(default)]
    name: Option<String>,
    #[builder(default, setter(strip_option))]
    sql: Option<String>,
}

impl_static_data!(
    MockTimeShiftDefinition,
    TimeShiftDefinitionStatic,
    interval,
    timeshift_type,
    name
);

impl TimeShiftDefinition for MockTimeShiftDefinition {
    crate::impl_static_data_method!(TimeShiftDefinitionStatic);

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
    fn test_mock_timeshift_with_sql() {
        let timeshift = MockTimeShiftDefinition::builder()
            .interval(Some("1 day".to_string()))
            .timeshift_type(Some("prior".to_string()))
            .name(Some("yesterday".to_string()))
            .sql("{CUBE.date_field}".to_string())
            .build();

        assert_eq!(timeshift.static_data().interval, Some("1 day".to_string()));
        assert!(timeshift.has_sql().unwrap());
        assert!(timeshift.sql().unwrap().is_some());
    }

    #[test]
    fn test_mock_timeshift_without_sql() {
        let timeshift = MockTimeShiftDefinition::builder()
            .interval(Some("1 week".to_string()))
            .build();

        assert_eq!(timeshift.static_data().interval, Some("1 week".to_string()));
        assert!(!timeshift.has_sql().unwrap());
        assert!(timeshift.sql().unwrap().is_none());
    }
}
