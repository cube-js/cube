use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::timeshift_definition::{TimeShiftDefinition, TimeShiftDefinitionStatic};
use crate::impl_static_data;
use crate::test_fixtures::cube_bridge::yaml::timeshift::YamlTimeShiftDefinition;
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
    #[builder(default, setter(strip_option(fallback = sql_opt)))]
    sql: Option<String>,
}

impl_static_data!(
    MockTimeShiftDefinition,
    TimeShiftDefinitionStatic,
    interval,
    timeshift_type,
    name
);

impl MockTimeShiftDefinition {
    pub fn from_yaml(yaml: &str) -> Result<Rc<Self>, CubeError> {
        let yaml_def: YamlTimeShiftDefinition = serde_yaml::from_str(yaml)
            .map_err(|e| CubeError::user(format!("Failed to parse YAML: {}", e)))?;
        Ok(yaml_def.build())
    }
}

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
    use indoc::indoc;

    #[test]
    fn test_from_yaml_all_fields() {
        let yaml = indoc! {"
            interval: 1 year
            type: prior
            name: date
            sql: \"{CUBE}.created_at\"
        "};

        let ts = MockTimeShiftDefinition::from_yaml(yaml).unwrap();
        let static_data = ts.static_data();

        assert_eq!(static_data.interval, Some("1 year".to_string()));
        assert_eq!(static_data.timeshift_type, Some("prior".to_string()));
        assert_eq!(static_data.name, Some("date".to_string()));
        assert!(ts.has_sql().unwrap());
    }

    #[test]
    fn test_from_yaml_minimal() {
        let yaml = indoc! {"
            interval: 1 month
        "};

        let ts = MockTimeShiftDefinition::from_yaml(yaml).unwrap();
        let static_data = ts.static_data();

        assert_eq!(static_data.interval, Some("1 month".to_string()));
        assert_eq!(static_data.timeshift_type, None);
        assert_eq!(static_data.name, None);
        assert!(!ts.has_sql().unwrap());
    }
}
