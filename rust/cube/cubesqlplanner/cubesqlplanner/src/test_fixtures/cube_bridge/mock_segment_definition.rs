use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::segment_definition::{SegmentDefinition, SegmentDefinitionStatic};
use crate::impl_static_data;
use crate::test_fixtures::cube_bridge::yaml::segment::YamlSegmentDefinition;
use crate::test_fixtures::cube_bridge::MockMemberSql;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub struct MockSegmentDefinition {
    #[builder(default)]
    segment_type: Option<String>,
    #[builder(default)]
    owned_by_cube: Option<bool>,

    sql: String,
}

impl_static_data!(
    MockSegmentDefinition,
    SegmentDefinitionStatic,
    segment_type,
    owned_by_cube
);

impl MockSegmentDefinition {
    pub fn from_yaml(yaml: &str) -> Result<Rc<Self>, CubeError> {
        let yaml_def: YamlSegmentDefinition = serde_yaml::from_str(yaml)
            .map_err(|e| CubeError::user(format!("Failed to parse YAML: {}", e)))?;
        Ok(yaml_def.build())
    }
}

impl SegmentDefinition for MockSegmentDefinition {
    crate::impl_static_data_method!(SegmentDefinitionStatic);

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
    use indoc::indoc;

    #[test]
    fn test_from_yaml_with_fields() {
        let yaml = indoc! {"
            type: bool
            sql: \"{CUBE}.amount > 100\"
        "};

        let segment = MockSegmentDefinition::from_yaml(yaml).unwrap();
        let static_data = segment.static_data();

        assert_eq!(static_data.segment_type, Some("bool".to_string()));

        let sql = segment.sql().unwrap();
        assert_eq!(sql.args_names(), &vec!["CUBE"]);
    }
}
