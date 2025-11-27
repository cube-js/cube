use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::segment_definition::{SegmentDefinition, SegmentDefinitionStatic};
use crate::impl_static_data;
use crate::test_fixtures::cube_bridge::MockMemberSql;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Mock implementation of SegmentDefinition for testing
#[derive(TypedBuilder)]
pub struct MockSegmentDefinition {
    // Fields from SegmentDefinitionStatic
    #[builder(default)]
    segment_type: Option<String>,
    #[builder(default)]
    owned_by_cube: Option<bool>,

    // Trait field
    sql: String,
}

impl_static_data!(
    MockSegmentDefinition,
    SegmentDefinitionStatic,
    segment_type,
    owned_by_cube
);

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
    use crate::test_fixtures::cube_bridge::MockBaseTools;

    use super::*;

    #[test]
    fn test_basic_segment() {
        let segment = MockSegmentDefinition::builder()
            .sql("{CUBE.status} = 'active'".to_string())
            .build();

        let sql = segment.sql().unwrap();
        assert_eq!(sql.args_names(), &vec!["CUBE"]);
    }

    #[test]
    fn test_segment_with_type() {
        let segment = MockSegmentDefinition::builder()
            .segment_type(Some("filter".to_string()))
            .sql("{CUBE.deleted} = false".to_string())
            .build();

        assert_eq!(
            segment.static_data().segment_type,
            Some("filter".to_string())
        );
    }

    #[test]
    fn test_segment_owned_by_cube() {
        let segment = MockSegmentDefinition::builder()
            .owned_by_cube(Some(true))
            .sql("{CUBE.is_valid} = true".to_string())
            .build();

        assert_eq!(segment.static_data().owned_by_cube, Some(true));
    }

    #[test]
    fn test_complex_segment_sql() {
        let segment = MockSegmentDefinition::builder()
            .sql(
                "{CUBE.created_at} >= '2024-01-01' AND {CUBE.status} IN ('active', 'pending')"
                    .to_string(),
            )
            .build();

        let sql = segment.sql().unwrap();
        assert_eq!(sql.args_names(), &vec!["CUBE"]);

        use crate::test_fixtures::cube_bridge::MockSecurityContext;
        let (template, args) = sql
            .compile_template_sql(
                Rc::new(MockBaseTools::default()),
                Rc::new(MockSecurityContext),
            )
            .unwrap();

        match template {
            crate::cube_bridge::member_sql::SqlTemplate::String(s) => {
                assert_eq!(
                    s,
                    "{arg:0} >= '2024-01-01' AND {arg:1} IN ('active', 'pending')"
                );
            }
            _ => panic!("Expected String template"),
        }

        assert_eq!(args.symbol_paths.len(), 2);
        assert_eq!(args.symbol_paths[0], vec!["CUBE", "created_at"]);
        assert_eq!(args.symbol_paths[1], vec!["CUBE", "status"]);
    }

    #[test]
    fn test_segment_with_cross_cube_reference() {
        let segment = MockSegmentDefinition::builder()
            .sql(
                "{CUBE.user_id} IN (SELECT id FROM {users} WHERE {users.is_premium} = true)"
                    .to_string(),
            )
            .build();

        let sql = segment.sql().unwrap();
        assert_eq!(sql.args_names(), &vec!["CUBE", "users"]);
    }
}
