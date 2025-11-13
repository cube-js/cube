use crate::cube_bridge::member_expression::{ExpressionStruct, ExpressionStructStatic};
use crate::cube_bridge::struct_with_sql_member::StructWithSqlMember;
use crate::impl_static_data;
use crate::test_fixtures::cube_bridge::MockStructWithSqlMember;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Mock implementation of ExpressionStruct for testing
#[derive(TypedBuilder)]
pub struct MockExpressionStruct {
    // Fields from ExpressionStructStatic
    expression_type: String,
    #[builder(default)]
    source_measure: Option<String>,
    #[builder(default)]
    replace_aggregation_type: Option<String>,

    // Optional trait fields
    #[builder(default)]
    add_filters: Option<Vec<Rc<MockStructWithSqlMember>>>,
}

impl_static_data!(
    MockExpressionStruct,
    ExpressionStructStatic,
    expression_type,
    source_measure,
    replace_aggregation_type
);

impl ExpressionStruct for MockExpressionStruct {
    crate::impl_static_data_method!(ExpressionStructStatic);

    fn has_add_filters(&self) -> Result<bool, CubeError> {
        Ok(self.add_filters.is_some())
    }

    fn add_filters(&self) -> Result<Option<Vec<Rc<dyn StructWithSqlMember>>>, CubeError> {
        match &self.add_filters {
            Some(filters) => {
                let result: Vec<Rc<dyn StructWithSqlMember>> = filters
                    .iter()
                    .map(|f| f.clone() as Rc<dyn StructWithSqlMember>)
                    .collect();
                Ok(Some(result))
            }
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
    fn test_basic_expression_struct() {
        let expr = MockExpressionStruct::builder()
            .expression_type("aggregate".to_string())
            .build();

        assert_eq!(expr.static_data().expression_type, "aggregate");
        assert!(!expr.has_add_filters().unwrap());
    }

    #[test]
    fn test_expression_struct_with_source_measure() {
        let expr = MockExpressionStruct::builder()
            .expression_type("measure_reference".to_string())
            .source_measure(Some("users.count".to_string()))
            .build();

        let static_data = expr.static_data();
        assert_eq!(static_data.source_measure, Some("users.count".to_string()));
    }

    #[test]
    fn test_expression_struct_with_replace_aggregation() {
        let expr = MockExpressionStruct::builder()
            .expression_type("aggregate".to_string())
            .replace_aggregation_type(Some("avg".to_string()))
            .build();

        let static_data = expr.static_data();
        assert_eq!(
            static_data.replace_aggregation_type,
            Some("avg".to_string())
        );
    }

    #[test]
    fn test_expression_struct_with_add_filters() {
        let filters = vec![
            Rc::new(
                MockStructWithSqlMember::builder()
                    .sql("{CUBE.status} = 'active'".to_string())
                    .build(),
            ),
            Rc::new(
                MockStructWithSqlMember::builder()
                    .sql("{CUBE.deleted} = false".to_string())
                    .build(),
            ),
        ];

        let expr = MockExpressionStruct::builder()
            .expression_type("aggregate".to_string())
            .add_filters(Some(filters))
            .build();

        assert!(expr.has_add_filters().unwrap());
        let result = expr.add_filters().unwrap().unwrap();
        assert_eq!(result.len(), 2);
    }
}