use crate::cube_bridge::member_expression::{
    MemberExpressionDefinition, MemberExpressionDefinitionStatic, MemberExpressionExpressionDef,
};
use crate::impl_static_data;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Mock implementation of MemberExpressionDefinition for testing
#[derive(TypedBuilder)]
pub struct MockMemberExpressionDefinition {
    // Fields from MemberExpressionDefinitionStatic
    #[builder(default)]
    expression_name: Option<String>,
    #[builder(default)]
    name: Option<String>,
    #[builder(default)]
    cube_name: Option<String>,
    #[builder(default)]
    definition: Option<String>,

    // Trait field
    expression: MemberExpressionExpressionDef,
}

impl_static_data!(
    MockMemberExpressionDefinition,
    MemberExpressionDefinitionStatic,
    expression_name,
    name,
    cube_name,
    definition
);

impl MemberExpressionDefinition for MockMemberExpressionDefinition {
    crate::impl_static_data_method!(MemberExpressionDefinitionStatic);

    fn expression(&self) -> Result<MemberExpressionExpressionDef, CubeError> {
        Ok(match &self.expression {
            MemberExpressionExpressionDef::Sql(sql) => {
                MemberExpressionExpressionDef::Sql(sql.clone())
            }
            MemberExpressionExpressionDef::Struct(expr) => {
                MemberExpressionExpressionDef::Struct(expr.clone())
            }
        })
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::cube_bridge::{MockExpressionStruct, MockMemberSql};

    #[test]
    fn test_member_expression_with_sql() {
        let sql = Rc::new(MockMemberSql::new("{CUBE.amount} * 2").unwrap());

        let expr = MockMemberExpressionDefinition::builder()
            .expression_name(Some("double_amount".to_string()))
            .name(Some("doubleAmount".to_string()))
            .cube_name(Some("orders".to_string()))
            .expression(MemberExpressionExpressionDef::Sql(
                sql as Rc<dyn crate::cube_bridge::member_sql::MemberSql>,
            ))
            .build();

        let static_data = expr.static_data();
        assert_eq!(
            static_data.expression_name,
            Some("double_amount".to_string())
        );
        assert_eq!(static_data.cube_name, Some("orders".to_string()));

        let result = expr.expression().unwrap();
        assert!(matches!(result, MemberExpressionExpressionDef::Sql(_)));
    }

    #[test]
    fn test_member_expression_with_struct() {
        let expr_struct = Rc::new(
            MockExpressionStruct::builder()
                .expression_type("aggregate".to_string())
                .source_measure(Some("users.revenue".to_string()))
                .build(),
        );

        let expr = MockMemberExpressionDefinition::builder()
            .expression_name(Some("rolling_revenue".to_string()))
            .name(Some("rollingRevenue".to_string()))
            .cube_name(Some("users".to_string()))
            .definition(Some("rolling revenue calculation".to_string()))
            .expression(MemberExpressionExpressionDef::Struct(
                expr_struct as Rc<dyn crate::cube_bridge::member_expression::ExpressionStruct>,
            ))
            .build();

        let static_data = expr.static_data();
        assert_eq!(
            static_data.expression_name,
            Some("rolling_revenue".to_string())
        );
        assert_eq!(
            static_data.definition,
            Some("rolling revenue calculation".to_string())
        );

        let result = expr.expression().unwrap();
        assert!(matches!(result, MemberExpressionExpressionDef::Struct(_)));
    }

    #[test]
    fn test_member_expression_minimal() {
        let sql = Rc::new(MockMemberSql::new("{CUBE.field}").unwrap());

        let expr = MockMemberExpressionDefinition::builder()
            .expression(MemberExpressionExpressionDef::Sql(
                sql as Rc<dyn crate::cube_bridge::member_sql::MemberSql>,
            ))
            .build();

        let static_data = expr.static_data();
        assert_eq!(static_data.expression_name, None);
        assert_eq!(static_data.name, None);
        assert_eq!(static_data.cube_name, None);
    }
}
