use crate::cube_bridge::member_expression::{ExpressionStruct, ExpressionStructStatic};
use crate::cube_bridge::struct_with_sql_member::StructWithSqlMember;
use crate::impl_static_data;
use crate::test_fixtures::cube_bridge::MockStructWithSqlMember;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub struct MockExpressionStruct {
    expression_type: String,
    #[builder(default)]
    source_measure: Option<String>,
    #[builder(default)]
    replace_aggregation_type: Option<String>,

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
