use crate::cube_bridge::member_expression::MemberExpressionDefinition;
use crate::cube_bridge::subquery_join::{SubqueryJoin, SubqueryJoinStatic};
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Mock implementation of `SubqueryJoin` for testing SQL-API grouped
/// sub-query joins (`subqueryJoins`).
#[derive(TypedBuilder)]
pub struct MockSubqueryJoin {
    sql: String,
    #[builder(default)]
    join_type: Option<String>,
    alias: String,
    // Trait field: the join ON condition, expressed as a member expression.
    on: Rc<dyn MemberExpressionDefinition>,
}

impl_static_data!(MockSubqueryJoin, SubqueryJoinStatic, sql, join_type, alias);

impl SubqueryJoin for MockSubqueryJoin {
    crate::impl_static_data_method!(SubqueryJoinStatic);

    fn on(&self) -> Result<Rc<dyn MemberExpressionDefinition>, CubeError> {
        Ok(self.on.clone())
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}
