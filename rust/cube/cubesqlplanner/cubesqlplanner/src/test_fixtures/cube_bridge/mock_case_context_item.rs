use crate::cube_bridge::case_context_item::{CaseContextItem, CaseContextItemStatic};
use crate::cube_bridge::member_sql::MemberSql;
use crate::test_fixtures::cube_bridge::MockMemberSql;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, TypedBuilder)]
pub struct MockCaseContextItem {
    context: String,
    sql: String,
    #[builder(default)]
    context_references: Option<Vec<String>>,
}

impl_static_data!(
    MockCaseContextItem,
    CaseContextItemStatic,
    context_references
);

impl CaseContextItem for MockCaseContextItem {
    crate::impl_static_data_method!(CaseContextItemStatic);

    fn context(&self) -> Result<Rc<dyn MemberSql>, CubeError> {
        Ok(Rc::new(MockMemberSql::new(&self.context)?))
    }

    fn sql(&self) -> Result<Rc<dyn MemberSql>, CubeError> {
        Ok(Rc::new(MockMemberSql::new(&self.sql)?))
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}
