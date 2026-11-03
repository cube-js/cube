use crate::cube_bridge::case_context_item::CaseContextItem;
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
}

impl CaseContextItem for MockCaseContextItem {
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
