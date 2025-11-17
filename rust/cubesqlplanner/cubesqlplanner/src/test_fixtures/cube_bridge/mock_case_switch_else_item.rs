use crate::cube_bridge::case_switch_else_item::CaseSwitchElseItem;
use crate::cube_bridge::member_sql::MemberSql;
use crate::test_fixtures::cube_bridge::MockMemberSql;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, TypedBuilder)]
pub struct MockCaseSwitchElseItem {
    sql: String,
}

impl CaseSwitchElseItem for MockCaseSwitchElseItem {
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

    #[test]
    fn test_mock_case_switch_else_item() {
        let item = MockCaseSwitchElseItem::builder()
            .sql("{CUBE.default_value}".to_string())
            .build();

        assert!(item.sql().is_ok());
    }
}
