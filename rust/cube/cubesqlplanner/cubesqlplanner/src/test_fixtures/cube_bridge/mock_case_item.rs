use crate::cube_bridge::case_item::CaseItem;
use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::string_or_sql::StringOrSql;
use crate::test_fixtures::cube_bridge::MockMemberSql;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, TypedBuilder)]
pub struct MockCaseItem {
    sql: String,
    label: StringOrSql,
}

impl CaseItem for MockCaseItem {
    fn sql(&self) -> Result<Rc<dyn MemberSql>, CubeError> {
        Ok(Rc::new(MockMemberSql::new(&self.sql)?))
    }

    fn label(&self) -> Result<StringOrSql, CubeError> {
        Ok(self.label.clone())
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_case_item() {
        let item = MockCaseItem::builder()
            .sql("{CUBE.status} = 'active'".to_string())
            .label(StringOrSql::String("Active".to_string()))
            .build();

        assert!(item.sql().is_ok());
        assert!(matches!(item.label().unwrap(), StringOrSql::String(_)));
    }
}
