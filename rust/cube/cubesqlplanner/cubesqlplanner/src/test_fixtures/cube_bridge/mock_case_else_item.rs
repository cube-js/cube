use crate::cube_bridge::case_else_item::CaseElseItem;
use crate::cube_bridge::string_or_sql::StringOrSql;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, TypedBuilder)]
pub struct MockCaseElseItem {
    label: StringOrSql,
}

impl CaseElseItem for MockCaseElseItem {
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
    fn test_mock_case_else_item() {
        let item = MockCaseElseItem::builder()
            .label(StringOrSql::String("Unknown".to_string()))
            .build();

        assert!(matches!(item.label().unwrap(), StringOrSql::String(_)));
    }
}
