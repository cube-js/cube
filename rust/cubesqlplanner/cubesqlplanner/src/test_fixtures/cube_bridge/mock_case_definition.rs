use crate::cube_bridge::case_definition::CaseDefinition;
use crate::cube_bridge::case_else_item::CaseElseItem;
use crate::cube_bridge::case_item::CaseItem;
use crate::test_fixtures::cube_bridge::{MockCaseElseItem, MockCaseItem};
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub struct MockCaseDefinition {
    when: Vec<Rc<MockCaseItem>>,
    else_label: Rc<MockCaseElseItem>,
}

impl CaseDefinition for MockCaseDefinition {
    fn when(&self) -> Result<Vec<Rc<dyn CaseItem>>, CubeError> {
        Ok(self
            .when
            .iter()
            .map(|item| item.clone() as Rc<dyn CaseItem>)
            .collect())
    }

    fn else_label(&self) -> Result<Rc<dyn CaseElseItem>, CubeError> {
        Ok(self.else_label.clone() as Rc<dyn CaseElseItem>)
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cube_bridge::string_or_sql::StringOrSql;

    #[test]
    fn test_mock_case_definition() {
        let when_items = vec![
            Rc::new(
                MockCaseItem::builder()
                    .sql("{CUBE.status} = 'active'".to_string())
                    .label(StringOrSql::String("Active".to_string()))
                    .build(),
            ),
            Rc::new(
                MockCaseItem::builder()
                    .sql("{CUBE.status} = 'inactive'".to_string())
                    .label(StringOrSql::String("Inactive".to_string()))
                    .build(),
            ),
        ];

        let else_item = Rc::new(
            MockCaseElseItem::builder()
                .label(StringOrSql::String("Unknown".to_string()))
                .build(),
        );

        let case_def = MockCaseDefinition::builder()
            .when(when_items)
            .else_label(else_item)
            .build();

        let when_result = case_def.when().unwrap();
        assert_eq!(when_result.len(), 2);

        let else_result = case_def.else_label().unwrap();
        assert!(else_result.label().is_ok());
    }
}
