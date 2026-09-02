use crate::cube_bridge::case_switch_definition::CaseSwitchDefinition;
use crate::cube_bridge::case_switch_else_item::CaseSwitchElseItem;
use crate::cube_bridge::case_switch_item::CaseSwitchItem;
use crate::cube_bridge::member_sql::MemberSql;
use crate::test_fixtures::cube_bridge::yaml::case::YamlCaseSwitchDefinition;
use crate::test_fixtures::cube_bridge::{
    MockCaseSwitchElseItem, MockCaseSwitchItem, MockMemberSql,
};
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub struct MockCaseSwitchDefinition {
    switch: String,
    when: Vec<Rc<MockCaseSwitchItem>>,
    else_sql: Rc<MockCaseSwitchElseItem>,
}

impl MockCaseSwitchDefinition {
    pub fn from_yaml(yaml: &str) -> Result<Rc<Self>, CubeError> {
        let yaml_def: YamlCaseSwitchDefinition = serde_yaml::from_str(yaml)
            .map_err(|e| CubeError::user(format!("Failed to parse YAML: {}", e)))?;
        Ok(yaml_def.build())
    }
}

impl CaseSwitchDefinition for MockCaseSwitchDefinition {
    fn switch(&self) -> Result<Rc<dyn MemberSql>, CubeError> {
        Ok(Rc::new(MockMemberSql::new(&self.switch)?))
    }

    fn when(&self) -> Result<Vec<Rc<dyn CaseSwitchItem>>, CubeError> {
        Ok(self
            .when
            .iter()
            .map(|item| item.clone() as Rc<dyn CaseSwitchItem>)
            .collect())
    }

    fn else_sql(&self) -> Result<Rc<dyn CaseSwitchElseItem>, CubeError> {
        Ok(self.else_sql.clone() as Rc<dyn CaseSwitchElseItem>)
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;

    #[test]
    fn test_from_yaml() {
        let yaml = indoc! {"
            switch: \"{CUBE.currency}\"
            when:
              - value: USD
                sql: \"'dollars'\"
              - value: EUR
                sql: \"'euros'\"
            else:
              sql: \"'unknown'\"
        "};

        let case_switch = MockCaseSwitchDefinition::from_yaml(yaml).unwrap();

        assert!(case_switch.switch().is_ok());

        let when_result = case_switch.when().unwrap();
        assert_eq!(when_result.len(), 2);

        let else_result = case_switch.else_sql().unwrap();
        assert!(else_result.sql().is_ok());
    }

    #[test]
    fn test_mock_case_switch_definition() {
        let when_items = vec![
            Rc::new(
                MockCaseSwitchItem::builder()
                    .value("1".to_string())
                    .sql("{CUBE.active_sql}".to_string())
                    .build(),
            ),
            Rc::new(
                MockCaseSwitchItem::builder()
                    .value("0".to_string())
                    .sql("{CUBE.inactive_sql}".to_string())
                    .build(),
            ),
        ];

        let else_item = Rc::new(
            MockCaseSwitchElseItem::builder()
                .sql("{CUBE.default_sql}".to_string())
                .build(),
        );

        let case_switch = MockCaseSwitchDefinition::builder()
            .switch("{CUBE.status_code}".to_string())
            .when(when_items)
            .else_sql(else_item)
            .build();

        assert!(case_switch.switch().is_ok());

        let when_result = case_switch.when().unwrap();
        assert_eq!(when_result.len(), 2);

        let else_result = case_switch.else_sql().unwrap();
        assert!(else_result.sql().is_ok());
    }
}
