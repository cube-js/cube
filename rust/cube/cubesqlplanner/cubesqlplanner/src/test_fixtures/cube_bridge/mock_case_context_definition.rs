use crate::cube_bridge::case_context_definition::CaseContextDefinition;
use crate::cube_bridge::case_context_item::CaseContextItem;
use crate::cube_bridge::case_switch_else_item::CaseSwitchElseItem;
use crate::test_fixtures::cube_bridge::yaml::case::YamlCaseContextDefinition;
use crate::test_fixtures::cube_bridge::{MockCaseContextItem, MockCaseSwitchElseItem};
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub struct MockCaseContextDefinition {
    when: Vec<Rc<MockCaseContextItem>>,
    else_sql: Rc<MockCaseSwitchElseItem>,
}

impl MockCaseContextDefinition {
    pub fn from_yaml(yaml: &str) -> Result<Rc<Self>, CubeError> {
        let yaml_def: YamlCaseContextDefinition = serde_yaml::from_str(yaml)
            .map_err(|e| CubeError::user(format!("Failed to parse YAML: {}", e)))?;
        Ok(yaml_def.build())
    }
}

impl CaseContextDefinition for MockCaseContextDefinition {
    fn when(&self) -> Result<Vec<Rc<dyn CaseContextItem>>, CubeError> {
        Ok(self
            .when
            .iter()
            .map(|item| item.clone() as Rc<dyn CaseContextItem>)
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
            when:
              - context: \"{QUERY_CONTEXT.grouped(CUBE.account_level9)}\"
                sql: \"{CUBE.amount_at_depth_3}\"
                context_references: [orders.account_level9]
              - context: \"{QUERY_CONTEXT.filtered(CUBE.depth)}\"
                sql: \"{CUBE.amount_raw}\"
            else:
              sql: \"{CUBE.amount_at_depth_0}\"
        "};

        let def = MockCaseContextDefinition::from_yaml(yaml).unwrap();
        let when = def.when().unwrap();
        assert_eq!(when.len(), 2);
        assert_eq!(
            when[0].static_data().context_references,
            Some(vec!["orders.account_level9".to_string()])
        );
        assert_eq!(when[1].static_data().context_references, None);
        assert!(when[0].context().is_ok());
        assert!(def.else_sql().is_ok());
    }
}
