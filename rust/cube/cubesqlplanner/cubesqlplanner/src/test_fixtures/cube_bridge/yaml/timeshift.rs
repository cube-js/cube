use crate::test_fixtures::cube_bridge::MockTimeShiftDefinition;
use serde::Deserialize;
use std::rc::Rc;

#[derive(Debug, Deserialize)]
pub struct YamlTimeShiftDefinition {
    #[serde(default)]
    interval: Option<String>,
    #[serde(rename = "type", default)]
    timeshift_type: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    sql: Option<String>,
}

impl YamlTimeShiftDefinition {
    pub fn build(self) -> Rc<MockTimeShiftDefinition> {
        let result = MockTimeShiftDefinition::builder()
            .interval(self.interval)
            .timeshift_type(self.timeshift_type)
            .name(self.name)
            .sql_opt(self.sql)
            .build();

        Rc::new(result)
    }
}
