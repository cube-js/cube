use crate::test_fixtures::cube_bridge::MockSegmentDefinition;
use serde::Deserialize;
use std::rc::Rc;

#[derive(Debug, Deserialize)]
pub struct YamlSegmentDefinition {
    #[serde(rename = "type", default)]
    segment_type: Option<String>,
    sql: String,
}

impl YamlSegmentDefinition {
    pub fn build(self) -> Rc<MockSegmentDefinition> {
        self.build_with_name(String::new())
    }

    pub fn build_with_name(self, name: String) -> Rc<MockSegmentDefinition> {
        Rc::new(
            MockSegmentDefinition::builder()
                .name(name)
                .segment_type(self.segment_type)
                .sql(self.sql)
                .build(),
        )
    }
}
