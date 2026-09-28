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
        Rc::new(
            MockSegmentDefinition::builder()
                .segment_type(self.segment_type)
                .sql(self.sql)
                .build(),
        )
    }
}
