use crate::test_fixtures::cube_bridge::MockPreAggregationTimeDimension;
use serde::Deserialize;
use std::rc::Rc;

#[derive(Debug, Deserialize)]
pub struct YamlPreAggregationTimeDimension {
    dimension: String,
    granularity: String,
}

impl YamlPreAggregationTimeDimension {
    pub fn build(self) -> Rc<MockPreAggregationTimeDimension> {
        Rc::new(
            MockPreAggregationTimeDimension::builder()
                .dimension(self.dimension)
                .granularity(self.granularity)
                .build(),
        )
    }
}
