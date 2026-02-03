use crate::test_fixtures::cube_bridge::MockPreAggregationDescription;
use serde::Deserialize;
use std::rc::Rc;

#[derive(Debug, Deserialize)]
pub struct YamlPreAggregationDefinition {
    #[serde(rename = "type", default = "default_type")]
    pre_aggregation_type: String,
    #[serde(default)]
    granularity: Option<String>,
    #[serde(default)]
    sql_alias: Option<String>,
    #[serde(default)]
    external: Option<bool>,
    #[serde(default)]
    allow_non_strict_date_range_match: Option<bool>,
    #[serde(default)]
    measures: Option<Vec<String>>,
    #[serde(default)]
    dimensions: Option<Vec<String>>,
    #[serde(default)]
    time_dimension: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    segments: Option<Vec<String>>,
    #[serde(default)]
    #[allow(dead_code)]
    partition_granularity: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    refresh_key: Option<YamlRefreshKey>,
    #[serde(default)]
    #[allow(dead_code)]
    scheduled_refresh: Option<bool>,
    #[serde(default)]
    #[allow(dead_code)]
    incremental: Option<bool>,
    #[serde(default)]
    #[allow(dead_code)]
    build_range_start: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    build_range_end: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    use_original_sql_pre_aggregations: Option<bool>,
    #[serde(default)]
    #[allow(dead_code)]
    union_with_source_data: Option<bool>,
    #[serde(default)]
    #[allow(dead_code)]
    indexes: Option<Vec<YamlIndex>>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct YamlRefreshKey {
    #[serde(default)]
    sql: Option<String>,
    #[serde(default)]
    every: Option<String>,
    #[serde(default)]
    update_window: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct YamlIndex {
    name: String,
    columns: Vec<String>,
}

fn default_type() -> String {
    "rollup".to_string()
}

impl YamlPreAggregationDefinition {
    pub fn build(self, name: String) -> Rc<MockPreAggregationDescription> {
        let measure_references = self.measures.map(|m| format_member_references(&m));
        let dimension_references = self.dimensions.map(|d| format_member_references(&d));
        let time_dimension_reference = self.time_dimension.map(|td| format!("{{{}}}", td));

        Rc::new(
            MockPreAggregationDescription::builder()
                .name(name)
                .pre_aggregation_type(self.pre_aggregation_type)
                .granularity(self.granularity)
                .sql_alias(self.sql_alias)
                .external(self.external)
                .allow_non_strict_date_range_match(self.allow_non_strict_date_range_match)
                .measure_references_opt(measure_references)
                .dimension_references_opt(dimension_references)
                .time_dimension_reference_opt(time_dimension_reference)
                .build(),
        )
    }
}

fn format_member_references(members: &[String]) -> String {
    members
        .iter()
        .map(|m| format!("{{{}}}", m))
        .collect::<Vec<_>>()
        .join(", ")
}
