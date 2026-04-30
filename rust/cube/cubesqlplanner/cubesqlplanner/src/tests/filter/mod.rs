mod partition_range;
mod to_sql;
mod to_sql_timezone;
mod use_raw_values;

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;

pub fn build_filter(schema_file: &str, filter_yaml: &str) -> (String, Vec<String>) {
    let schema = MockSchema::from_yaml_file(schema_file);
    let ctx = TestContext::new(schema).unwrap();

    let query = format!("measures:\n  - visitors.count\n{}", filter_yaml);
    ctx.build_filter_sql(&query)
        .expect("Should generate filter SQL")
}

pub fn assert_filter(result: &(String, Vec<String>), expected_sql: &str, expected_params: &[&str]) {
    assert_eq!(result.0, expected_sql, "SQL mismatch");
    let params: Vec<&str> = result.1.iter().map(|s| s.as_str()).collect();
    assert_eq!(
        params, expected_params,
        "Params mismatch for SQL: {}",
        result.0
    );
}
