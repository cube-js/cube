use super::assert_filter;
use crate::test_fixtures::cube_bridge::{MockDriverTools, MockSchema};
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn build_with_visible_tz(filter_yaml: &str) -> (String, Vec<String>) {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let driver = MockDriverTools::with_timezone("America/Los_Angeles".to_string())
        .with_visible_in_db_time_zone();
    let base_tools = schema.create_base_tools_with_driver(driver).unwrap();
    let ctx = TestContext::new_with_base_tools(schema, base_tools).unwrap();

    let query = format!("measures:\n  - visitors.count\n{}", filter_yaml);
    ctx.build_filter_sql(&query)
        .expect("Should generate filter SQL")
}

#[test]
fn test_in_date_range_applies_in_db_time_zone() {
    let result = build_with_visible_tz(indoc! {r#"
        filters:
          - dimension: visitors.created_at
            operator: inDateRange
            values:
              - "2024-01-01"
              - "2024-12-31"
    "#});
    assert_filter(
        &result,
        r#"("visitors".created_at >= $_0_$::timestamptz AND "visitors".created_at <= $_1_$::timestamptz)"#,
        &["2024-01-01T08:00:00.000", "2025-01-01T07:59:59.999"],
    );
}

#[test]
fn test_before_date_applies_in_db_time_zone() {
    let result = build_with_visible_tz(indoc! {r#"
        filters:
          - dimension: visitors.created_at
            operator: beforeDate
            values:
              - "2024-06-01"
    "#});
    assert_filter(
        &result,
        r#"("visitors".created_at < $_0_$::timestamptz)"#,
        &["2024-06-01T07:00:00.000"],
    );
}

#[test]
fn test_non_date_filter_unaffected_by_in_db_time_zone() {
    let result = build_with_visible_tz(indoc! {"
        filters:
          - dimension: visitors.source
            operator: equals
            values:
              - google
    "});
    assert_filter(&result, r#"("visitors".source = $_0_$)"#, &["google"]);
}
