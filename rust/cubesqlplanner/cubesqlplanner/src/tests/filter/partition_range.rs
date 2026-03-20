use super::assert_filter;
use crate::test_fixtures::cube_bridge::{MockDriverTools, MockSchema};
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn build(filter_yaml: &str) -> (String, Vec<String>) {
    super::build_filter("common/visitors.yaml", filter_yaml)
}

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
fn test_in_date_range_from_partition_range() {
    let result = build(indoc! {r#"
            filters:
              - dimension: visitors.created_at
                operator: inDateRange
                values:
                  - "__FROM_PARTITION_RANGE"
                  - "2024-12-31"
        "#});
    // __FROM_PARTITION_RANGE skips formatting and tz conversion but still gets allocated + cast
    assert_filter(
        &result,
        r#"("visitors".created_at >= $_0_$::timestamptz AND "visitors".created_at <= $_1_$::timestamptz)"#,
        &["__FROM_PARTITION_RANGE", "2024-12-31T23:59:59.999"],
    );
}

#[test]
fn test_in_date_range_to_partition_range() {
    let result = build(indoc! {r#"
            filters:
              - dimension: visitors.created_at
                operator: inDateRange
                values:
                  - "2024-01-01"
                  - "__TO_PARTITION_RANGE"
        "#});
    assert_filter(
        &result,
        r#"("visitors".created_at >= $_0_$::timestamptz AND "visitors".created_at <= $_1_$::timestamptz)"#,
        &["2024-01-01T00:00:00.000", "__TO_PARTITION_RANGE"],
    );
}

#[test]
fn test_in_date_range_both_partition_range() {
    let result = build(indoc! {r#"
            filters:
              - dimension: visitors.created_at
                operator: inDateRange
                values:
                  - "__FROM_PARTITION_RANGE"
                  - "__TO_PARTITION_RANGE"
        "#});
    assert_filter(
        &result,
        r#"("visitors".created_at >= $_0_$::timestamptz AND "visitors".created_at <= $_1_$::timestamptz)"#,
        &["__FROM_PARTITION_RANGE", "__TO_PARTITION_RANGE"],
    );
}

#[test]
fn test_not_in_date_range_partition_range() {
    let result = build(indoc! {r#"
            filters:
              - dimension: visitors.created_at
                operator: notInDateRange
                values:
                  - "__FROM_PARTITION_RANGE"
                  - "__TO_PARTITION_RANGE"
        "#});
    assert_filter(
        &result,
        r#"("visitors".created_at < $_0_$::timestamptz OR "visitors".created_at > $_1_$::timestamptz)"#,
        &["__FROM_PARTITION_RANGE", "__TO_PARTITION_RANGE"],
    );
}

#[test]
fn test_before_date_partition_range() {
    let result = build(indoc! {r#"
            filters:
              - dimension: visitors.created_at
                operator: beforeDate
                values:
                  - "__TO_PARTITION_RANGE"
        "#});
    assert_filter(
        &result,
        r#"("visitors".created_at < $_0_$::timestamptz)"#,
        &["__TO_PARTITION_RANGE"],
    );
}

#[test]
fn test_after_or_on_date_partition_range() {
    let result = build(indoc! {r#"
            filters:
              - dimension: visitors.created_at
                operator: afterOrOnDate
                values:
                  - "__FROM_PARTITION_RANGE"
        "#});
    assert_filter(
        &result,
        r#"("visitors".created_at >= $_0_$::timestamptz)"#,
        &["__FROM_PARTITION_RANGE"],
    );
}

// ── partition range + db timezone ──────────────────────────────────────────
// Partition range values must skip tz conversion even when db timezone is enabled

#[test]
fn test_partition_range_skips_db_timezone() {
    let result = build_with_visible_tz(indoc! {r#"
        filters:
          - dimension: visitors.created_at
            operator: inDateRange
            values:
              - "__FROM_PARTITION_RANGE"
              - "__TO_PARTITION_RANGE"
    "#});
    // Regular dates get tz conversion, but partition range values must NOT
    assert_filter(
        &result,
        r#"("visitors".created_at >= $_0_$::timestamptz AND "visitors".created_at <= $_1_$::timestamptz)"#,
        &["__FROM_PARTITION_RANGE", "__TO_PARTITION_RANGE"],
    );
}

#[test]
fn test_partition_range_mixed_with_regular_date_and_tz() {
    let result = build_with_visible_tz(indoc! {r#"
        filters:
          - dimension: visitors.created_at
            operator: inDateRange
            values:
              - "__FROM_PARTITION_RANGE"
              - "2024-12-31"
    "#});
    // FROM: partition range — no tz conversion, no formatting
    // TO: regular date — gets formatted and tz-converted
    assert_filter(
        &result,
        r#"("visitors".created_at >= $_0_$::timestamptz AND "visitors".created_at <= $_1_$::timestamptz)"#,
        &["__FROM_PARTITION_RANGE", "2025-01-01T07:59:59.999"],
    );
}
