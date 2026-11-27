//! Measures that declare the aggregate in `agg_type` and the data type in `type`

use crate::planner::{AggregationType, MeasureKind};
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn ctx() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/measure_agg_type.yaml");
    TestContext::new(schema).unwrap()
}

#[test]
fn agg_type_selects_the_measure_kind() {
    let ctx = ctx();

    let cases = [
        ("events.last_event_at", AggregationType::Max, "time", true),
        ("events.first_source", AggregationType::Min, "string", true),
        ("events.total_amount", AggregationType::Sum, "number", true),
        (
            "events.unique_users",
            AggregationType::CountDistinct,
            "number",
            false,
        ),
    ];
    for (path, expected, value_type, additive) in cases {
        let symbol = ctx.create_measure(path).unwrap();
        let measure = symbol.as_measure().unwrap();
        assert!(
            matches!(measure.kind(), MeasureKind::Aggregated(a) if a.agg_type() == expected),
            "{path} should aggregate"
        );
        assert!(!measure.is_calculated(), "{path}");
        assert_eq!(measure.value_type(), Some(value_type), "{path}");
        assert_eq!(measure.is_additive(), additive, "{path}");
    }

    let symbol = ctx.create_measure("events.rows_count").unwrap();
    let measure = symbol.as_measure().unwrap();
    assert!(matches!(measure.kind(), MeasureKind::Count(_)));
    assert_eq!(measure.value_type(), Some("number"));
}

#[test]
fn aggregate_named_by_type_has_no_value_type() {
    let ctx = ctx();
    let symbol = ctx.create_measure("events.legacy_max").unwrap();
    let measure = symbol.as_measure().unwrap();

    assert!(matches!(
        measure.kind(),
        MeasureKind::Aggregated(a) if a.agg_type() == AggregationType::Max
    ));
    assert_eq!(measure.value_type(), None);
}

#[test]
fn agg_type_wraps_sql_in_the_aggregate() {
    let query_yaml = indoc! {"
        measures:
          - events.last_event_at
          - events.first_source
          - events.rows_count
        dimensions:
          - events.created_at
    "};

    let sql = ctx().build_sql(query_yaml).expect("Should generate SQL");

    assert!(sql.contains("max(\"events\".created_at)"), "got: {sql}");
    assert!(sql.contains("min(\"events\".source)"), "got: {sql}");
    assert!(sql.contains("count("), "got: {sql}");
}

#[test]
fn agg_type_measure_matches_rollup() {
    let query_yaml = indoc! {"
        measures:
          - events.last_event_at
          - events.total_amount
        dimensions:
          - events.status
    "};

    let (sql, pre_aggrs) = ctx()
        .build_sql_with_used_pre_aggregations(query_yaml)
        .expect("Should generate SQL");

    assert_eq!(pre_aggrs.len(), 1, "got: {sql}");
    assert!(sql.contains("max("), "got: {sql}");
    assert!(sql.contains("sum("), "got: {sql}");
}

#[test]
fn agg_type_measure_through_view() {
    let query_yaml = indoc! {"
        measures:
          - events_view.last_event_at
        dimensions:
          - events_view.status
          - events_view.created_at
    "};

    let sql = ctx().build_sql(query_yaml).expect("Should generate SQL");

    assert!(sql.contains("max(\"events\".created_at)"), "got: {sql}");
    assert!(!sql.contains("max(max("), "got: {sql}");
}
