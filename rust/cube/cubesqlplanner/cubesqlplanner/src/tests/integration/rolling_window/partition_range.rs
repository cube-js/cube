//! A partitioned pre-aggregation is built with placeholders for its date range,
//! which the orchestrator replaces in the bound params only, never in the SQL text.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::{table_rows, TestContext};
use indoc::indoc;

const SEED: &str = "rolling_window_partition_range_tables.sql";

fn create_context() -> TestContext {
    TestContext::new_with_generated_time_series(MockSchema::from_yaml_file(
        "common/rolling_window_partition_range.yaml",
    ))
    .unwrap()
}

// `projects.id` multiplies `events`, so `running_count` turns into a count
// distinct, which is planned over a time series.
fn build_query(date_from: &str, date_to: &str) -> String {
    format!(
        indoc! {r#"
            measures:
              - events.count
              - events.running_count
            dimensions:
              - events.status
              - projects.id
            time_dimensions:
              - dimension: events.created_at
                granularity: day
                dateRange:
                  - "{}"
                  - "{}"
            order:
              - id: events.created_at
              - id: events.status
              - id: projects.id
            pre_aggregation_query: true
        "#},
        date_from, date_to
    )
}

#[test]
fn test_time_series_binds_partition_range() {
    let ctx = create_context();
    let query = build_query("__FROM_PARTITION_RANGE", "__TO_PARTITION_RANGE");

    let (sql, _) = ctx.build_sql_and_params(&query).unwrap();

    assert!(
        !sql.contains("__FROM_PARTITION_RANGE") && !sql.contains("__TO_PARTITION_RANGE"),
        "partition range placeholder rendered into the SQL text:\n{sql}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partition_build_matches_literal_range() {
    let ctx = create_context();
    let from = "2024-01-01T00:00:00.000";
    let to = "2024-01-04T23:59:59.999";

    let partition = ctx
        .try_execute_pg_partition(
            &build_query("__FROM_PARTITION_RANGE", "__TO_PARTITION_RANGE"),
            SEED,
            (from, to),
        )
        .await;
    let literal = ctx.try_execute_pg(&build_query(from, to), SEED).await;

    if let (Some(partition), Some(literal)) = (partition, literal) {
        assert!(
            table_rows(&literal).len() > 1,
            "literal range built no rows:\n{literal}"
        );
        assert_eq!(partition, literal);
    }
}

/// Without generated series the points are listed while planning, which an
/// unknown range rules out.
#[test]
fn test_partition_range_requires_generated_time_series() {
    let ctx = TestContext::new(MockSchema::from_yaml_file(
        "common/rolling_window_partition_range.yaml",
    ))
    .unwrap();
    let query = build_query("__FROM_PARTITION_RANGE", "__TO_PARTITION_RANGE");

    let err = ctx.build_sql_and_params(&query).unwrap_err();

    assert!(
        err.message.contains("generated time series"),
        "unexpected error: {}",
        err.message
    );
}
