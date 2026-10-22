//! A partitioned pre-aggregation is built with `__FROM_PARTITION_RANGE` and
//! `__TO_PARTITION_RANGE` in place of its date range. The orchestrator swaps
//! them only in the bound params, so they must never reach the SQL text.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

#[test]
fn test_time_series_binds_partition_range() {
    let ctx = TestContext::new_with_generated_time_series(MockSchema::from_yaml_file(
        "common/rolling_window_partition_range.yaml",
    ))
    .unwrap();

    // `projects.id` multiplies `events`, so `running_count` turns into a
    // count distinct and is planned over a generated time series.
    let query = indoc! {r#"
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
              - "__FROM_PARTITION_RANGE"
              - "__TO_PARTITION_RANGE"
    "#};

    let (sql, params) = ctx.build_sql_and_params(query).unwrap();

    assert!(
        !sql.contains("__FROM_PARTITION_RANGE") && !sql.contains("__TO_PARTITION_RANGE"),
        "partition range placeholder rendered into the SQL text:\n{sql}\nparams: {params:?}"
    );
}
