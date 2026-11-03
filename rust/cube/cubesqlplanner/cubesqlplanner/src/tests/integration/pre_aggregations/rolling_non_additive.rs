//! A rolling window over a non-additive measure aggregates raw rows across
//! the window, so a rollup storing the measure per day cannot serve it: the
//! distinct users of a week are not derivable from daily distinct counts.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use cubenativeutils::CubeError;
use indoc::indoc;

const YAML: &str = "common/rolling_count_distinct_pre_agg.yaml";
const SEED: &str = "rolling_count_distinct_pre_agg_tables.sql";

fn query(measure: &str) -> String {
    format!(
        indoc! {"
            measures:
              - rcd_sessions.{}
            time_dimensions:
              - dimension: rcd_sessions.created_at
                granularity: day
                dateRange:
                  - '2024-01-01'
                  - '2024-01-10'
        "},
        measure
    )
}

#[cfg(feature = "integration-postgres")]
async fn oracle(aggregate: &str) -> Vec<String> {
    use crate::test_fixtures::test_utils::integration_context::format_simple_query_results;
    use crate::test_fixtures::test_utils::pg_service::connect_and_seed;

    let client = connect_and_seed(SEED).await;
    let sql = format!(
        "SELECT d, {aggregate} AS v \
         FROM generate_series('2024-01-01'::timestamp, '2024-01-10'::timestamp, interval '1 day') AS d \
         LEFT JOIN rcd_sessions s ON s.created_at >= d AND s.created_at < d + interval '1 week' \
         GROUP BY d ORDER BY d"
    );
    let messages = client.simple_query(&sql).await.unwrap();
    measure_column(&format_simple_query_results(&messages))
}

// Last column of each row, ordered by the time column, so the comparison does
// not depend on how either side formats the day.
#[cfg(feature = "integration-postgres")]
fn measure_column(result: &str) -> Vec<String> {
    let mut rows = crate::test_fixtures::test_utils::table_rows(result);
    rows.sort();
    rows.into_iter()
        .map(|row| row.last().cloned().unwrap_or_default())
        .collect()
}

async fn assert_rolling_measure(measure: &str, oracle_aggregate: &str) -> Result<(), CubeError> {
    let ctx = TestContext::new(MockSchema::from_yaml_file(YAML))?;
    let query = query(measure);

    #[cfg(feature = "integration-postgres")]
    {
        let result = ctx.try_execute_pg(&query, SEED).await.unwrap();
        assert_eq!(
            measure_column(&result),
            oracle(oracle_aggregate).await,
            "result:\n{result}"
        );
    }
    #[cfg(not(feature = "integration-postgres"))]
    let _ = oracle_aggregate;

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(&query)?;
    assert!(
        pre_aggrs.is_empty(),
        "`daily` stores `{measure}` per day and cannot serve a weekly window. \
         Generated SQL:\n{sql}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_count_distinct_skips_daily_rollup() -> Result<(), CubeError> {
    assert_rolling_measure("weekly_users", "COUNT(DISTINCT s.user_id)").await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_avg_skips_daily_rollup() -> Result<(), CubeError> {
    assert_rolling_measure("weekly_avg_amount", "AVG(s.amount)").await
}
