use datafusion::logical_plan::LogicalPlan;

use crate::{
    compile::{
        test::{init_testing_logger, LogicalPlanTestUtils, TestContext},
        DatabaseProtocol,
    },
    sql::CUBESQL_DISABLE_POST_PROCESSING_VAR,
};

/// A window function is never pushed down to Cube, so the plan keeps a `WindowAggr` over a
/// `CubeScan` that has no limit of its own - at execution time that scan is cut off at
/// `CUBEJS_DB_QUERY_LIMIT` rows and the window is computed over whatever made it through.
const UNBOUNDED_POST_PROCESSING_QUERY: &str = "SELECT customer_gender, SUM(count) OVER () \
     FROM KibanaSampleDataEcommerce GROUP BY customer_gender, count";

async fn context_with_post_processing_disabled() -> TestContext {
    init_testing_logger();
    let ctx = TestContext::new(DatabaseProtocol::PostgreSQL).await;
    ctx.set_session_flag(CUBESQL_DISABLE_POST_PROCESSING_VAR, true);
    ctx
}

/// Also pins where `truncated_post_processing_scans` sits in `CubePlanCost`: ranked above
/// `table_scans`, a plan that never detected a cube scan becomes the cheapest way to score zero
/// on that tier, and this query comes back as an unrejected `TableScan` plan instead of an error.
#[tokio::test]
async fn test_disable_post_processing_rejects_truncated_intermediate_result() {
    let ctx = context_with_post_processing_disabled().await;

    let err = ctx
        .convert_sql_to_cube_query(UNBOUNDED_POST_PROCESSING_QUERY)
        .await
        .expect_err("post-processing over a truncated intermediate result should be rejected");

    let err = err.to_string();
    assert!(
        err.contains("post-processing") && err.contains("50000"),
        "unexpected error: {}",
        err
    );
}

#[tokio::test]
async fn test_post_processing_allowed_by_default() {
    init_testing_logger();
    let ctx = TestContext::new(DatabaseProtocol::PostgreSQL).await;

    ctx.convert_sql_to_cube_query(UNBOUNDED_POST_PROCESSING_QUERY)
        .await
        .expect("post-processing should be allowed unless it was explicitly disabled");
}

/// There is nothing to truncate without a cube scan, so these stay allowed - unlike under the
/// blanket `cubesql_penalize_post_processing` used by `sql4sql`.
#[tokio::test]
async fn test_disable_post_processing_allows_query_without_cube_scan() {
    let ctx = context_with_post_processing_disabled().await;

    ctx.convert_sql_to_cube_query("SELECT COUNT(*) FROM information_schema.tables")
        .await
        .expect("a query that reads no cube should not be affected");
}

#[tokio::test]
async fn test_disable_post_processing_allows_full_pushdown() {
    let ctx = context_with_post_processing_disabled().await;

    let plan = ctx
        .convert_sql_to_cube_query(
            "SELECT customer_gender, AVG(avgPrice) FROM KibanaSampleDataEcommerce GROUP BY 1",
        )
        .await
        .expect("a query that needs no post-processing should not be affected");

    assert!(
        matches!(plan.as_logical_plan(), LogicalPlan::Extension(_)),
        "expected a plan with nothing on top of the scan, got: {}",
        plan.as_logical_plan().display_indent()
    );
}

/// The point of scoping the check to truncated intermediate results: post-processing that reads
/// every row it is supposed to can't silently produce a wrong answer, so it stays allowed.
#[tokio::test]
async fn test_disable_post_processing_allows_bounded_intermediate_result() {
    let ctx = context_with_post_processing_disabled().await;

    let plan = ctx
        .convert_sql_to_cube_query(
            "SELECT DATE_PART('doy', order_date) FROM KibanaSampleDataEcommerce LIMIT 100",
        )
        .await
        .expect("post-processing over a limited scan should not be affected");

    // Guards the point of the test: `datepart` is evaluated in memory, over a scan the `LIMIT`
    // was pushed into. If this ever becomes a full pushdown, the test stops proving anything.
    let plan = plan.as_logical_plan();
    assert!(
        matches!(plan, LogicalPlan::Projection(_)),
        "expected post-processing on top of the scan, got: {}",
        plan.display_indent()
    );
    assert_eq!(plan.find_cube_scan().request.limit, Some(100));
}

/// `Distinct` is an in-memory operator, but it is absent from the node list behind
/// `ast_size_outside_wrapper`, so deriving "is there post-processing above" from that list let
/// this shape through: `Distinct` sits directly on the scan, nothing else above it, and the
/// scan carries no limit. Answering it from the truncated first `CUBEJS_DB_QUERY_LIMIT` rows
/// drops distinct values, and the flag reported success while doing it.
///
/// Here the plan is salvageable - `DISTINCT` pushes into a wrapper - so the fix shows up as a
/// full pushdown rather than an error. Guarding on the plan shape rather than on `Ok` keeps the
/// test honest: `Ok` alone is exactly what the bug produced.
#[tokio::test]
async fn test_disable_post_processing_counts_distinct_as_post_processing() {
    let query = "SELECT DISTINCT MEASURE(count) FROM KibanaSampleDataEcommerce";

    {
        let ctx = TestContext::new(DatabaseProtocol::PostgreSQL).await;
        let plan = ctx.convert_sql_to_cube_query(query).await.unwrap();
        let plan = plan.as_logical_plan();
        assert!(
            matches!(plan, LogicalPlan::Distinct(_)),
            "expected the unguarded plan to post-process with Distinct, got: {}",
            plan.display_indent()
        );
        assert_eq!(
            plan.find_cube_scan().request.limit,
            None,
            "the scan feeding Distinct has to be unbounded for this test to mean anything"
        );
    }

    let ctx = context_with_post_processing_disabled().await;
    let plan = ctx
        .convert_sql_to_cube_query(query)
        .await
        .expect("DISTINCT pushes into a wrapper, so a safe plan exists");

    assert!(
        matches!(plan.as_logical_plan(), LogicalPlan::Extension(_)),
        "expected Distinct to be pushed down rather than run over a truncated scan, got: {}",
        plan.as_logical_plan().display_indent()
    );
}
