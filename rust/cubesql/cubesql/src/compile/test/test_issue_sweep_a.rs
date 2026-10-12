//! Repro tests for open SQL API bug reports (sweep A).
//!
//! Each test asserts the correct (Postgres-compatible) behavior, so it fails while the bug
//! exists and becomes a regression test once it is fixed.

use pretty_assertions::assert_eq;

use crate::compile::{
    rewrite::rewriter::Rewriter,
    test::{execute_query, init_testing_logger, utils::LogicalPlanTestUtils, TestContext},
    DatabaseProtocol,
};

/// https://github.com/cube-js/cube/issues/7880
/// Grafana's "All" option renders `('ALL' = 'ALL' OR col IN ('ALL'))`. The literal comparison
/// folds to TRUE, which leaves an empty `CubeScanFilters([])` inside the filter list, and the
/// converter panics with "Expected filter but found CubeScanFilters([])".
#[tokio::test]
async fn test_issue_7880_tautological_or_with_literal_comparison() {
    init_testing_logger();

    let context = TestContext::new(DatabaseProtocol::PostgreSQL).await;
    // language=PostgreSQL
    let query = r#"
        SELECT
          date_trunc('month', order_date) AS "time",
          count(count) AS "Count"
        FROM KibanaSampleDataEcommerce
        WHERE
          order_date >= '2019-08-31T22:00:00Z'
          AND order_date <= '2024-03-07T13:39:28.923Z'
          AND ('ALL' = 'ALL' OR customer_gender IN ('ALL'))
        GROUP BY 1
        ORDER BY 1
    "#;

    let plan = context
        .convert_sql_to_cube_query(query)
        .await
        .unwrap_or_else(|e| panic!("issue #7880 still reproduces: {}", e));
    let request = plan.as_logical_plan().find_cube_scan().request;
    // The tautological OR group must not restrict customer_gender.
    let filters = request.filters.unwrap_or_default();
    assert!(
        filters
            .iter()
            .all(|f| f.member.as_deref() != Some("KibanaSampleDataEcommerce.customer_gender")),
        "unexpected customer_gender filter: {:?}",
        filters
    );
}

/// https://github.com/cube-js/cube/issues/8156
/// `SELECT cte.col` fails when the CTE body is a UNION:
/// "No field named 'test.name'. Valid fields are 'name'."
#[tokio::test]
async fn test_issue_8156_qualified_column_from_union_cte() {
    init_testing_logger();

    // Without cubes: executed fully in DataFusion
    // language=PostgreSQL
    let query = r#"
        WITH test AS (
            SELECT 'a' AS name
            UNION
            SELECT 'b' AS name
        )
        SELECT test.name FROM test ORDER BY test.name
    "#;
    let result = execute_query(query.to_string(), DatabaseProtocol::PostgreSQL)
        .await
        .unwrap_or_else(|e| panic!("issue #8156 still reproduces: {}", e));
    assert_eq!(
        result,
        "+------+\n| name |\n+------+\n| a    |\n| b    |\n+------+"
    );

    // As reported: UNION of two cubes
    // language=PostgreSQL
    let query = r#"
        WITH test AS (
            SELECT customer_gender AS name FROM KibanaSampleDataEcommerce
            UNION
            SELECT content AS name FROM Logs
        )
        SELECT test.name FROM test
    "#;
    let context = TestContext::new(DatabaseProtocol::PostgreSQL).await;
    context
        .convert_sql_to_cube_query(query)
        .await
        .unwrap_or_else(|e| panic!("issue #8156 still reproduces (cubes): {}", e));
}

/// https://github.com/cube-js/cube/issues/8359
/// `SUM(CASE WHEN ... THEN 1 ELSE 0.0 END) / COUNT(*)` must be a fractional division, since
/// `0.0` makes the CASE numeric. With SQL push down the Float64 literal `0.0` is rendered as
/// `0`, so the data source computes an integer SUM and an integer division (result 0 or 1).
#[tokio::test]
async fn test_issue_8359_case_float_literal_division_push_down() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    // language=PostgreSQL
    let query = r#"
        SELECT
            DATE_TRUNC('week', "source"."order_date") AS "order_date",
            SUM(
                CASE
                    WHEN "source"."count" = 1 THEN 1
                    ELSE 0.0
                END
            ) / COUNT(*) AS "one order"
        FROM (
            SELECT
                DATE_TRUNC('week', "KibanaSampleDataEcommerce"."order_date") AS "order_date",
                "KibanaSampleDataEcommerce"."customer_gender" AS "customer_gender",
                COUNT(DISTINCT "KibanaSampleDataEcommerce"."id") AS "count"
            FROM "KibanaSampleDataEcommerce"
            GROUP BY 1, 2
            ORDER BY 1 ASC, 2 ASC
        ) AS "source"
        GROUP BY 1
        ORDER BY 1 ASC
    "#;

    let context = TestContext::new(DatabaseProtocol::PostgreSQL).await;
    let plan = context.convert_sql_to_cube_query(query).await.unwrap();
    let sql = plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql;

    assert!(
        !sql.contains("THEN 1 ELSE 0 END"),
        "issue #8359: float literal 0.0 was pushed down as integer 0, \
         making the division integer on the data source:\n{}",
        sql
    );
}

/// https://github.com/cube-js/cube/issues/8413
/// `SELECT *` combined with `WHERE col IN (SELECT ... FROM cte)` fails with
/// "No field named '__subquery-0.filter_1'". An explicit column list works.
#[tokio::test]
async fn test_issue_8413_select_star_with_in_subquery_from_cte() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let context = TestContext::new(DatabaseProtocol::PostgreSQL).await;
    // language=PostgreSQL
    let query = r#"
        WITH test_filter AS (
            SELECT 'male' AS filter_1
        )
        SELECT * FROM KibanaSampleDataEcommerce
        WHERE customer_gender IN (SELECT filter_1 FROM test_filter)
    "#;
    let plan = context
        .convert_sql_to_cube_query(query)
        .await
        .unwrap_or_else(|e| panic!("issue #8413 still reproduces: {}", e));
    let sql = plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql;
    assert!(sql.contains("IN (SELECT"), "unexpected SQL: {}", sql);
}

/// https://github.com/cube-js/cube/issues/9379
/// A CASE over static comparisons whose branches are a time dimension and `DATE_TRUNC` of it
/// is not planned for a data source without a `functions/DATETRUNC` template (MySQL):
/// "Can't detect Cube query and it may be not supported yet". The same query is planned with
/// the default (Postgres-like) templates. `DATE_TRUNC` over a time dimension can be expressed
/// as the dimension's granularity, so it must not depend on the function template.
#[tokio::test]
async fn test_issue_9379_case_with_date_trunc_without_datetrunc_template() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    // language=PostgreSQL
    let query = r#"
        SELECT
          CASE
            WHEN 'a' = 'a' THEN KibanaSampleDataEcommerce.order_date
            WHEN 'a' = 'c' THEN (DATE_TRUNC('week', KibanaSampleDataEcommerce.order_date))
            WHEN 'a' = 'b' THEN (DATE_TRUNC('month', KibanaSampleDataEcommerce.order_date))
            ELSE NULL
          END AS "dynamic_date__0",
          MEASURE(KibanaSampleDataEcommerce.sumPrice) AS "sumPrice__sum__0"
        FROM KibanaSampleDataEcommerce
        GROUP BY 1
    "#;

    // MySQL templates (MysqlQuery.sqlTemplates) have no functions/DATETRUNC
    let context = TestContext::with_custom_templates(
        DatabaseProtocol::PostgreSQL,
        vec![("functions/DATETRUNC".to_string(), "".to_string())],
    )
    .await;
    context
        .convert_sql_to_cube_query(query)
        .await
        .unwrap_or_else(|e| panic!("issue #9379 still reproduces: {}", e));
}
