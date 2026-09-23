//! Reproductions of old GitHub bug reports for the SQL API.
//!
//! Each test asserts the CORRECT (Postgres-compatible) behavior, so it fails while the bug exists
//! and becomes a regression test once it is fixed.

use datafusion::arrow::datatypes::DataType;

use crate::compile::{
    rewrite::rewriter::Rewriter,
    test::{execute_query, init_testing_logger, utils::LogicalPlanTestUtils, TestContext},
    DatabaseProtocol,
};

/// https://github.com/cube-js/cube/issues/8156
/// `SELECT cte.field` fails when the CTE body is a UNION:
/// "No field named 'test.name'. Valid fields are 'name'."
/// Self-contained variant executed fully in DataFusion.
#[tokio::test]
async fn test_issue_8156_qualified_column_from_union_cte_df() {
    init_testing_logger();

    // language=PostgreSQL
    let query = r#"
        WITH test AS (
            SELECT 'a' AS name
            UNION
            SELECT 'b' AS name
        )
        SELECT test.name FROM test ORDER BY test.name
    "#;

    let result = execute_query(query.to_string(), DatabaseProtocol::PostgreSQL).await;
    let result = result.unwrap_or_else(|e| panic!("issue #8156 still reproduces: {}", e));
    assert!(
        result.contains("| a    |"),
        "unexpected result:\n{}",
        result
    );
    assert!(
        result.contains("| b    |"),
        "unexpected result:\n{}",
        result
    );
}

/// https://github.com/cube-js/cube/issues/8156
/// Same as above, but the UNION branches select from two cubes, as in the report.
#[tokio::test]
async fn test_issue_8156_qualified_column_from_union_cte_cubes() {
    init_testing_logger();

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
    let plan = context.convert_sql_to_cube_query(query).await;
    let plan = plan.unwrap_or_else(|e| panic!("issue #8156 still reproduces: {}", e));
    let logical_plan = plan.as_logical_plan();
    assert_eq!(logical_plan.find_cube_scans().len(), 2);
}

/// https://github.com/cube-js/cube/issues/8359
/// `SUM(CASE WHEN ... THEN 1 ELSE 0.0 END) / COUNT(*)` must be a fractional (numeric) division,
/// because `0.0` makes the CASE numeric. Postgres returns 0.5 here; the report says Cube gives 0/1.
/// Self-contained variant executed fully in DataFusion.
#[tokio::test]
async fn test_issue_8359_case_numeric_division_df() {
    init_testing_logger();

    // language=PostgreSQL
    let query = r#"
        SELECT
            SUM(CASE WHEN t.x = 1 THEN 1 ELSE 0.0 END) / COUNT(*) AS share
        FROM (
            SELECT 1 AS x
            UNION ALL
            SELECT 2 AS x
        ) t
    "#;

    let result = execute_query(query.to_string(), DatabaseProtocol::PostgreSQL)
        .await
        .unwrap();
    assert!(
        result.contains("0.5"),
        "issue #8359: expected fractional result 0.5, got:\n{}",
        result
    );

    // Simplified query from the issue comments
    // language=PostgreSQL
    let query = r#"
        SELECT SUM(CASE WHEN 1 = 1 THEN 1 ELSE 0.0 END) / 2 AS share
    "#;
    let result = execute_query(query.to_string(), DatabaseProtocol::PostgreSQL)
        .await
        .unwrap();
    assert!(
        result.contains("0.5"),
        "issue #8359: expected fractional result 0.5, got:\n{}",
        result
    );
}

/// https://github.com/cube-js/cube/issues/8359
/// Metabase-style "share" over a grouped subquery of a cube, with SQL push down.
/// The output column must not be typed as an integer.
#[tokio::test]
async fn test_issue_8359_case_numeric_division_push_down() {
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
    let logical_plan = plan.as_logical_plan();

    let field = logical_plan
        .schema()
        .field_with_unqualified_name("one order");
    let data_type = field.unwrap().data_type().clone();
    println!("issue #8359 push down output type: {:?}", data_type);
    let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
    println!("issue #8359 push down SQL: {}", sql);

    // The SQL sent to the data source must keep `0.0` fractional; otherwise the source DB
    // (Postgres/Redshift) computes SUM(int) / COUNT(*) as integer division and returns 0 or 1.
    // The Float64 literal is currently rendered with `format!("{f}")`, which prints `0.0` as `0`.
    assert!(
        !sql.contains("THEN 1 ELSE 0 END"),
        "issue #8359: float literal 0.0 was pushed down as integer 0, \
         making the division integer on the data source:\n{}",
        sql
    );

    assert!(
        !matches!(
            data_type,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
        ),
        "issue #8359: division result typed as integer: {:?}",
        data_type
    );
}

/// https://github.com/cube-js/cube/issues/8413
/// CTE used as `IN (SELECT ... FROM cte)` filter on a cube fails with
/// "No field named '__subquery-0.filter_1'".
#[tokio::test]
async fn test_issue_8413_cte_in_subquery_filter() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let context = TestContext::new(DatabaseProtocol::PostgreSQL).await;

    // Query as reported (SELECT *), adapted to the test cube
    // language=PostgreSQL
    let query = r#"
        WITH test_filter AS (
            SELECT 'male' AS filter_1
        )
        SELECT * FROM KibanaSampleDataEcommerce
        WHERE customer_gender IN (SELECT filter_1 FROM test_filter)
    "#;
    let plan = context.convert_sql_to_cube_query(query).await;
    let plan = plan.unwrap_or_else(|e| panic!("issue #8413 still reproduces (SELECT *): {}", e));
    let sql = plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql;
    assert!(sql.contains("IN (SELECT"), "unexpected SQL: {}", sql);
}

/// https://github.com/cube-js/cube/issues/8413
/// Number-typed variant from the maintainer's comment.
#[tokio::test]
async fn test_issue_8413_cte_in_subquery_filter_number() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let context = TestContext::new(DatabaseProtocol::PostgreSQL).await;

    // language=PostgreSQL
    let query = r#"
        WITH test_filter AS (
            SELECT 123 AS filter_1
        )
        SELECT * FROM KibanaSampleDataEcommerce
        WHERE id IN (SELECT filter_1 FROM test_filter)
    "#;
    let plan = context.convert_sql_to_cube_query(query).await;
    let plan = plan.unwrap_or_else(|e| panic!("issue #8413 still reproduces (number): {}", e));
    let sql = plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql;
    assert!(sql.contains("IN (SELECT"), "unexpected SQL: {}", sql);
}
