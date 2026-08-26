//! Tests that validate that complex but self-contained queries can be executed correctly by DF

use crate::compile::{
    test::{execute_query, init_testing_logger},
    DatabaseProtocol,
};

#[tokio::test]
async fn test_join_with_coercion() {
    init_testing_logger();

    insta::assert_snapshot!(execute_query(
        // language=PostgreSQL
        r#"
                WITH
                    t1 AS (
                        SELECT 1::int2 AS i1
                    ),
                    t2 AS (
                        SELECT 1::int4 AS i2
                    )
                    SELECT
                        *
                    FROM
                        t1 LEFT JOIN t2 ON (t1.i1 = t2.i2)
                "#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await
    .unwrap());
}

#[tokio::test]
async fn test_triple_join_with_coercion() {
    init_testing_logger();

    insta::assert_snapshot!(execute_query(
        // language=PostgreSQL
        r#"
                WITH
                    t1 AS (
                        SELECT 1::int2 AS i1
                    ),
                    t2 AS (
                        SELECT 1::int4 AS i2
                    ),
                    t3 AS (
                        SELECT 1::int8 AS i3
                    )
                    SELECT
                        *
                    FROM
                        t1
                            LEFT JOIN t2 ON (t1.i1 = t2.i2)
                            LEFT JOIN t3 ON (t3.i3 = t2.i2)
                "#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await
    .unwrap());
}

#[tokio::test]
async fn union_all_alias_mismatch() {
    init_testing_logger();

    // language=PostgreSQL
    let query = r#"
SELECT
    foo,
    bar
FROM (
    SELECT
        'foo' as foo,
        'bar' as bar
    UNION ALL
    SELECT
        'foo' as foo,
        'bar' as qux
) t
GROUP BY
    foo, bar
;
        "#;

    insta::assert_snapshot!(
        execute_query(query.to_string(), DatabaseProtocol::PostgreSQL,)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn union_all_ctes_with_type_coercion() {
    init_testing_logger();

    // language=PostgreSQL
    let query = r#"
WITH a AS (
    SELECT 1::bigint AS t LIMIT 1
), b AS (
    SELECT 2::bigint AS t LIMIT 1
), c AS (
    SELECT 3.5::float8 AS t LIMIT 1
)
SELECT 'A' AS l, t FROM a
UNION ALL
SELECT 'B' AS l, t FROM b
UNION ALL
SELECT 'C' AS l, t FROM c
;
        "#;

    insta::assert_snapshot!(
        execute_query(query.to_string(), DatabaseProtocol::PostgreSQL)
            .await
            .unwrap()
    );
}

/// See https://www.postgresql.org/docs/current/functions-math.html
#[tokio::test]
async fn test_round() {
    init_testing_logger();

    // language=PostgreSQL
    let query = r#"
SELECT
    round(42.4), -- 42
    round(42.4382, 2), -- 42.44
    round(1234.56, -1) -- 1230
;
        "#;

    insta::assert_snapshot!(
        execute_query(query.to_string(), DatabaseProtocol::PostgreSQL)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_date_part_interval() {
    init_testing_logger();

    // language=PostgreSQL
    let query = r#"
        SELECT
            DATE_PART('day', INTERVAL '1 year 2 month 3 day 4 hour 5 minute 6 second') AS d
        "#;

    insta::assert_snapshot!(
        execute_query(query.to_string(), DatabaseProtocol::PostgreSQL)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_numeric_math_scalar() {
    init_testing_logger();

    // language=PostgreSQL
    let query = r#"
        SELECT
            a % 2::numeric AS m
        FROM (
            SELECT
                5::numeric AS a
            UNION ALL
            SELECT
                3.5::numeric AS a
        ) AS t
        "#;

    insta::assert_snapshot!(
        execute_query(query.to_string(), DatabaseProtocol::PostgreSQL)
            .await
            .unwrap()
    );
}

/// Reproduces https://github.com/cube-js/cube/issues/11653
///
/// `CaseExpr::return_type` in the DataFusion fork resolves to the type of the first
/// non-NULL THEN branch, but `case_when_no_expr` (the `CASE WHEN cond THEN ...` form)
/// passes every later THEN branch to `if_then_else` without casting it to that type -
/// unlike `case_when_with_expr` (the `CASE expr WHEN value THEN ...` form), which does
/// cast. Nothing coerces the branches at planning time either, so a CASE whose THEN
/// branches have different types panics at execution with
/// `true_values downcast failed to array::Int64Array`, which tears down the whole
/// Postgres connection instead of surfacing a SQL error.
///
/// Ignored until the fork coerces THEN branches to the CASE return type.
#[tokio::test]
#[ignore]
async fn test_case_with_heterogeneous_then_types() {
    init_testing_logger();

    // language=PostgreSQL
    let query = r#"
        SELECT
            CASE
                WHEN i > 1 THEN 0
                WHEN i > 0 THEN i
            END AS c
        FROM (SELECT 1::int4 AS i) AS t
        "#;

    let result = execute_query(query.to_string(), DatabaseProtocol::PostgreSQL)
        .await
        .unwrap();

    assert!(result.contains("1"), "unexpected result: {}", result);
}

/// Reproduces https://github.com/cube-js/cube/issues/11653 with the shape a BI tool's
/// ODBC/JDBC driver emits while discovering columns: a CASE over `pg_catalog` mixing an
/// integer literal (Int64) with an OID column (UInt32) across its THEN branches.
///
/// Ignored until the fork coerces THEN branches to the CASE return type.
#[tokio::test]
#[ignore]
async fn test_case_with_heterogeneous_then_types_pg_catalog() {
    init_testing_logger();

    // language=PostgreSQL
    let query = r#"
        SELECT
            CASE
                WHEN t.typtype = 'd' THEN 0
                WHEN t.typtype = 'b' THEN t.typbasetype
                ELSE 0
            END AS base
        FROM pg_catalog.pg_type t
        "#;

    execute_query(query.to_string(), DatabaseProtocol::PostgreSQL)
        .await
        .unwrap();
}
