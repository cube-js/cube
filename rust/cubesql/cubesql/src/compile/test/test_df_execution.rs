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
