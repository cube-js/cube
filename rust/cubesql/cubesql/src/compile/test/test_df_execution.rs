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
