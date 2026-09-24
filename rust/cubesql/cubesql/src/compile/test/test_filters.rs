use cubeclient::models::{
    V1LoadRequestQuery, V1LoadRequestQueryFilterItem, V1LoadRequestQueryTimeDimension,
};
use datafusion::physical_plan::displayable;
use pretty_assertions::assert_eq;

use crate::compile::{
    rewrite::rewriter::Rewriter,
    test::{
        convert_select_to_query_plan, convert_select_to_query_plan_customized, execute_query,
        init_testing_logger, utils::LogicalPlanTestUtils,
    },
    DatabaseProtocol,
};

#[tokio::test]
async fn test_filter_date_greated_and_not_null() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
SELECT
    dim_str0
FROM MultiTypeCube
WHERE
      (dim_date0 IS NOT NULL)
  AND (dim_date0 > '2019-01-01 00:00:00')
GROUP BY
    dim_str0
;
"#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await;

    let logical_plan = query_plan.as_logical_plan();
    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec![]),
            dimensions: Some(vec!["MultiTypeCube.dim_str0".to_string()]),
            segments: Some(vec![]),
            order: Some(vec![]),
            filters: Some(vec![
                V1LoadRequestQueryFilterItem {
                    member: Some("MultiTypeCube.dim_date0".to_string()),
                    operator: Some("set".to_string()),
                    values: None,
                    or: None,
                    and: None,
                },
                V1LoadRequestQueryFilterItem {
                    member: Some("MultiTypeCube.dim_date0".to_string()),
                    operator: Some("afterDate".to_string()),
                    values: Some(vec!["2019-01-01T00:00:00.000Z".to_string()]),
                    or: None,
                    and: None,
                },
            ],),
            ..Default::default()
        }
    );
}

#[tokio::test]
async fn test_filter_dim_in_null() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
        SELECT
            dim_str0
        FROM
            MultiTypeCube
        WHERE dim_str1 IN (NULL)
        "#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await;

    let physical_plan = query_plan.as_physical_plan().await.unwrap();
    println!(
        "Physical plan: {}",
        displayable(physical_plan.as_ref()).indent()
    );

    // For now this tests only that query is rewritable
    // TODO support this as "notSet" filter

    assert!(query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains(r#"\"sql\":\"${MultiTypeCube.dim_str1} IN (NULL)\""#));
}

#[tokio::test]
async fn test_filter_superset_is_null() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
SELECT dim_str0 FROM MultiTypeCube WHERE (dim_str1 IS NULL OR dim_str1 IN (NULL) AND (1<>1))
        "#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await;

    let physical_plan = query_plan.as_physical_plan().await.unwrap();
    println!(
        "Physical plan: {}",
        displayable(physical_plan.as_ref()).indent()
    );

    // For now this tests only that query is rewritable
    // TODO support this as "notSet" filter

    assert!(query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains(r#"\"sql\":\"((${MultiTypeCube.dim_str1} IS NULL) OR (${MultiTypeCube.dim_str1} IN (NULL) AND FALSE))\""#));
}

/// Single filter in CubeScan does not support both measuser in dimensions, so it should not get pushed to CubeScan
#[tokio::test]
async fn test_mixed_filters() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
SELECT
    dim_str0,
    avgPrice
FROM (
    SELECT
        dim_str0,
        AVG(avgPrice) AS avgPrice
    FROM
        MultiTypeCube
    GROUP BY 1
) t
WHERE
    avgPrice > 1
    OR (
        avgPrice = 1
        AND
        dim_str0 = 'completed'
    )
;
        "#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await;

    let physical_plan = query_plan.as_physical_plan().await.unwrap();
    println!(
        "Physical plan: {}",
        displayable(physical_plan.as_ref()).indent()
    );

    let logical_plan = query_plan.as_logical_plan();
    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec!["MultiTypeCube.avgPrice".to_string()]),
            dimensions: Some(vec!["MultiTypeCube.dim_str0".to_string()]),
            segments: Some(vec![]),
            order: Some(vec![]),
            filters: None,
            ..Default::default()
        }
    );
}

/// HAVING on a measure combined with ORDER BY on the same measure used to leave
/// a raw `measure()` aggregate in the Sort above the rewritten CubeScan
/// ("Physical plan does not support logical expression measure(...)").
#[tokio::test]
async fn test_measure_having_and_order_by_measure() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
SELECT
    customer_gender,
    notes,
    DATE_TRUNC('month', order_date) AS order_date_month,
    MEASURE(sumPrice)
FROM KibanaSampleDataEcommerce
WHERE
    order_date >= '2026-01-01'
    AND order_date <= '2026-06-26'
    AND customer_gender IN ('male', 'female')
GROUP BY 1, 2, 3
HAVING
    MEASURE(sumPrice) IS NOT NULL
    AND MEASURE(sumPrice) != 0
ORDER BY MEASURE(sumPrice) DESC
LIMIT 5000
;
"#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await;

    // The whole query must be pushed to a single CubeScan; before the fix
    // physical planning failed on the leftover Sort node.
    let physical_plan = query_plan.as_physical_plan().await.unwrap();
    println!(
        "Physical plan: {}",
        displayable(physical_plan.as_ref()).indent()
    );

    assert_eq!(
        query_plan.as_logical_plan().find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec!["KibanaSampleDataEcommerce.sumPrice".to_string()]),
            dimensions: Some(vec![
                "KibanaSampleDataEcommerce.customer_gender".to_string(),
                "KibanaSampleDataEcommerce.notes".to_string(),
            ]),
            segments: Some(vec![]),
            time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                granularity: Some("month".to_string()),
                date_range: Some(serde_json::json!(vec![
                    "2026-01-01T00:00:00.000Z".to_string(),
                    "2026-06-26T00:00:00.000Z".to_string(),
                ])),
            }]),
            order: Some(vec![vec![
                "KibanaSampleDataEcommerce.sumPrice".to_string(),
                "desc".to_string(),
            ]]),
            limit: Some(5000),
            filters: Some(vec![
                V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["male".to_string(), "female".to_string()]),
                    or: None,
                    and: None,
                },
                V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.sumPrice".to_string()),
                    operator: Some("set".to_string()),
                    values: None,
                    or: None,
                    and: None,
                },
                V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.sumPrice".to_string()),
                    operator: Some("notEquals".to_string()),
                    values: Some(vec!["0".to_string()]),
                    or: None,
                    and: None,
                },
            ]),
            ..Default::default()
        }
    );
}

/// A date past 2262-04-11 has no `i64` nanosecond timestamp. Coercing such a bound to the
/// column's `TIMESTAMP` type used to overflow inside the Arrow cast kernel and abort the query;
/// the kernel now reports an error, normalization keeps the bound as a `DATE`, and the filter
/// pushes down with the date intact.
#[tokio::test]
async fn test_filter_date_beyond_nanosecond_range() {
    init_testing_logger();

    for (bound, expected) in [
        ("2262-04-11", "2262-04-11T00:00:00.000Z"),
        ("2262-04-12", "2262-04-12T00:00:00.000Z"),
        ("9999-12-31", "9999-12-31T00:00:00.000Z"),
        ("1600-01-01", "1600-01-01T00:00:00.000Z"),
    ] {
        // The `date` literal and the bare string take different normalization paths.
        for literal in [format!("date '{bound}'"), format!("'{bound}'")] {
            let query_plan = convert_select_to_query_plan(
                format!(
                    r#"
SELECT dim_str0
FROM MultiTypeCube
WHERE dim_date0 <= {literal}
GROUP BY dim_str0
"#
                ),
                DatabaseProtocol::PostgreSQL,
            )
            .await;

            assert_eq!(
                query_plan
                    .as_logical_plan()
                    .find_cube_scan()
                    .request
                    .filters,
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("MultiTypeCube.dim_date0".to_string()),
                    operator: Some("beforeOrOnDate".to_string()),
                    values: Some(vec![expected.to_string()]),
                    or: None,
                    and: None,
                }]),
                "{literal} must push down with the date preserved"
            );
        }
    }
}

/// `BETWEEN` and `IN` coerce their operands on separate paths.
#[tokio::test]
async fn test_filter_between_and_in_list_date_beyond_nanosecond_range() {
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        r#"
SELECT dim_str0
FROM MultiTypeCube
WHERE dim_date0 BETWEEN date '2020-01-01' AND date '9999-12-31'
GROUP BY dim_str0
"#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await;

    assert_eq!(
        query_plan
            .as_logical_plan()
            .find_cube_scan()
            .request
            .time_dimensions,
        Some(vec![V1LoadRequestQueryTimeDimension {
            dimension: "MultiTypeCube.dim_date0".to_string(),
            granularity: None,
            date_range: Some(serde_json::json!(vec![
                "2020-01-01T00:00:00.000Z".to_string(),
                "9999-12-31T00:00:00.000Z".to_string(),
            ])),
        }]),
    );

    let query_plan = convert_select_to_query_plan(
        r#"
SELECT dim_str0
FROM MultiTypeCube
WHERE dim_date0 IN (date '2020-01-01', date '9999-12-31')
GROUP BY dim_str0
"#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await;

    assert_eq!(
        query_plan
            .as_logical_plan()
            .find_cube_scan()
            .request
            .filters,
        Some(vec![V1LoadRequestQueryFilterItem {
            member: Some("MultiTypeCube.dim_date0".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec![
                "2020-01-01T00:00:00.000Z".to_string(),
                "9999-12-31T00:00:00.000Z".to_string(),
            ]),
            or: None,
            and: None,
        }]),
    );
}

/// `PlanNormalize` runs as `optimize(..).unwrap_or(plan)`, so a bound that fails to fold must
/// be declined locally rather than error out of the rule and take every other normalization
/// (here the `DATE - DATE` to `DATEDIFF` rewrite) with it.
#[tokio::test]
async fn test_filter_date_beyond_nanosecond_range_keeps_other_normalizations() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    for bound in [
        "dim_date0 <= date '9999-12-31'",
        "dim_date0 BETWEEN date '2020-01-01' AND date '9999-12-31'",
        "dim_date0 IN (date '2020-01-01', date '9999-12-31')",
    ] {
        let query_plan = convert_select_to_query_plan(
            format!(
                r#"
SELECT dim_str0
FROM MultiTypeCube
WHERE (dim_date1::date - dim_date2::date) > 3 AND {bound}
GROUP BY dim_str0
"#
            ),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let sql = query_plan
            .as_logical_plan()
            .find_cube_scan_wrapped_sql()
            .wrapped_sql
            .sql;
        assert!(
            sql.contains("DATEDIFF(day,"),
            "DATE - DATE rewrite must survive `{}`, got: {}",
            bound,
            sql
        );
    }
}

/// Casts the rewriter folds itself: a `TIMESTAMP` literal is parsed to nanoseconds by Arrow,
/// and a `DATE` cast of the column is coerced next to the date literal. Neither can be folded
/// past 2262-04-11, so both stay symbolic and are pushed down as SQL instead of panicking.
#[tokio::test]
async fn test_filter_date_beyond_nanosecond_range_in_unfoldable_casts() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    for (predicate, expected_sql) in [
        (
            "dim_date0 <= TIMESTAMP '9999-12-31 00:00:00'",
            "(${MultiTypeCube.dim_date0} <= CAST($1 AS TIMESTAMP))",
        ),
        (
            "dim_date0::date <= date '9999-12-31'",
            "(CAST(${MultiTypeCube.dim_date0} AS DATE) <= DATE('9999-12-31'))",
        ),
        (
            "dim_date0 <= date '9999-12-31' + interval '1 day'",
            "(${MultiTypeCube.dim_date0} <= CAST((DATE('9999-12-31') + INTERVAL '1 DAY') AS TIMESTAMP))",
        ),
    ] {
        let query_plan = convert_select_to_query_plan(
            format!(
                r#"
SELECT dim_str0
FROM MultiTypeCube
WHERE {predicate}
GROUP BY dim_str0
"#
            ),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let sql = query_plan
            .as_logical_plan()
            .find_cube_scan_wrapped_sql()
            .wrapped_sql
            .sql;
        assert!(
            sql.contains(expected_sql),
            "`{}` must push down as SQL, got: {}",
            predicate,
            sql
        );
    }
}

/// Shapes that don't become member filters (`<>`) are pushed down as SQL. There an out-of-range
/// bound must render as a timestamp literal, not a `DATE`: strict dialects (BigQuery) reject
/// comparing a `TIMESTAMP` with a `DATE`. A bound on the left is still a member filter.
#[tokio::test]
async fn test_filter_date_beyond_nanosecond_range_pushed_down_as_timestamp() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        r#"
SELECT dim_str0
FROM MultiTypeCube
WHERE date '9999-12-31' >= dim_date0
GROUP BY dim_str0
"#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await;
    assert_eq!(
        query_plan
            .as_logical_plan()
            .find_cube_scan()
            .request
            .filters,
        Some(vec![V1LoadRequestQueryFilterItem {
            member: Some("MultiTypeCube.dim_date0".to_string()),
            operator: Some("beforeOrOnDate".to_string()),
            values: Some(vec!["9999-12-31T00:00:00.000Z".to_string()]),
            or: None,
            and: None,
        }]),
    );

    for (predicate, expected_sql) in [
        (
            "dim_date0 <> date '9999-12-31'",
            "(${MultiTypeCube.dim_date0} != TIMESTAMP('9999-12-31T00:00:00.000Z'))",
        ),
        (
            "dim_date0 <> '9999-12-31'",
            "(${MultiTypeCube.dim_date0} != TIMESTAMP('9999-12-31T00:00:00.000Z'))",
        ),
    ] {
        let query_plan = convert_select_to_query_plan_customized(
            format!(
                r#"
SELECT dim_str0
FROM MultiTypeCube
WHERE {predicate}
GROUP BY dim_str0
"#
            ),
            DatabaseProtocol::PostgreSQL,
            vec![(
                "expressions/timestamp_literal".to_string(),
                "TIMESTAMP('{{ value }}')".to_string(),
            )],
        )
        .await;

        let sql = query_plan
            .as_logical_plan()
            .find_cube_scan_wrapped_sql()
            .wrapped_sql
            .sql;
        assert!(
            sql.contains(expected_sql),
            "`{}` must push down as a timestamp comparison, got: {}",
            predicate,
            sql
        );
    }
}

/// When DataFusion evaluates the comparison itself, the out-of-range bound is compared at
/// millisecond precision instead of being coerced back to nanoseconds, which would fail.
#[tokio::test]
async fn test_date_beyond_nanosecond_range_evaluated_by_datafusion() {
    init_testing_logger();

    assert_eq!(
        execute_query(
            "SELECT CAST('2020-01-01 00:00:00' AS TIMESTAMP) <= DATE '9999-12-31' AS lte"
                .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await
        .unwrap(),
        "+------+\n\
        | lte  |\n\
        +------+\n\
        | true |\n\
        +------+"
    );
}

/// `NOT (DATE(col) = '...')` is rewritten to `col < day_start OR col >= next_day_start`. The last
/// day a nanosecond timestamp reaches (2262-04-11) has no next day, so the rewrite must decline
/// instead of overflowing into a bound that matches every row.
#[tokio::test]
async fn test_filter_not_date_equals_last_nanosecond_day() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        r#"
SELECT dim_str0
FROM MultiTypeCube
WHERE NOT (DATE(dim_date0) = '2262-04-11')
GROUP BY dim_str0
"#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await;
    let sql = query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql;
    assert!(
        sql.contains("NOT ((DATE(${MultiTypeCube.dim_date0}) = DATE('2262-04-11')))"),
        "the predicate must be pushed down unchanged, got: {}",
        sql
    );

    let query_plan = convert_select_to_query_plan(
        r#"
SELECT dim_str0
FROM MultiTypeCube
WHERE NOT (DATE(dim_date0) = '2262-04-10')
GROUP BY dim_str0
"#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await;
    assert_eq!(
        query_plan
            .as_logical_plan()
            .find_cube_scan()
            .request
            .filters,
        Some(vec![V1LoadRequestQueryFilterItem {
            member: None,
            operator: None,
            values: None,
            or: Some(vec![
                serde_json::json!({
                    "member": "MultiTypeCube.dim_date0",
                    "operator": "beforeDate",
                    "values": ["2262-04-10T00:00:00.000Z"],
                }),
                serde_json::json!({
                    "member": "MultiTypeCube.dim_date0",
                    "operator": "afterOrOnDate",
                    "values": ["2262-04-11T00:00:00.000Z"],
                }),
            ]),
            and: None,
        }]),
    );
}

/// `date_trunc` equality and `IN` on a date past 2262-04-11 become a date range bounded by
/// millisecond timestamps, as a plain comparison does.
#[tokio::test]
async fn test_filter_date_trunc_date_beyond_nanosecond_range() {
    init_testing_logger();

    for (predicate, expected_range) in [
        (
            "date_trunc('day', dim_date0) = DATE '9999-12-31'",
            ["9999-12-31T00:00:00.000Z", "9999-12-31T23:59:59.999Z"],
        ),
        (
            "date_trunc('day', dim_date0) IN (DATE '9999-12-30', DATE '9999-12-31')",
            ["9999-12-30T00:00:00.000Z", "9999-12-31T23:59:59.999Z"],
        ),
    ] {
        let query_plan = convert_select_to_query_plan(
            format!(
                r#"
SELECT dim_str0
FROM MultiTypeCube
WHERE {predicate}
GROUP BY dim_str0
"#
            ),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        assert_eq!(
            query_plan
                .as_logical_plan()
                .find_cube_scan()
                .request
                .time_dimensions,
            Some(vec![V1LoadRequestQueryTimeDimension {
                dimension: "MultiTypeCube.dim_date0".to_string(),
                granularity: None,
                date_range: Some(serde_json::json!(expected_range)),
            }]),
            "{}",
            predicate
        );
    }
}
