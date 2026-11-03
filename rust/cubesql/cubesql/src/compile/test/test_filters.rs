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

/// Filter over a literal column projected on top of a cube goes through the rewriter
/// rather than DataFusion filter push down, and must not be lost either.
#[tokio::test]
async fn test_filter_over_literal_column_of_cube() {
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
        SELECT c, customer_gender
        FROM (SELECT 'a' AS c, customer_gender FROM KibanaSampleDataEcommerce) AS t
        WHERE c = 'b'
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

    if !Rewriter::sql_push_down_enabled() {
        assert!(displayable(physical_plan.as_ref())
            .indent()
            .to_string()
            .contains("FilterExec: c@0 = b"));
        return;
    }

    let wrapped_sql = query_plan.as_logical_plan().find_cube_scan_wrapped_sql();
    assert_eq!(
        wrapped_sql.request.segments,
        Some(vec![
            r#"{"cubeName":"KibanaSampleDataEcommerce","alias":"t_c___utf8__b__","expr":{"type":"SqlFunction","cubeParams":[],"sql":"($0$ = $1$)"},"groupingSet":null}"#.to_string(),
        ])
    );
    assert_eq!(
        wrapped_sql.wrapped_sql.values,
        vec![
            Some("a".to_string()),
            Some("a".to_string()),
            Some("b".to_string())
        ]
    );
}

/// A date past 2262-04-11 has no nanosecond timestamp; normalization folds it to a millisecond
/// timestamp instead of aborting, and the filter pushes down with the date intact.
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
            "SELECT CAST('2020-01-01 00:00:00' AS TIMESTAMP) <= DATE '9999-12-31' AS lte, \
                CAST('2020-01-01 00:00:00' AS TIMESTAMP) \
                    BETWEEN DATE '2019-01-01' AND DATE '9999-12-31' AS between_bounds"
                .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await
        .unwrap(),
        "+------+----------------+\n\
        | lte  | between_bounds |\n\
        +------+----------------+\n\
        | true | true           |\n\
        +------+----------------+"
    );
}

/// `NOT (DATE(col) = '...')` is rewritten to `col < day_start OR col >= next_day_start`. Past
/// the nanosecond range the bounds become millisecond timestamps; 9999-12-31 has no next day a
/// member filter can name, so that one is pushed down as SQL.
#[tokio::test]
async fn test_filter_not_date_equals_beyond_nanosecond_range() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    for (date, next_day) in [
        ("2262-04-10", "2262-04-11"),
        ("2262-04-11", "2262-04-12"),
        ("9998-12-31", "9999-01-01"),
    ] {
        let query_plan = convert_select_to_query_plan(
            format!(
                r#"
SELECT dim_str0
FROM MultiTypeCube
WHERE NOT (DATE(dim_date0) = '{date}')
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
                member: None,
                operator: None,
                values: None,
                or: Some(vec![
                    serde_json::json!({
                        "member": "MultiTypeCube.dim_date0",
                        "operator": "beforeDate",
                        "values": [format!("{date}T00:00:00.000Z")],
                    }),
                    serde_json::json!({
                        "member": "MultiTypeCube.dim_date0",
                        "operator": "afterOrOnDate",
                        "values": [format!("{next_day}T00:00:00.000Z")],
                    }),
                ]),
                and: None,
            }]),
            "{}",
            date
        );
    }

    let query_plan = convert_select_to_query_plan(
        r#"
SELECT dim_str0
FROM MultiTypeCube
WHERE NOT (DATE(dim_date0) = '9999-12-31')
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
        sql.contains("NOT ((DATE(${MultiTypeCube.dim_date0}) = DATE('9999-12-31')))"),
        "the predicate must be pushed down unchanged, got: {}",
        sql
    );
}

/// `DATE(col) = '...'` past the nanosecond range becomes a date range, as it does in range.
#[tokio::test]
async fn test_filter_date_equals_date_str_beyond_nanosecond_range() {
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        r#"
SELECT dim_str0
FROM MultiTypeCube
WHERE DATE(dim_date0) = '9999-12-31'
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
            date_range: Some(serde_json::json!([
                "9999-12-31T00:00:00.000Z",
                "9999-12-31T23:59:59.999Z"
            ])),
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

/// https://github.com/cube-js/cube/issues/7880
/// Grafana's "All" option renders `('ALL' = 'ALL' OR col IN ('ALL'))`. The literal
/// comparison folds to TRUE, which leaves an empty `CubeScanFilters([])` inside the
/// filter list and the converter panics with "Expected filter but found CubeScanFilters([])".
#[tokio::test]
async fn test_filter_tautological_or_with_literal_comparison() {
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
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
"#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await;

    let logical_plan = query_plan.as_logical_plan();
    let request = logical_plan.find_cube_scan().request;
    assert_eq!(
        request.measures,
        Some(vec!["KibanaSampleDataEcommerce.count".to_string()])
    );
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
