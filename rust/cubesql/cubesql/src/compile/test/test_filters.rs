use cubeclient::models::{
    V1LoadRequestQuery, V1LoadRequestQueryFilterItem, V1LoadRequestQueryTimeDimension,
};
use datafusion::physical_plan::displayable;
use pretty_assertions::assert_eq;

use crate::compile::{
    rewrite::rewriter::Rewriter,
    test::{convert_select_to_query_plan, init_testing_logger, utils::LogicalPlanTestUtils},
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

/// A date literal past 2262-04-11 cannot be held by an i64 nanosecond timestamp. Coercing one to
/// `Timestamp(Nanosecond)` used to overflow arrow's unchecked multiply and abort the query; now
/// normalization declines instead, and the filter pushes down with the date intact.
#[tokio::test]
async fn test_filter_date_beyond_nanosecond_range_is_pushed_down() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    for (bound, expected) in [
        ("2262-04-12", "2262-04-12T00:00:00.000Z"),
        ("9999-12-31", "9999-12-31T00:00:00.000Z"),
    ] {
        let query_plan = convert_select_to_query_plan(
            // language=PostgreSQL
            format!(
                r#"
SELECT dim_str0
FROM MultiTypeCube
WHERE dim_date0 <= date '{bound}'
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
            "{bound} must push down with the date preserved"
        );
    }
}

/// The last nanosecond-representable date must still plan and push down normally — the guard
/// above must reject only what genuinely overflows.
#[tokio::test]
async fn test_filter_date_at_nanosecond_range_boundary_is_pushed_down() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
SELECT dim_str0
FROM MultiTypeCube
WHERE dim_date0 <= date '2262-04-11'
GROUP BY dim_str0
"#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await;

    let filters = query_plan
        .as_logical_plan()
        .find_cube_scan()
        .request
        .filters
        .unwrap_or_default();
    assert_eq!(
        filters
            .iter()
            .map(|filter| filter.operator.clone().unwrap_or_default())
            .collect::<Vec<_>>(),
        vec!["beforeOrOnDate".to_string()],
        "2262-04-11 must still push down as a date filter"
    );
}

/// `BETWEEN` normalizes its bounds through a separate path, so an out-of-range bound must be
/// handled there too rather than aborting the query.
#[tokio::test]
async fn test_filter_between_date_beyond_nanosecond_range() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
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

    // A two-sided range on a time dimension pushes down as a dateRange rather than two filters.
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
        "both BETWEEN bounds must push down with the out-of-range one intact"
    );
}

/// The same filter written without a `date` prefix is a bare string normalized against the
/// column's `Timestamp(Nanosecond)` type. That normalization used to give up on a date this far
/// out and leave the bound as a raw string, so the filter pushed down as `9999-12-31` while every
/// other date filter pushes down an ISO instant; it now normalizes like the rest.
#[tokio::test]
async fn test_filter_string_date_beyond_nanosecond_range_is_pushed_down() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
SELECT dim_str0
FROM MultiTypeCube
WHERE dim_date0 <= '9999-12-31'
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
        "the string form must still normalize and push down"
    );
}

/// An out-of-range bound must not cost the rest of the query its normalization.
///
/// `PlanNormalize` is run as `optimize(..).unwrap_or(plan)`, so an `Err` anywhere in the rule
/// silently reverts the *whole* plan. The bound is therefore declined at the coercion site instead.
/// Without that, the `DATE - DATE` → `DATEDIFF` rewrite below disappears — and on dialects where
/// `DATE - DATE` yields an INTERVAL, comparing it against `3` is wrong SQL, which is precisely why
/// the rewrite exists.
#[tokio::test]
async fn test_out_of_range_bound_keeps_other_normalizations() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    for (label, predicate) in [
        ("control", "(dim_date1::date - dim_date2::date) > 3"),
        (
            "with an out-of-range bound",
            "(dim_date1::date - dim_date2::date) > 3 AND dim_date0 <= date '9999-12-31'",
        ),
        (
            "with an out-of-range BETWEEN bound",
            "(dim_date1::date - dim_date2::date) > 3 \
             AND dim_date0 BETWEEN date '2020-01-01' AND date '9999-12-31'",
        ),
        (
            "with an out-of-range IN element",
            "(dim_date1::date - dim_date2::date) > 3 \
             AND dim_date0 IN (date '2020-01-01', date '9999-12-31')",
        ),
    ] {
        let query_plan = convert_select_to_query_plan(
            // language=PostgreSQL
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
            sql.contains("DATEDIFF(day,"),
            "the DATE - DATE rewrite must survive {}, got: {}",
            label,
            sql
        );
    }
}
