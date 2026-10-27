use cubeclient::models::V1LoadRequestQueryTimeDimension;
use pretty_assertions::assert_eq;
use serde_json::json;

use super::LogicalPlanTestUtils;
use crate::{
    compile::{
        test::{convert_select_to_query_plan, init_testing_logger},
        DatabaseProtocol, Rewriter,
    },
    transport::TransportLoadRequestQuery,
};

/// Metabase groups by day as `CAST(CAST(col AS date) AS timestamp)`. That has to plan
/// like `date_trunc('day', col)` so the query can be served from a `day` rollup.
#[tokio::test]
async fn test_metabase_cast_date_to_timestamp_group_by_day() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let now = "str_to_date('2022-01-01 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')";
    let logical_plan = convert_select_to_query_plan(
        format!(
            r#"
            SELECT
                CAST(CAST("public"."KibanaSampleDataEcommerce"."order_date" AS date) AS timestamp) AS "order_date",
                SUM("public"."KibanaSampleDataEcommerce"."sumPrice") AS "sum"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE "public"."KibanaSampleDataEcommerce"."order_date" >= CAST(({now} + INTERVAL '-30 day') AS date)
                AND "public"."KibanaSampleDataEcommerce"."order_date" < CAST({now} AS date)
            GROUP BY CAST(CAST("public"."KibanaSampleDataEcommerce"."order_date" AS date) AS timestamp)
            ORDER BY CAST(CAST("public"."KibanaSampleDataEcommerce"."order_date" AS date) AS timestamp) ASC
            "#
        ),
        DatabaseProtocol::PostgreSQL,
    )
    .await
    .as_logical_plan();

    assert_eq!(
        logical_plan.expect_root_cube_scan().request,
        TransportLoadRequestQuery {
            measures: Some(vec!["KibanaSampleDataEcommerce.sumPrice".to_string()]),
            dimensions: Some(vec![]),
            segments: Some(vec![]),
            time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                granularity: Some("day".to_string()),
                date_range: Some(json!(vec![
                    "2021-12-02T00:00:00.000Z".to_string(),
                    "2021-12-31T23:59:59.999Z".to_string(),
                ])),
            }]),
            order: Some(vec![vec![
                "KibanaSampleDataEcommerce.order_date".to_string(),
                "asc".to_string(),
            ]]),
            ..Default::default()
        }
    )
}

/// Plain grouped pushdown of the PowerBI distinct-count idiom. The
/// `MAX(CASE ...)` half is dropped rather than counted, so a group holding a
/// NULL comes back one lower than the query asks for -- see the note on
/// "aggregate-function-powerbi-count-distinct-max-case" for the numbers.
#[tokio::test]
async fn test_powerbi_count_distinct_with_max_case() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let logical_plan = convert_select_to_query_plan(
        r#"
        select
            "rows"."customer_gender" as "customer_gender",
            count(distinct("rows"."countDistinct")) + max(
                case
                    when "rows"."countDistinct" is null then 1
                    else 0
                end
            ) as "a0"
        from
            "public"."KibanaSampleDataEcommerce" "rows"
        group by
            "customer_gender"
        limit
            1000001
        ;"#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await
    .as_logical_plan();

    assert_eq!(
        logical_plan.find_cube_scan().request,
        TransportLoadRequestQuery {
            measures: Some(vec!["KibanaSampleDataEcommerce.countDistinct".to_string()]),
            dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
            segments: Some(vec![]),
            order: Some(vec![]),
            limit: Some(1000001),
            ..Default::default()
        }
    )
}

/// Same idiom reached through a subselect and with an outer `ORDER BY`, the shape
/// that used to give up on push to cube and rebuild the distinct count as SQL over
/// an ungrouped scan. It must stay a grouped request.
#[tokio::test]
async fn test_powerbi_count_distinct_with_max_case_order_by_limit() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        r#"
        select
            "rows"."customer_gender" as "customer_gender",
            count(distinct("rows"."countDistinct")) + max(
                case
                    when "rows"."countDistinct" is null then 1
                    else 0
                end
            ) as "a0"
        from
            (
                select
                    "_"."customer_gender",
                    "_"."countDistinct"
                from
                    "public"."KibanaSampleDataEcommerce" "_"
            ) "rows"
        group by
            "customer_gender"
        order by
            "a0" desc
        limit
            10
        ;"#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await;

    let logical_plan = query_plan.as_logical_plan();

    let sql = logical_plan
        .find_cube_scan_wrapped_sql_deep()
        .wrapped_sql
        .sql;
    // The key is absent entirely from a grouped request, so this holds whatever
    // spacing the serializer uses.
    assert!(
        !sql.contains(r#""ungrouped""#),
        "expected a grouped request, got: {}",
        sql
    );
    assert!(
        !sql.contains("COUNT(DISTINCT"),
        "distinct count must be left to Cube, not rebuilt as SQL, got: {}",
        sql
    );
    assert!(
        sql.contains("${KibanaSampleDataEcommerce.countDistinct}"),
        "expected the measure to be pushed to Cube, got: {}",
        sql
    );
    assert!(
        sql.contains(r#"\"sql\":\"0\""#),
        "expected the MAX(CASE ...) half to become a literal 0 member, got: {}",
        sql
    );
    assert!(
        sql.contains("ORDER BY") && sql.contains("LIMIT 10"),
        "expected order and limit to be pushed down, got: {}",
        sql
    );
}

/// A measure whose agg type `is_same_agg_type("countDistinct")` rejects -- `max`
/// here -- keeps its `MAX(CASE ...)`, which must not be folded away.
#[tokio::test]
async fn test_powerbi_max_case_over_non_count_distinct_is_not_dropped() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        r#"
        select
            "rows"."customer_gender" as "customer_gender",
            max(
                case
                    when "rows"."maxPrice" is null then 1
                    else 0
                end
            ) as "a0"
        from
            (
                select
                    "_"."customer_gender",
                    "_"."maxPrice"
                from
                    "public"."KibanaSampleDataEcommerce" "_"
            ) "rows"
        group by
            "customer_gender"
        order by
            "a0" desc
        limit
            10
        ;"#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await;

    let sql = query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql_deep()
        .wrapped_sql
        .sql;
    assert!(
        sql.contains("MAX(CASE WHEN"),
        "expected the CASE to survive for a non-distinct measure, got: {}",
        sql
    );
    // `MAX(CASE WHEN` alone would also appear on the old lossy fallback path, so
    // pin the part that actually distinguishes the two: no literal 0 member.
    assert!(
        !sql.contains(r#"\"sql\":\"0\""#),
        "MAX(CASE ...) must not be folded to a literal 0 here, got: {}",
        sql
    );
}

/// `is_same_agg_type("countDistinct")` also accepts `number`, and neither rule
/// requires the paired `COUNT(DISTINCT ...)` to be present, so a lone
/// `MAX(CASE WHEN <number measure> IS NULL ...)` is folded to 0 as well. That is
/// wider than the idiom this workaround targets; it is pinned here because the
/// grouped path at "aggregate-function-powerbi-count-distinct-max-case" does the
/// same, and the two must keep matching.
#[tokio::test]
async fn test_powerbi_max_case_over_number_measure_is_dropped() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        r#"
        select
            "rows"."dim_str0" as "d",
            max(
                case
                    when "rows"."measure_num0" is null then 1
                    else 0
                end
            ) as "a0"
        from
            (
                select
                    "_"."dim_str0",
                    "_"."measure_num0"
                from
                    "MultiTypeCube" "_"
            ) "rows"
        group by
            "d"
        order by
            "a0" desc
        limit
            10
        ;"#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await;

    let sql = query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql_deep()
        .wrapped_sql
        .sql;
    assert!(
        sql.contains(r#"\"sql\":\"0\""#),
        "expected the MAX(CASE ...) over a number measure to fold to 0, got: {}",
        sql
    );
    assert!(
        !sql.contains("MAX(CASE WHEN"),
        "expected no MAX(CASE ...) left in the pushed down SQL, got: {}",
        sql
    );
}
