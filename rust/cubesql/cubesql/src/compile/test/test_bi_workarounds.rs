use cubeclient::models::V1CubeMetaType;
use datafusion::logical_plan::{plan::Extension, LogicalPlan};
use pretty_assertions::assert_eq;

use super::LogicalPlanTestUtils;
use crate::{
    compile::{
        engine::df::wrapper::CubeScanWrappedSqlNode,
        test::{
            convert_select_to_query_plan, convert_select_to_query_plan_with_meta,
            init_testing_logger,
        },
        DatabaseProtocol, QueryPlan, Rewriter,
    },
    transport::{CubeMeta, CubeMetaDimension, CubeMetaMeasure, TransportLoadRequestQuery},
};

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

// Repro attempt for https://github.com/cube-js/cube/issues/11542
//
// PowerBI (and similar BI tools) wraps an already non-additive `countDistinct`
// measure in an outer `COUNT(DISTINCT measure) + MAX(CASE WHEN measure IS NULL THEN 1 ELSE 0 END)`
// idiom, additionally wrapping the cube in a passthrough subquery, the way a
// generated report typically does.
//
// Finding: with the standard test schema (a single plain cube, no Cube View
// spanning multiple joined cubes), this does NOT reproduce the reported
// "collapses to 1" bug. The e-graph rewriter's `split/aggregate_function.rs`
// rule "aggregate-function-powerbi-count-distinct-max-case" recognizes the
// idiom and collapses it to a plain reference to the underlying
// `KibanaSampleDataEcommerce.countDistinct` measure (computed correctly by
// Cube's load API) plus a hardcoded literal 0 standing in for the
// MAX(CASE...) null-counting part (see the "TODO: workaround for PowerBI"
// comment on that rule -- NULLs are deliberately NOT added as +1, which is a
// documented limitation, but is not the "collapses to 1" bug). No
// `CubeScanWrapperNode`/`CubeScanWrappedSqlNode` (raw generated-SQL text) is
// ever created for this query shape -- `wrapper_nodes: 0` /
// `ast_size_inside_wrapper: 0` in the rewriter's cost log -- so the textual
// `COUNT(DISTINCT ...)` reconstruction in
// `rewrite/rules/wrapper/aggregate_function.rs::transform_agg_fun_expr`
// (which *does* literally emit "COUNT_DISTINCT" as SQL text) never runs here
// either. That code path is only exercised when the query can't be expressed
// as plain measure/dimension pushdown (e.g. querying a Cube View that joins
// multiple cubes, or other cases that force raw SQL generation) -- which
// this test, using a single plain cube, does not exercise. That remains the
// most plausible way the originally reported bug could still occur and is
// worth a follow-up repro against a multi-cube View.
#[tokio::test]
async fn test_powerbi_count_distinct_with_max_case_wrapped_subquery() {
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
            ) as "value"
        from
            (
                select
                    "customer_gender",
                    "countDistinct"
                from
                    "public"."KibanaSampleDataEcommerce"
            ) as "rows"
        group by
            "rows"."customer_gender"
        ;"#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await;

    let logical_plan = query_plan.as_logical_plan();

    println!(
        "cube_scan request: {:#?}",
        logical_plan.find_cube_scan().request
    );

    // Only one measure is requested from Cube's load API: the real
    // countDistinct measure. No wrapper/raw-SQL node is created for this
    // query shape (confirmed by find_cube_scan() -- which would panic if
    // there were zero or more than one CubeScanNode reachable, wrapped or
    // not -- succeeding here), so there is no textual COUNT(DISTINCT ...)
    // to re-apply over an already-aggregated value.
    assert_eq!(
        logical_plan.find_cube_scan().request,
        TransportLoadRequestQuery {
            measures: Some(vec!["KibanaSampleDataEcommerce.countDistinct".to_string()]),
            dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
            segments: Some(vec![]),
            order: Some(vec![]),
            ..Default::default()
        }
    );

    // The root node is not a CubeScanWrapperNode/CubeScanWrappedSqlNode --
    // i.e. this query is resolved via plain measure pushdown to Cube's load
    // API, not via raw wrapped-SQL text generation. This is a regression
    // signal: if a future change starts routing this query through the
    // wrapper instead, this assertion will start failing and should be
    // revisited together with the wrapper's COUNT(DISTINCT) text
    // reconstruction in rewrite/rules/wrapper/aggregate_function.rs, since
    // that is the mechanism that could reproduce the originally reported
    // "collapses to 1" bug.
    let is_wrapped_sql_node = matches!(
        logical_plan,
        LogicalPlan::Extension(Extension { ref node })
            if node.as_any().downcast_ref::<CubeScanWrappedSqlNode>().is_some()
    );
    assert!(
        !is_wrapped_sql_node,
        "expected this query to be resolved via plain measure pushdown \
         (no CubeScanWrapper), but it produced a wrapped SQL node -- re-check \
         whether the outer COUNT(DISTINCT) got reconstructed as literal SQL \
         text over the already-aggregated countDistinct value"
    );
}

/// Meta for a single Cube `View` (`items_semantic_view`) exposing one dimension
/// and one `countDistinct` measure, mirroring the bug reporter's schema
/// (`item_count` = `COUNT(DISTINCT item_id)` with a filter, exposed through a
/// View). Unlike `views_meta()` in test_cube_join_views.rs, this View does not
/// combine multiple underlying cubes.
fn items_semantic_view_meta() -> Vec<CubeMeta> {
    vec![CubeMeta {
        name: "items_semantic_view".to_string(),
        description: None,
        title: None,
        r#type: V1CubeMetaType::View,
        dimensions: vec![CubeMetaDimension {
            name: "items_semantic_view.category_code".to_string(),
            r#type: "string".to_string(),
            alias_member: Some("items.category_code".to_string()),
            ..CubeMetaDimension::default()
        }],
        measures: vec![CubeMetaMeasure {
            name: "items_semantic_view.item_count".to_string(),
            title: None,
            short_title: None,
            description: None,
            r#type: "number".to_string(),
            agg_type: Some("countDistinct".to_string()),
            meta: None,
            alias_member: Some("items.item_count".to_string()),
            format: None,
            format_description: None,
            currency: None,
        }],
        segments: vec![],
        joins: None,
        folders: None,
        nested_folders: None,
        hierarchies: None,
        meta: None,
    }]
}

async fn plan_powerbi_count_distinct_variant(sql: &str) -> QueryPlan {
    convert_select_to_query_plan_with_meta(sql.to_string(), items_semantic_view_meta()).await
}

fn is_wrapped_sql_node(logical_plan: &LogicalPlan) -> bool {
    matches!(
        logical_plan,
        LogicalPlan::Extension(Extension { ref node })
            if node.as_any().downcast_ref::<CubeScanWrappedSqlNode>().is_some()
    )
}

// CONFIRMED repro for https://github.com/cube-js/cube/issues/11542.
//
// Being a Cube `View` (vs. a plain `Cube`) is *not* what triggers the bug --
// see `test_powerbi_count_distinct_no_order_limit_is_not_wrapped` below, which
// uses this exact View schema with the issue's literal query shape (a
// passthrough subquery + PowerBI's `COUNT(DISTINCT m) + MAX(CASE WHEN m IS
// NULL THEN 1 ELSE 0 END)` idiom) and resolves correctly via plain measure
// pushdown, same as the plain-Cube case in
// `test_powerbi_count_distinct_with_max_case_wrapped_subquery` above.
//
// The actual trigger is an outer `ORDER BY` + `LIMIT` on the query (which
// PowerBI's paging/sorting typically adds, and which the reporter's "simplified"
// repro SQL omitted). When both are present *together with* the PowerBI
// COUNT(DISTINCT)+MAX(CASE...) idiom, the e-graph rewriter can no longer
// express the query as a single grouped measure/dimension pushdown (the
// combined `count_distinct + 0` expression can't be pushed down as a Load API
// `order`), so it falls back to `CubeScanWrapperNode`: an *ungrouped*
// (`"ungrouped": true`) CubeScan for the raw `item_count` measure, wrapped in
// literal generated SQL that re-applies `COUNT(DISTINCT "rows"."item_count")`
// and `MAX(CASE WHEN ... IS NULL THEN 1 ELSE 0 END)` itself
// (rewrite/rules/wrapper/aggregate_function.rs::transform_agg_fun_expr, which
// emits `COUNT_DISTINCT` as literal SQL text without the powerbi-idiom
// simplification that the non-wrapper split rule in
// rewrite/rules/split/aggregate_function.rs applies).
//
// Critically, requesting a `countDistinct` measure *ungrouped* from Cube's
// Load API does not return the raw distinct values -- per
// `packages/cubejs-schema-compiler/src/adapter/BaseQuery.js`,
// `renderSqlMeasure()` (around line 3840), when `this.ungrouped` is true and
// `symbol.type` is `count`/`countDistinct`/`countDistinctApprox`, the measure
// is rendered as a `CASE WHEN (<expr>) IS NOT NULL THEN 1 END` presence
// indicator (1/NULL per row) rather than the underlying expression. That
// indicator is correct input for a later `SUM()` (reconstructing a `count`),
// but is exactly what collapses a later `COUNT(DISTINCT ...)` down to 1, as
// reported: all non-null rows become the literal value `1`, so
// `COUNT(DISTINCT 1, 1, ..., 1) = 1` regardless of how many original distinct
// values there were.
//
// This reproduces without needing a View spanning multiple cubes, and without
// needing anything else PowerBI-specific beyond ORDER BY + LIMIT, which
// PowerBI (and most BI tools paginating results) routinely add.
#[tokio::test]
async fn test_powerbi_count_distinct_with_order_by_limit_is_wrapped_and_wrong() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = plan_powerbi_count_distinct_variant(
        r#"
        select
            rows.category_code as category_code,
            count(distinct(rows.item_count)) + max(
                case
                    when rows.item_count is null then 1
                    else 0
                end
            ) as value
        from
            (
                select
                    category_code,
                    item_count
                from
                    items_semantic_view
            ) as rows
        group by
            rows.category_code
        order by
            value desc
        limit
            1000001
        ;"#,
    )
    .await;

    let logical_plan = query_plan.as_logical_plan();
    assert!(
        is_wrapped_sql_node(&logical_plan),
        "expected ORDER BY + LIMIT to force this query through \
         CubeScanWrapperNode -- if this now fails, the rewriter has changed \
         how it plans this query shape and this repro needs to be revisited"
    );

    let wrapped_sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
    println!("wrapped sql:\n{}", wrapped_sql);

    // The inner CubeScan requests the countDistinct measure *ungrouped*, which
    // is the mechanism that causes the schema-compiler to materialize it as a
    // 1/NULL presence indicator (see BaseQuery.js reference above) instead of
    // the real distinct values.
    assert!(
        wrapped_sql.contains("\"ungrouped\": true"),
        "expected the wrapped SQL's inner CubeScan request to be ungrouped -- \
         this is the step that causes Cube to materialize the countDistinct \
         measure as a 1/NULL indicator instead of the real distinct values. \
         Wrapped SQL:\n{}",
        wrapped_sql
    );

    // The outer wrapper SQL re-applies a literal COUNT(DISTINCT ...) over
    // that already-collapsed indicator column -- this is the exact
    // "COUNT(DISTINCT 1, 1, ..., 1) = 1" bug from the issue.
    assert!(
        wrapped_sql.contains("COUNT(DISTINCT \"rows\".\"item_count\")"),
        "expected the wrapper to literally reconstruct COUNT(DISTINCT ...) as \
         SQL text over the (already-collapsed) item_count column. Wrapped \
         SQL:\n{}",
        wrapped_sql
    );
}

// Control: the same View, same PowerBI idiom, but *without* ORDER BY/LIMIT --
// this is the issue's own "simplified" repro SQL verbatim. It does NOT
// reproduce the bug: being a `View` is not sufficient on its own, matching
// the plain-`Cube` finding in
// `test_powerbi_count_distinct_with_max_case_wrapped_subquery` above.
#[tokio::test]
async fn test_powerbi_count_distinct_no_order_limit_is_not_wrapped() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = plan_powerbi_count_distinct_variant(
        r#"
        select
            rows.category_code as category_code,
            count(distinct(rows.item_count)) + max(
                case
                    when rows.item_count is null then 1
                    else 0
                end
            ) as value
        from
            (
                select
                    category_code,
                    item_count
                from
                    items_semantic_view
            ) as rows
        group by
            rows.category_code
        ;"#,
    )
    .await;

    let logical_plan = query_plan.as_logical_plan();
    assert!(
        !is_wrapped_sql_node(&logical_plan),
        "expected this query (no ORDER BY/LIMIT) to resolve via plain \
         grouped measure pushdown even though the source is a View, but it \
         produced a wrapped SQL node instead"
    );
    assert_eq!(
        logical_plan.find_cube_scan().request,
        TransportLoadRequestQuery {
            measures: Some(vec!["items_semantic_view.item_count".to_string()]),
            dimensions: Some(vec!["items_semantic_view.category_code".to_string()]),
            segments: Some(vec![]),
            order: Some(vec![]),
            ..Default::default()
        }
    );
}

// Control: ORDER BY + LIMIT *with* a plain `COUNT(DISTINCT ...)` (no
// `MAX(CASE...)` idiom) still resolves via plain grouped measure pushdown --
// i.e. ORDER BY + LIMIT alone are not enough; it's specifically the compound
// `count_distinct + max_case` expression that the rewriter cannot push down
// together with an ORDER BY/LIMIT, forcing the fallback to the wrapper.
#[tokio::test]
async fn test_plain_count_distinct_with_order_by_limit_is_not_wrapped() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = plan_powerbi_count_distinct_variant(
        r#"
        select
            rows.category_code as category_code,
            count(distinct(rows.item_count)) as value
        from
            (
                select
                    category_code,
                    item_count
                from
                    items_semantic_view
            ) as rows
        group by
            rows.category_code
        order by
            value desc
        limit
            1000001
        ;"#,
    )
    .await;

    let logical_plan = query_plan.as_logical_plan();
    assert!(
        !is_wrapped_sql_node(&logical_plan),
        "expected plain COUNT(DISTINCT ...) with ORDER BY + LIMIT to still \
         resolve via plain grouped measure pushdown, not the wrapper"
    );
}
