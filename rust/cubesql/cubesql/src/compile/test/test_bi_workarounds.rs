use datafusion::logical_plan::{plan::Extension, LogicalPlan};
use pretty_assertions::assert_eq;

use super::LogicalPlanTestUtils;
use crate::{
    compile::{
        engine::df::wrapper::CubeScanWrappedSqlNode,
        test::{convert_select_to_query_plan, init_testing_logger},
        DatabaseProtocol, Rewriter,
    },
    transport::TransportLoadRequestQuery,
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
