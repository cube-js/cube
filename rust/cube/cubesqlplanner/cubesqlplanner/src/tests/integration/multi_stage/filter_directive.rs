use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_stage.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_stage_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_exclude_drops_dim_filter_from_leaf() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_exclude_status
        dimensions:
          - orders.category
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - completed
        order:
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_keep_only_drops_other_dim_filter() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_keep_only_status
        dimensions:
          - orders.status
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - completed
          - dimension: orders.category
            operator: equals
            values:
              - books
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_include_dim_filter() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_only_completed
        dimensions:
          - orders.category
        order:
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_include_or_group() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_or_completed_pending
        dimensions:
          - orders.category
        order:
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_include_time_dim_filter() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_after_feb
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_exclude_plus_include_replaces_filter() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_replace_status
        dimensions:
          - orders.category
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - completed
        order:
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_mode_fixed_top_level_equivalent_to_relative() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_fixed_completed
        dimensions:
          - orders.category
        order:
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_exclude_drops_segment_from_leaf() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_exclude_segment
        dimensions:
          - orders.category
        segments:
          - orders.completed_orders
        order:
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_keep_only_segment_drops_other_filters() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_keep_only_segment
        dimensions:
          - orders.category
        segments:
          - orders.completed_orders
        filters:
          - dimension: orders.category
            operator: equals
            values:
              - books
        order:
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_mode_fixed_in_chain_diverges_from_relative() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.t_chain_relative
          - orders.t_chain_fixed
        dimensions:
          - orders.category
        order:
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// `grain.exclude` and `filter.exclude` naming the same dimension, which the
// query both groups by and filters on. The value ignores the filter and the
// partition; the row set stays the query's — the statuses the query filtered
// out may not reappear.
#[tokio::test(flavor = "multi_thread")]
async fn test_grain_and_filter_exclude_keeps_query_row_set() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_grain_and_filter_exclude_status
        dimensions:
          - orders.status
          - orders.category
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - completed
        order:
          - id: orders.status
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Share-of-total over a grand-total denominator that ignores the query filter.
// The denominator's own aggregation must span every status while the ratio is
// reported only for the rows the query asked for; a widened row set would show
// up as extra rows whose share coalesces to 0.
#[tokio::test(flavor = "multi_thread")]
async fn test_share_of_grand_total_keeps_query_row_set() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_share_percent
        dimensions:
          - orders.status
          - orders.category
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - completed
        order:
          - id: orders.status
          - id: orders.category
    "#};

    let sql = ctx.build_sql(query).unwrap();
    // The window text comes from a `format!` in the physical plan, not from a
    // dialect template, so `OVER (` is stable; whitespace is stripped only so
    // the check does not depend on how the expression is laid out.
    let dense: String = sql.chars().filter(|c| !c.is_whitespace()).collect();
    assert!(
        !dense.contains("OVER("),
        "the denominator has to hold the query's rows through a keys side, \
         which a window expression cannot do:\n{}",
        sql
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// A NULL dimension value is a grid key like any other: the keys side and the
// measure side must still meet on it.
#[tokio::test(flavor = "multi_thread")]
async fn test_grain_exclude_keeps_null_dimension_key() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_grain_and_filter_exclude_status
        dimensions:
          - orders.status
          - orders.category_nullable
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - completed
        order:
          - id: orders.status
          - id: orders.category_nullable
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// The dropped filter's dimension is not one the query groups by, so the grain
// reshape has nothing to remove and the measure side keeps the full grid — yet
// the rows within that grid still widen. Grouping by the primary key makes it
// visible: only the six completed orders may appear.
#[tokio::test(flavor = "multi_thread")]
async fn test_filter_exclude_keeps_row_set_when_member_not_grouped() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_exclude_status
          - orders.amount_grain_and_filter_exclude_status
        dimensions:
          - orders.id
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - completed
        order:
          - id: orders.id
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// `keep_only` on a segment, with the dropped filter's dimension outside the
// query grid so the value differs from the plain measure: the segment still
// restricts to completed orders while the category filter is gone, so the
// measure spans every category and the plain measure only books.
#[tokio::test(flavor = "multi_thread")]
async fn test_keep_only_segment_value_ignores_dropped_filter() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_keep_only_segment
        dimensions:
          - orders.status
        segments:
          - orders.completed_orders
        filters:
          - dimension: orders.category
            operator: equals
            values:
              - books
        order:
          - id: orders.status
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// A rolling window rewrites the date range it hands to its base state, so the
// filter the inner measure drops no longer carries the query's own bounds. The
// row set survives that regardless: a rolling window takes its rows from the
// time series built out of the query's `dateRange`, and its values through the
// frame condition derived from the same range — neither depends on the leaf
// keeping its date filter.
#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_window_over_dropped_date_filter() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.rolling_amount_no_date_bound
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-03-01"
              - "2024-03-31"
        order:
          - id: orders.created_at
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// The same shape with a dimension and a range narrow enough that the trailing
// window reaches months the leaf can see but the query did not ask for.
#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_window_over_dropped_date_filter_by_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.rolling_amount_no_date_bound
        dimensions:
          - orders.category
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-01-31"
        order:
          - id: orders.created_at
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// The directive's `include` predicate shares operator and values with the query
// filter it drops but names a different member. The keys side is planned on the
// inherited state, so a member-blind CTE dedup key would hand back the widened
// measure CTE and leave the row set widened.
#[tokio::test(flavor = "multi_thread")]
async fn test_filter_exclude_with_lookalike_include_keeps_query_row_set() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_exclude_status_include_lookalike
        dimensions:
          - orders.status
          - orders.category
        filters:
          - dimension: orders.status
            operator: notEquals
            values:
              - pending
        order:
          - id: orders.status
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Rank is out of the keys side on purpose: ranking within the query grid would
// leave one row per partition and collapse every rank to 1. The cost is that the
// statuses the query filtered out stay in the result, carrying a NULL for every
// measure that does honour the filter. Pins that trade rather than endorsing it.
#[tokio::test(flavor = "multi_thread")]
async fn test_rank_with_filter_exclude_ranks_over_whole_universe() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.category_rank_all_statuses
        dimensions:
          - orders.status
          - orders.category
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - completed
        order:
          - id: orders.status
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
