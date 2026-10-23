use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const SEED: &str = "integration_multiplied_subqueries_tables.sql";

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multiplied_subqueries.yaml");
    TestContext::new(schema).unwrap()
}

// Unpinned calc group, cross-joined into the keys rows. Expected:
//   by_category: books = 100 + 10 + 1 = 111, games = 100
//   by_status: completed = 100 + 1 = 101, pending = 10
#[tokio::test(flavor = "multi_thread")]
async fn test_calc_group_cross_joined_in_keys_subquery() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - customers.total_ltv
        dimensions:
          - orders.group_mode
          - orders.group_mode_label
        order:
          - id: orders.group_mode
          - id: orders.group_mode_label
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// A filter pinning `group_mode` to one value turns the calc group into a
// literal inside the keys subquery. Expected: completed = 101, pending = 10.
#[tokio::test(flavor = "multi_thread")]
async fn test_calc_group_pinned_by_filter_in_keys_subquery() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - customers.total_ltv
        dimensions:
          - orders.group_mode_label
        filters:
          - dimension: orders.group_mode
            operator: equals
            values:
              - by_status
        order:
          - id: orders.group_mode_label
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Measure subquery joining the `orders_count` dimension subquery. Per customer:
// 1 → 2 * 10 = 20, 2 → 1 * 100 = 100, 3 → 1 * 10 = 10.
// Expected: completed = 30, pending = 100
#[tokio::test(flavor = "multi_thread")]
async fn test_sub_query_dim_in_measure_subquery() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - customers.weighted_orders
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

// Sub_query dimension selected in the keys subquery. Orders per customer:
// 1 → 2, 2 → 1, 3 → 1. Expected: completed / 1 = 1, completed / 2 = 100,
// pending / 1 = 10
#[tokio::test(flavor = "multi_thread")]
async fn test_sub_query_dim_selected_in_keys_subquery() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - customers.total_ltv
        dimensions:
          - orders.status
          - customers.orders_count
        order:
          - id: orders.status
          - id: customers.orders_count
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Sub_query dimension filtered in the keys subquery: customers 2 and 3 have
// one order. Expected: completed = 1, pending = 10
#[tokio::test(flavor = "multi_thread")]
async fn test_sub_query_dim_filtered_in_keys_subquery() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - customers.total_ltv
        dimensions:
          - orders.status
        filters:
          - dimension: customers.orders_count
            operator: equals
            values:
              - "1"
        order:
          - id: orders.status
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Key-cube primary key selected as a dimension, projected once in the keys.
// Expected: 1 / completed = 100, 2 / pending = 10, 3 / completed = 1
#[tokio::test(flavor = "multi_thread")]
async fn test_key_cube_primary_key_selected_with_multiplied_measure() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - customers.total_ltv
        dimensions:
          - customers.id
          - orders.status
        order:
          - id: customers.id
          - id: orders.status
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Multiplied aggregate over the cube joining the `orders_count` dimension
// subquery itself. Expected: completed = 2 + 1 = 3, pending = 1
#[tokio::test(flavor = "multi_thread")]
async fn test_sub_query_dim_in_multiplied_aggregate_over_cube() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - customers.total_orders_count
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

// An unpinned calc group in a measure subquery would emit a row per group value
// for each key and multiply the result, so the query is rejected.
#[test]
fn test_calc_group_in_measure_subquery_is_rejected() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - customers.mode_weight
        dimensions:
          - orders.status
          - customers.mode
    "#};

    let err = ctx.build_sql(query).unwrap_err();
    assert!(
        err.message
            .contains("depends on calculation group customers.mode"),
        "unexpected error: {}",
        err.message
    );
}

// Calc group pinned to `a`, a literal in the measure subquery: each customer
// weighs its region, 1 → 10, 2 → 100, 3 → 10.
// Expected: completed = 20, pending = 100
#[tokio::test(flavor = "multi_thread")]
async fn test_calc_group_pinned_in_measure_subquery() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - customers.mode_weight
        dimensions:
          - orders.status
        filters:
          - dimension: customers.mode
            operator: equals
            values:
              - a
        order:
          - id: orders.status
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

const ROW_GRAIN_LEAF_QUERY: &str = indoc! {r#"
    measures:
      - customers.total_ltv_stage
    dimensions:
      - customers.orders_count
    order:
      - id: customers.orders_count
      - id: customers.total_ltv_stage
    ungrouped: true
"#};

// A row-grain multi-stage leaf still aggregates the sub_query dimension it
// reads per key.
#[test]
fn test_sub_query_dim_aggregates_in_row_grain_multi_stage_leaf() {
    let ctx = create_context();

    let sql = ctx.build_sql(ROW_GRAIN_LEAF_QUERY).unwrap();

    assert!(
        sql.contains("count(\"msq_orders_per_customer\".id)"),
        "the dimension subquery must aggregate per key, got:\n{sql}"
    );
}

// Expected: (1, 1), (1, 10), (2, 100), (NULL, 1000).
// FIXME: the aggregate stage above the row-grain leaf renders its `sum`
// without a GROUP BY in ungrouped queries.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn test_sub_query_dim_in_row_grain_multi_stage_leaf() {
    let ctx = create_context();

    ctx.build_sql(ROW_GRAIN_LEAF_QUERY).unwrap();

    if let Some(result) = ctx.try_execute_pg(ROW_GRAIN_LEAF_QUERY, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Tier per status: completed totals 101 → big, pending 10 → small.
// FIXME: the dimension CTE's ON condition renders `orders.status` against the
// default alias, not the keys subquery's `customers_key_` one.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_dim_with_multiplied_measure() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - customers.total_ltv
        dimensions:
          - orders.status
          - customers.ltv_tier
        order:
          - id: orders.status
          - id: customers.ltv_tier
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
