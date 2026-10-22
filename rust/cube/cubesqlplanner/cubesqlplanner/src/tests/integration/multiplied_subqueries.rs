use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const SEED: &str = "integration_multiplied_subqueries_tables.sql";

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multiplied_subqueries.yaml");
    TestContext::new(schema).unwrap()
}

// `customers.total_ltv` is multiplied by `orders`, so the keys subquery groups
// by the calc-group dimensions. With no filter pinning `group_mode` both of its
// values are cross-joined into the keys rows. Expected:
//   by_category: books = 100 + 10 + 1 = 111, games = 100
//   by_status:   completed = 100 + 1 = 101, pending = 10
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

// `customers.weighted_orders` reaches `regions`, so under the multiplying
// `orders` join it is aggregated through a measure subquery, and it reads the
// sub_query dimension `customers.orders_count`, so that subquery's source
// joins the dimension subquery. Per customer: 1 → 2 * 10 = 20,
// 2 → 1 * 100 = 100, 3 → 1 * 10 = 10. Expected:
//   completed = 20 + 10 = 30, pending = 100
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

// A sub_query dimension selected next to a multiplied measure is joined into
// the keys subquery. Orders per customer: 1 → 2, 2 → 1, 3 → 1. Expected:
//   completed / 1 = 1, completed / 2 = 100, pending / 1 = 10
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

// A filter on a sub_query dimension restricts the keys subquery, which joins
// the dimension subquery only for the WHERE. Customers with one order are 2
// and 3. Expected: completed = 1, pending = 10
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

// The key cube's primary key selected as a dimension is projected once in the
// keys subquery, not again as the re-join key. Expected:
//   1 / completed = 100, 2 / pending = 10, 3 / completed = 1
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

// `customers.total_orders_count` reads only its own cube, so the multiplied
// aggregate joins the cube and the dimension subquery for `orders_count`
// directly. Expected: completed = 2 + 1 = 3, pending = 1
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

// A measure aggregated through a measure subquery emits one row per key for
// the select above to aggregate. Resolving a calc group it depends on there
// would emit one row per group value and multiply the result, so the query is
// rejected instead.
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
