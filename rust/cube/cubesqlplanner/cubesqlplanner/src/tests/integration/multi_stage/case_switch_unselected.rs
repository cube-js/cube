//! A `case` measure over a `switch` dimension that the query neither selects
//! nor filters resolves to its `else` branch, one row per query grain.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_case_switch_unselected.yaml");
    TestContext::new(schema).unwrap()
}

#[cfg(feature = "integration-postgres")]
const SEED: &str = "integration_case_switch_unselected_tables.sql";

#[cfg(feature = "integration-postgres")]
async fn assert_matches_oracle(ctx: &TestContext, query: &str, oracle_sql: &str) {
    use crate::test_fixtures::test_utils::{integration_context, pg_service};

    let actual = ctx.try_execute_pg(query, SEED).await.unwrap();
    let client = pg_service::connect_and_seed(SEED).await;
    let messages = client.simple_query(oracle_sql).await.unwrap();
    let expected = integration_context::format_simple_query_results(&messages);
    assert_eq!(actual, expected, "query:\n{query}");
}

#[cfg(not(feature = "integration-postgres"))]
async fn assert_matches_oracle(ctx: &TestContext, query: &str, _oracle_sql: &str) {
    ctx.build_sql(query).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_case_switch_unselected_by_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.dynamic_amount
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "#};

    assert_matches_oracle(
        &ctx,
        query,
        "SELECT status AS orders__status, sum(amount_usd) AS orders__dynamic_amount \
         FROM case_switch_orders GROUP BY 1 ORDER BY 1",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_case_switch_unselected_total() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.dynamic_amount
    "#};

    assert_matches_oracle(
        &ctx,
        query,
        "SELECT sum(amount_usd) AS orders__dynamic_amount FROM case_switch_orders",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_case_switch_unselected_open_else() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.dynamic_amount_open_else
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "#};

    assert_matches_oracle(
        &ctx,
        query,
        "SELECT status AS orders__status, sum(amount_usd) AS orders__dynamic_amount_open_else \
         FROM case_switch_orders GROUP BY 1 ORDER BY 1",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_case_switch_unselected_calculated_on_top() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.dynamic_amount_doubled
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "#};

    assert_matches_oracle(
        &ctx,
        query,
        "SELECT status AS orders__status, sum(amount_usd) * 2 AS orders__dynamic_amount_doubled \
         FROM case_switch_orders GROUP BY 1 ORDER BY 1",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_case_switch_filtered_switch_value() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.dynamic_amount
        dimensions:
          - orders.status
        filters:
          - member: orders.currency_selector
            operator: equals
            values:
              - EUR
        order:
          - id: orders.status
    "#};

    assert_matches_oracle(
        &ctx,
        query,
        "SELECT status AS orders__status, sum(amount_eur) AS orders__dynamic_amount \
         FROM case_switch_orders GROUP BY 1 ORDER BY 1",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_case_switch_selected_switch_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.dynamic_amount_open_else
        dimensions:
          - orders.status
          - orders.currency_selector
        order:
          - id: orders.status
          - id: orders.currency_selector
    "#};

    assert_matches_oracle(
        &ctx,
        query,
        "SELECT status AS orders__status, c AS orders__currency_selector, \
           CASE c WHEN 'EUR' THEN sum(amount_eur) WHEN 'NOK' THEN sum(amount_nok) \
           ELSE sum(amount_usd) END AS orders__dynamic_amount_open_else \
         FROM case_switch_orders CROSS JOIN (VALUES ('USD'), ('EUR'), ('NOK')) v(c) \
         GROUP BY 1, 2 ORDER BY 1, 2",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_case_switch_unselected_through_view() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders_view.dynamic_amount
        dimensions:
          - orders_view.status
        order:
          - id: orders_view.status
    "#};

    assert_matches_oracle(
        &ctx,
        query,
        "SELECT status AS orders_view__status, sum(amount_usd) AS orders_view__dynamic_amount \
         FROM case_switch_orders GROUP BY 1 ORDER BY 1",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_case_switch_filtered_through_view() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders_view.dynamic_amount
        filters:
          - member: orders_view.currency_selector
            operator: equals
            values:
              - NOK
    "#};

    assert_matches_oracle(
        &ctx,
        query,
        "SELECT sum(amount_nok) AS orders_view__dynamic_amount FROM case_switch_orders",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_case_switch_other_cube_switch_unpinned() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.dynamic_amount
          - refunds.dynamic_refund
        filters:
          - member: orders.currency_selector
            operator: equals
            values:
              - EUR
    "#};

    assert_matches_oracle(
        &ctx,
        query,
        "SELECT sum(amount_eur) AS orders__dynamic_amount, \
           sum(amount_usd) AS refunds__dynamic_refund \
         FROM case_switch_orders",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_case_switch_unselected_disjoint_else() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.dynamic_amount_disjoint_else
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "#};

    assert_matches_oracle(
        &ctx,
        query,
        "SELECT status AS orders__status, \
           sum(amount_usd + amount_eur) AS orders__dynamic_amount_disjoint_else \
         FROM case_switch_orders GROUP BY 1 ORDER BY 1",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_case_switch_unselected_else_in_joined_cube() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.dynamic_amount_joined_else
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "#};

    assert_matches_oracle(
        &ctx,
        query,
        "SELECT status AS orders__status, sum(amount_eur) AS orders__dynamic_amount_joined_else \
         FROM case_switch_orders GROUP BY 1 ORDER BY 1",
    )
    .await;
}
