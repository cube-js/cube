//! A multiplied view measure reaches the cubes its SQL references along the
//! view's join path, as the keys subquery does.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::{table_rows, TestContext};
use indoc::indoc;

const SEED: &str = "multiplied_measure_view_join_path_tables.sql";

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/multiplied_measure_view_join_path.yaml");
    TestContext::new(schema).unwrap()
}

#[cfg(feature = "integration-postgres")]
async fn oracle_rows(sql: &str) -> Vec<Vec<String>> {
    use crate::test_fixtures::test_utils::{integration_context, pg_service};
    let client = pg_service::connect_and_seed(SEED).await;
    let messages = client.simple_query(sql).await.unwrap();
    table_rows(&integration_context::format_simple_query_results(&messages))
}

#[cfg(not(feature = "integration-postgres"))]
async fn oracle_rows(_sql: &str) -> Vec<Vec<String>> {
    unreachable!()
}

async fn assert_matches_oracle(query: &str, oracle_sql: &str) {
    let ctx = create_context();
    ctx.build_sql(query).unwrap();
    let Some(result) = ctx.try_execute_pg(query, SEED).await else {
        return;
    };
    assert_eq!(
        table_rows(&result),
        oracle_rows(oracle_sql).await,
        "{result}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_measure_without_fan_out() {
    assert_matches_oracle(
        indoc! {"
            measures:
              - orders_view.total
        "},
        indoc! {"
            SELECT sum(o.amount + c.bonus)
            FROM mvjp_orders o
            JOIN mvjp_accounts a ON a.id = o.account_id
            JOIN mvjp_customers c ON c.id = a.owner_id
        "},
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_view_measure_follows_view_join_path() {
    assert_matches_oracle(
        indoc! {"
            measures:
              - orders_view.total
            dimensions:
              - orders_view.sku
            order:
              - id: orders_view.sku
        "},
        indoc! {"
            SELECT li.sku, sum(o.amount + c.bonus)
            FROM mvjp_orders o
            JOIN mvjp_accounts a ON a.id = o.account_id
            JOIN mvjp_customers c ON c.id = a.owner_id
            JOIN mvjp_line_items li ON li.order_id = o.id
            GROUP BY li.sku
            ORDER BY li.sku
        "},
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_cube_measure_keeps_default_join_path() {
    assert_matches_oracle(
        indoc! {"
            measures:
              - orders.total
            dimensions:
              - line_items.sku
            order:
              - id: line_items.sku
        "},
        indoc! {"
            SELECT li.sku, sum(o.amount + c.bonus)
            FROM mvjp_orders o
            JOIN mvjp_customers c ON c.id = o.customer_id
            JOIN mvjp_line_items li ON li.order_id = o.id
            GROUP BY li.sku
            ORDER BY li.sku
        "},
    )
    .await;
}
