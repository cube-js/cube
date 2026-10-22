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
