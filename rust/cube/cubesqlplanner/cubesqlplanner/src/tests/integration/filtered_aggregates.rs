use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const SEED: &str = "integration_filtered_aggregates_tables.sql";

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_filtered_aggregates.yaml");
    TestContext::new(schema).unwrap()
}

// The model of the "Calculating filtered aggregates" recipe: subquery
// dimensions chained over two hops (sales -> store -> retailer), with a
// dimension passed back down the same path to filter the innermost measure.
#[tokio::test(flavor = "multi_thread")]
async fn test_subquery_dimensions_chained_over_two_hops() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - retailer.sales_goal
          - retailer.sales_goal_achieved
        dimensions:
          - retailer.name
          - retailer.sales
          - retailer.sales_for_goal
        order:
          - id: retailer.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
