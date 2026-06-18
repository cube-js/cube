use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const SEED: &str = "integration_transitive_tables.sql";

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_transitive.yaml");
    TestContext::new(schema).unwrap()
}

// 3-hop join: cube_a.total_value + cube_d.label
// X: ids 1,2 → 10+20=30, Y: id 3 → 30
#[tokio::test(flavor = "multi_thread")]
async fn test_transitive_3_hops() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - cube_a.total_value
        dimensions:
          - cube_d.label
        order:
          - id: cube_d.label
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// 3-hop with filter: cube_a.count + filter cube_d.label='X' → 2
#[tokio::test(flavor = "multi_thread")]
async fn test_transitive_with_filter() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - cube_a.count
        filters:
          - dimension: cube_d.label
            operator: equals
            values:
              - X
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
