#![cfg(feature = "integration-postgres")]

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::IntegrationTestContext;
use indoc::indoc;

#[tokio::test(flavor = "multi_thread")]
async fn test_simple_join_pg() {
    let schema = MockSchema::from_yaml_file("common/diamond_joins.yaml");
    let ctx = IntegrationTestContext::new(schema, "diamond_tables.sql").await;

    let query_yaml = indoc! {"
        measures:
          - cube_a.count
        dimensions:
          - cube_c.code
    "};

    let result = ctx.execute_query(query_yaml).await;
    insta::assert_snapshot!(result);
}
