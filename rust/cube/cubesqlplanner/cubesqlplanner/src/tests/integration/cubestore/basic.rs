//! Queries over external pre-aggregations executed against a live
//! CubeStore instance (requires `--features integration-cubestore`
//! and a locally built `cubestored` binary).

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

#[tokio::test(flavor = "multi_thread")]
async fn test_basic_pre_agg_cubestore() {
    let schema = MockSchema::from_yaml_file("common/integration_cubestore_basic.yaml");
    let ctx = TestContext::new_with_external_cubestore(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - visitors.count
        dimensions:
          - visitors.source
        order:
          - id: visitors.source
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1, "Should use one pre-aggregation");
    assert_eq!(pre_aggrs[0].name(), "daily_rollup");

    if let Some(result) = ctx
        .try_execute_cubestore(query_yaml, "pre_aggregation_tables.sql")
        .await
    {
        insta::assert_snapshot!("basic_pre_agg_cubestore_result", result);
    }
}
