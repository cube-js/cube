use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/switch_values_column_collision.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "switch_values_column_collision_tables.sql";

// The `total` stage does not depend on `kind`, so it cross-joins the switch
// values; the fact table also has a physical `kind` column.
#[tokio::test(flavor = "multi_thread")]
async fn test_switch_filter_with_same_named_source_column() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - facts.amount_by_kind
          - facts.total
        filters:
          - dimension: facts.kind
            operator: contains
            values:
              - a
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_switch_filter_with_same_named_source_column_in_view() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - facts_view.amount_by_kind
          - facts_view.total
        filters:
          - dimension: facts_view.kind
            operator: contains
            values:
              - a
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
