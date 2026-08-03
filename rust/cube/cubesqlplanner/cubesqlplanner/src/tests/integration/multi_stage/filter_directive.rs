use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_stage.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_stage_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_exclude_drops_dim_filter_from_leaf() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_exclude_status
        dimensions:
          - orders.category
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - completed
        order:
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_keep_only_drops_other_dim_filter() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_keep_only_status
        dimensions:
          - orders.status
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - completed
          - dimension: orders.category
            operator: equals
            values:
              - books
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_include_dim_filter() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_only_completed
        dimensions:
          - orders.category
        order:
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_include_or_group() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_or_completed_pending
        dimensions:
          - orders.category
        order:
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_include_time_dim_filter() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_after_feb
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

#[tokio::test(flavor = "multi_thread")]
async fn test_exclude_plus_include_replaces_filter() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_replace_status
        dimensions:
          - orders.category
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - completed
        order:
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_mode_fixed_top_level_equivalent_to_relative() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_fixed_completed
        dimensions:
          - orders.category
        order:
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_exclude_drops_segment_from_leaf() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_exclude_segment
        dimensions:
          - orders.category
        segments:
          - orders.completed_orders
        order:
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_keep_only_segment_drops_other_filters() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_keep_only_segment
        dimensions:
          - orders.category
        segments:
          - orders.completed_orders
        filters:
          - dimension: orders.category
            operator: equals
            values:
              - books
        order:
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_mode_fixed_in_chain_diverges_from_relative() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.t_chain_relative
          - orders.t_chain_fixed
        dimensions:
          - orders.category
        order:
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
