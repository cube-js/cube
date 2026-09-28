//! Multi-stage `include` grain refs carrying switch / case-switch
//! dimensions. The planner selects these inside the multi-stage CTEs,
//! and case pruning / calc-group value resolution must follow each
//! leaf's own filters — not the outer query's.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_stage.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_stage_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_include_case_switch_dim() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_by_status_case
        dimensions:
          - orders.status_case_label
        filters:
          - member: orders.status
            operator: equals
            values:
              - completed
        order:
          - id: orders.status_case_label
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert!(
        !sql.contains("CASE"),
        "a filter restricting the switch to one value prunes the case everywhere, including the include copy inside the CTE"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_include_case_switch_dim_with_filter_excluded_from_leaf() {
    let ctx = create_context();

    // The `exclude` filter directive drops the status filter inside the
    // measure's CTEs, so the include copy of the case dimension must stay
    // unpruned there: every row keeps its real label.
    let query = indoc! {r#"
        measures:
          - orders.amount_by_status_case_unfiltered
        filters:
          - member: orders.status
            operator: equals
            values:
              - completed
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert!(
        sql.contains("CASE"),
        "the leaf runs without the status filter, so its case dimension must keep all branches"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_include_case_over_calc_group() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_by_group_mode_label
        dimensions:
          - orders.group_mode_label
        filters:
          - member: orders.group_mode
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

#[tokio::test(flavor = "multi_thread")]
async fn test_include_calc_group_dim() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_by_group_mode
        filters:
          - member: orders.group_mode
            operator: equals
            values:
              - by_status
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
