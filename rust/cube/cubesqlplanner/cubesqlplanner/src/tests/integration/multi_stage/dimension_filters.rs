use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_stage_dimension_filter.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_stage_dimension_filter_tables.sql";

async fn assert_query(name: &str, query: &str) {
    let ctx = create_context();
    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(name, result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subquery_dim_filter_on_unselected_dimension() {
    assert_query(
        "subquery_dim_filter_on_unselected_dimension",
        indoc! {r#"
        dimensions:
          - activity.tier_subquery_dim
        filters:
          - member: activity.player
            operator: equals
            values:
              - p2
    "#},
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_dim_filter_on_unselected_dimension() {
    assert_query(
        "multi_stage_dim_filter_on_unselected_dimension",
        indoc! {r#"
        dimensions:
          - activity.tier_ms_dim
        filters:
          - member: activity.player
            operator: equals
            values:
              - p2
    "#},
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subquery_dim_filter_on_selected_dimension() {
    assert_query(
        "subquery_dim_filter_on_selected_dimension",
        indoc! {r#"
        dimensions:
          - activity.player
          - activity.tier_subquery_dim
        filters:
          - member: activity.player
            operator: equals
            values:
              - p2
        order:
          - id: activity.player
    "#},
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subquery_dim_date_range_filter() {
    assert_query(
        "subquery_dim_date_range_filter",
        indoc! {r#"
        dimensions:
          - activity.tier_subquery_dim
        filters:
          - member: activity.played_at
            operator: inDateRange
            values:
              - "2024-01-01"
              - "2024-01-31"
        timezone: UTC
    "#},
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subquery_dim_time_dimension_date_range() {
    assert_query(
        "subquery_dim_time_dimension_date_range",
        indoc! {r#"
        dimensions:
          - activity.tier_subquery_dim
        time_dimensions:
          - dimension: activity.played_at
            dateRange:
              - "2024-01-01"
              - "2024-01-31"
        timezone: UTC
    "#},
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_dim_time_dimension_date_range() {
    assert_query(
        "multi_stage_dim_time_dimension_date_range",
        indoc! {r#"
        dimensions:
          - activity.tier_ms_dim
        time_dimensions:
          - dimension: activity.played_at
            dateRange:
              - "2024-01-01"
              - "2024-01-31"
        timezone: UTC
    "#},
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_dim_filter_on_itself() {
    assert_query(
        "multi_stage_dim_filter_on_itself",
        indoc! {r#"
        dimensions:
          - activity.player
          - activity.tier_ms_dim
        filters:
          - member: activity.tier_ms_dim
            operator: equals
            values:
              - high
    "#},
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_dim_filter_on_itself_and_unselected_dimension() {
    assert_query(
        "multi_stage_dim_filter_on_itself_and_unselected_dimension",
        indoc! {r#"
        dimensions:
          - activity.tier_ms_dim
        filters:
          - member: activity.tier_ms_dim
            operator: equals
            values:
              - low
          - member: activity.player
            operator: equals
            values:
              - p2
    "#},
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_dim_date_range_with_granularity() {
    assert_query(
        "multi_stage_dim_date_range_with_granularity",
        indoc! {r#"
        dimensions:
          - activity.tier_ms_dim
        time_dimensions:
          - dimension: activity.played_at
            granularity: month
            dateRange:
              - "2024-01-15"
              - "2024-03-31"
        timezone: UTC
        order:
          - id: activity.played_at
    "#},
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_dim_and_group_with_unselected_dimension() {
    assert_query(
        "multi_stage_dim_and_group_with_unselected_dimension",
        indoc! {r#"
        dimensions:
          - activity.tier_ms_dim
        filters:
          - and:
              - member: activity.tier_ms_dim
                operator: equals
                values:
                  - low
              - member: activity.player
                operator: equals
                values:
                  - p2
    "#},
    )
    .await;
}

#[test]
fn test_multi_stage_dim_or_group_with_unselected_dimension_is_rejected() {
    let ctx = create_context();
    let err = ctx
        .build_sql(indoc! {r#"
        dimensions:
          - activity.tier_ms_dim
        filters:
          - or:
              - member: activity.tier_ms_dim
                operator: equals
                values:
                  - low
              - member: activity.player
                operator: equals
                values:
                  - p2
    "#})
        .unwrap_err();
    assert!(
        err.message
            .contains("can't mix multi-stage dimensions with other members"),
        "{}",
        err.message
    );
}

#[test]
fn test_filter_on_unselected_multi_stage_dim_is_rejected() {
    let ctx = create_context();
    let err = ctx
        .build_sql(indoc! {r#"
        dimensions:
          - activity.tier_ms_dim
        filters:
          - member: activity.tier_subquery_dim
            operator: equals
            values:
              - low
    "#})
        .unwrap_err();
    assert!(
        err.message.contains(
            "Filtering on multi-stage dimension activity.tier_subquery_dim requires it to be selected"
        ),
        "{}",
        err.message
    );
}
