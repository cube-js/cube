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
fn test_filter_on_unselected_multi_stage_dim_is_rejected() {
    let ctx = create_context();
    let err = ctx
        .build_sql(indoc! {r#"
        dimensions:
          - activity.tier_ms_dim
        filters:
          - member: activity.tier_label_ms_dim
            operator: equals
            values:
              - low
    "#})
        .unwrap_err();
    assert!(
        err.message.contains(
            "Filtering on multi-stage dimension activity.tier_label_ms_dim requires it to be selected"
        ),
        "{}",
        err.message
    );
}

// A subquery dimension is computed by its own subquery, so a query can filter
// on it without selecting it. The multi-stage dimension aggregates every row the
// filter keeps, hence `high` although each row alone is `low`.
#[tokio::test(flavor = "multi_thread")]
async fn test_filter_on_unselected_subquery_dim_under_multi_stage_dim() {
    assert_query(
        "filter_on_unselected_subquery_dim_under_multi_stage_dim",
        indoc! {r#"
        dimensions:
          - activity.tier_ms_dim
        filters:
          - member: activity.tier_subquery_dim
            operator: equals
            values:
              - low
    "#},
    )
    .await;
}

// The subquery dimension inside the multi-stage time dimension is computed by
// its stage, not by the outer query.
#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_time_dim_over_subquery_dim_with_granularity() {
    assert_query(
        "multi_stage_time_dim_over_subquery_dim_with_granularity",
        indoc! {r#"
        measures:
          - activity.theo_win
        dimensions:
          - activity.played_at
          - activity.theo_win_subquery_dim
        time_dimensions:
          - dimension: activity.settled_at
            granularity: month
        order:
          - id: activity.played_at
    "#},
    )
    .await;
}

// The leaf computes the tier over all rows (410, high); the OR then keeps p2's rows.
#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_dim_mixed_or_group_with_measure() {
    assert_query(
        "multi_stage_dim_mixed_or_group_with_measure",
        indoc! {r#"
        measures:
          - activity.theo_win
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
    "#},
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_dim_mixed_or_group_without_measure() {
    assert_query(
        "multi_stage_dim_mixed_or_group_without_measure",
        indoc! {r#"
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
    "#},
    )
    .await;
}

// Per player the tier is p1 high, p2 low; the OR keeps both players.
#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_dim_mixed_or_group_grouped_by_player() {
    assert_query(
        "multi_stage_dim_mixed_or_group_grouped_by_player",
        indoc! {r#"
        dimensions:
          - activity.player
          - activity.tier_ms_dim
        filters:
          - or:
              - member: activity.tier_ms_dim
                operator: equals
                values:
                  - high
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
async fn test_multi_stage_dim_or_group_on_itself_only() {
    assert_query(
        "multi_stage_dim_or_group_on_itself_only",
        indoc! {r#"
        dimensions:
          - activity.tier_ms_dim
        filters:
          - or:
              - member: activity.tier_ms_dim
                operator: equals
                values:
                  - low
              - member: activity.tier_ms_dim
                operator: equals
                values:
                  - none
          - member: activity.player
            operator: equals
            values:
              - p2
    "#},
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_or_group_over_two_multi_stage_dims() {
    assert_query(
        "or_group_over_two_multi_stage_dims",
        indoc! {r#"
        dimensions:
          - activity.tier_ms_dim
          - activity.tier_subquery_dim
        filters:
          - or:
              - member: activity.tier_ms_dim
                operator: equals
                values:
                  - low
              - member: activity.tier_subquery_dim
                operator: equals
                values:
                  - none
          - member: activity.player
            operator: equals
            values:
              - p2
    "#},
    )
    .await;
}

// The OR is nested in an AND: the leaf keeps the date range and drops only the OR.
#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_dim_mixed_or_group_nested_in_and() {
    assert_query(
        "multi_stage_dim_mixed_or_group_nested_in_and",
        indoc! {r#"
        measures:
          - activity.theo_win
        dimensions:
          - activity.tier_ms_dim
        filters:
          - and:
              - or:
                  - member: activity.tier_ms_dim
                    operator: equals
                    values:
                      - high
                  - member: activity.player
                    operator: equals
                    values:
                      - p2
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

// p1 (300) is high and passes the OR; p2 (110) is low and above 100, so it fails.
#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_measure_in_mixed_or_measure_filter() {
    assert_query(
        "multi_stage_measure_in_mixed_or_measure_filter",
        indoc! {r#"
        measures:
          - activity.theo_win
        dimensions:
          - activity.player
        filters:
          - or:
              - member: activity.tier
                operator: equals
                values:
                  - high
              - member: activity.theo_win
                operator: lt
                values:
                  - "100"
    "#},
    )
    .await;
}
