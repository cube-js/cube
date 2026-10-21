use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::formatdoc;

const SEED: &str = "multi_stage_dimension_views_tables.sql";

async fn run(dimension: &str, measure: Option<&str>) -> Option<String> {
    run_query(dimension, &[], measure).await
}

async fn run_query(dimension: &str, extra_dims: &[&str], measure: Option<&str>) -> Option<String> {
    let schema = MockSchema::from_yaml_file("common/multi_stage_dimension_views.yaml");
    let ctx = TestContext::new(schema).unwrap();
    let extra_dims = extra_dims
        .iter()
        .map(|d| format!("  - {d}\n"))
        .collect::<String>();
    let measures = measure
        .map(|m| format!("measures:\n  - {m}\n"))
        .unwrap_or_default();
    let query = formatdoc! {"
        dimensions:
          - {dimension}
        {extra_dims}{measures}order:
          - id: {dimension}
    "};
    ctx.build_sql(&query)
        .unwrap_or_else(|e| panic!("failed to plan {dimension}: {e}"));
    ctx.try_execute_pg(&query, SEED).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subquery_dim_on_cube() {
    if let Some(result) = run("activity.tier_subquery_dim", None).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_dim_on_cube() {
    if let Some(result) = run("activity.tier_ms_dim", None).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subquery_dim_through_view() {
    if let Some(result) = run("activity_view.tier_subquery_dim", None).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_dim_through_view() {
    if let Some(result) = run("activity_view.tier_ms_dim", None).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subquery_dim_through_view_with_measure() {
    if let Some(result) = run(
        "activity_view.tier_subquery_dim",
        Some("activity_view.theo_win"),
    )
    .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_dim_through_view_with_measure() {
    if let Some(result) = run("activity_view.tier_ms_dim", Some("activity_view.theo_win")).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subquery_dim_through_prefixed_view() {
    if let Some(result) = run("activity_prefixed_view.activity_tier_subquery_dim", None).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_dim_through_prefixed_view() {
    if let Some(result) = run("activity_prefixed_view.activity_tier_ms_dim", None).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subquery_dim_through_view_by_player() {
    if let Some(result) = run_query(
        "activity_view.tier_subquery_dim",
        &["activity_view.player"],
        Some("activity_view.theo_win"),
    )
    .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_dim_through_view_by_player() {
    if let Some(result) = run_query(
        "activity_view.tier_ms_dim",
        &["activity_view.player"],
        Some("activity_view.theo_win"),
    )
    .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_same_dim_on_cube_and_through_views() {
    for (name, first, second) in [
        (
            "ms_dim_cube_and_view",
            "activity.tier_ms_dim",
            "activity_view.tier_ms_dim",
        ),
        (
            "subquery_dim_cube_and_view",
            "activity.tier_subquery_dim",
            "activity_view.tier_subquery_dim",
        ),
        (
            "ms_dim_view_and_prefixed_view",
            "activity_view.tier_ms_dim",
            "activity_prefixed_view.activity_tier_ms_dim",
        ),
    ] {
        let schema = MockSchema::from_yaml_file("common/multi_stage_dimension_views.yaml");
        let ctx = TestContext::new(schema).unwrap();
        let query = formatdoc! {"
            dimensions:
              - {first}
              - {second}
              - activity_view.player
            measures:
              - activity_view.theo_win
            order:
              - id: activity_view.player
        "};
        ctx.build_sql(&query)
            .unwrap_or_else(|e| panic!("failed to plan {first} + {second}: {e}"));
        if let Some(result) = ctx.try_execute_pg(&query, SEED).await {
            insta::assert_snapshot!(name, result);
        }
    }
}
