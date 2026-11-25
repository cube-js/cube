use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    TestContext::new(MockSchema::from_yaml_file(
        "common/view_dimension_overrides.yaml",
    ))
    .unwrap()
}

const SEED: &str = "view_dimension_overrides_tables.sql";

// Cancelled in 2026: subscriptions 1 and 3, not the one purchased in 2026.
#[tokio::test(flavor = "multi_thread")]
async fn test_filter_on_overridden_dimension() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - biz.cancellations_cancellations
        filters:
          - member: biz.dt_year
            operator: equals
            values: ["2026"]
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert!(sql.contains("subscriptions__cancel_cal"), "{sql}");
    assert!(!sql.contains("subscriptions__purchase_cal"), "{sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// 2026-02: 2.
#[tokio::test(flavor = "multi_thread")]
async fn test_group_by_overridden_dimension() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - biz.cancellations_cancellations
        dimensions:
          - biz.dt_year_month
        filters:
          - member: biz.dt_year
            operator: equals
            values: ["2026"]
        order:
          - id: biz.dt_year_month
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Purchased in 2026: 2026-01: 1 (subscription 3), 2026-02: 1 (subscription 4).
#[tokio::test(flavor = "multi_thread")]
async fn test_other_role_of_the_same_fact() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - biz.purchases_count
        dimensions:
          - biz.dt_year_month
        filters:
          - member: biz.dt_year
            operator: equals
            values: ["2026"]
        order:
          - id: biz.dt_year_month
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// 2026-02-01: 2.
#[tokio::test(flavor = "multi_thread")]
async fn test_overridden_time_dimension_granularity() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - biz.cancellations_cancellations
        time_dimensions:
          - dimension: biz.dt_ts
            granularity: month
            dateRange:
              - "2026-01-01"
              - "2026-12-31"
        order:
          - id: biz.dt_ts
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Both measures of the cancellations include share its overrides: 2 and 2.
#[tokio::test(flavor = "multi_thread")]
async fn test_measures_sharing_overrides() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - biz.cancellations_count
          - biz.cancellations_cancellations
        filters:
          - member: biz.dt_year
            operator: equals
            values: ["2026"]
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// A dimension of the fact itself is not overridden: pro: 2.
#[tokio::test(flavor = "multi_thread")]
async fn test_overridden_next_to_plain_dimension() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - biz.cancellations_cancellations
        dimensions:
          - biz.subs_plan
        filters:
          - member: biz.dt_year
            operator: equals
            values: ["2026"]
        order:
          - id: biz.subs_plan
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Without a calendar dimension there is nothing to override: all 3 cancellations.
#[tokio::test(flavor = "multi_thread")]
async fn test_no_overridden_dimension_in_query() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - biz.cancellations_cancellations
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert!(!sql.contains("vdo_calendar"), "{sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// A measure without overrides keeps its own join: 2025-09: 1, 2026-02: 2.
#[tokio::test(flavor = "multi_thread")]
async fn test_measure_without_overrides() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - biz.carts_count
        dimensions:
          - biz.dt_year_month
        order:
          - id: biz.dt_year_month
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[test]
fn test_multi_stage_measure_with_overrides_is_rejected() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - biz.cancellations_cancellations_cumulative
        time_dimensions:
          - dimension: biz.dt_ts
            granularity: month
            dateRange:
              - "2026-01-01"
              - "2026-03-31"
    "#};

    let error = ctx.build_sql(query).unwrap_err();
    assert!(
        error
            .message
            .contains("Dimension overrides of multi-stage measure biz.cancellations_cancellations_cumulative are not supported yet"),
        "{}",
        error.message
    );
}

// Each fact on its own date, one filter: 2026-02: 2 cancellations, 2 carts.
#[tokio::test(flavor = "multi_thread")]
async fn test_overridden_and_plain_measures_by_one_dimension() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - biz.cancellations_cancellations
          - biz.carts_count
        dimensions:
          - biz.dt_year_month
        filters:
          - member: biz.dt_year
            operator: equals
            values: ["2026"]
        order:
          - id: biz.dt_year_month
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// 2026-01: 1 purchase. 2026-02: 1 purchase, 2 cancellations, 2 carts.
#[tokio::test(flavor = "multi_thread")]
async fn test_two_roles_of_one_fact_and_another_fact() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - biz.purchases_count
          - biz.cancellations_cancellations
          - biz.carts_count
        dimensions:
          - biz.dt_year_month
        filters:
          - member: biz.dt_year
            operator: equals
            values: ["2026"]
        order:
          - id: biz.dt_year_month
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Purchased in 2026: 2 (3, 4); cancelled in 2026: 2 (1, 3). One filter must
// not narrow both roles to the subscription that has both dates in 2026.
#[tokio::test(flavor = "multi_thread")]
async fn test_two_roles_of_one_fact_by_filter_only() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - biz.purchases_count
          - biz.cancellations_cancellations
        filters:
          - member: biz.dt_year
            operator: equals
            values: ["2026"]
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// The same cube measure under two roles: 2026-01: 1 / -, 2026-02: 1 / 2.
#[tokio::test(flavor = "multi_thread")]
async fn test_one_cube_measure_under_two_roles() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - biz.purchases_count
          - biz.cancellations_count
        dimensions:
          - biz.dt_year_month
        filters:
          - member: biz.dt_year
            operator: equals
            values: ["2026"]
        order:
          - id: biz.dt_year_month
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// 2026-01-01: 1 purchase. 2026-02-01: 1 purchase, 2 cancellations.
#[tokio::test(flavor = "multi_thread")]
async fn test_two_roles_by_time_dimension_granularity() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - biz.purchases_count
          - biz.cancellations_cancellations
        time_dimensions:
          - dimension: biz.dt_ts
            granularity: month
            dateRange:
              - "2026-01-01"
              - "2026-12-31"
        order:
          - id: biz.dt_ts
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// The measure filter applies over the merged legs: only 2026-02 has more than
// one cancellation, and it keeps its purchase and carts.
#[tokio::test(flavor = "multi_thread")]
async fn test_measure_filter_over_merged_legs() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - biz.purchases_count
          - biz.cancellations_cancellations
          - biz.carts_count
        dimensions:
          - biz.dt_year_month
        filters:
          - member: biz.dt_year
            operator: equals
            values: ["2026"]
          - member: biz.cancellations_cancellations
            operator: gt
            values: ["1"]
        order:
          - id: biz.dt_year_month
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Order and limit apply over the merged legs: the last month, 2026-02.
#[tokio::test(flavor = "multi_thread")]
async fn test_order_and_limit_over_merged_legs() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - biz.purchases_count
          - biz.cancellations_cancellations
        dimensions:
          - biz.dt_year_month
        filters:
          - member: biz.dt_year
            operator: equals
            values: ["2026"]
        order:
          - id: biz.dt_year_month
            desc: true
        limit: 1
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[test]
fn test_ungrouped_query_with_different_overrides_is_rejected() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - biz.purchases_count
          - biz.cancellations_cancellations
        dimensions:
          - biz.dt_year_month
        ungrouped: true
    "#};

    let error = ctx.build_sql(query).unwrap_err();
    assert!(
        error
            .message
            .contains("An ungrouped query can't have measures that need different dimensions"),
        "{}",
        error.message
    );
}

// A rolling measure without overrides next to an overridden one. Purchases:
// 2026-01: 1, 2026-02: 1. Carts so far: 2026-01: 1, 2026-02: 3, 2026-03: 3.
#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_measure_next_to_overridden_measure() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - biz.purchases_count
          - biz.carts_count_cumulative
        time_dimensions:
          - dimension: biz.dt_ts
            granularity: month
            dateRange:
              - "2026-01-01"
              - "2026-03-31"
        order:
          - id: biz.dt_ts
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// A view dimension built on an overridden one is overridden with it, alone:
// Y2025: 1 cancellation (2), Y2026: 2 (1, 3); subscription 4, never
// cancelled, has no cancel year: NULL, 0.
#[tokio::test(flavor = "multi_thread")]
async fn test_view_dimension_built_on_overridden_dimension() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - biz.cancellations_cancellations
        dimensions:
          - biz.dt_label
        order:
          - id: biz.dt_label
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// The same view dimension under two roles. Y2025: 2 purchases (1, 2),
// 1 cancellation (2); Y2026: 2 purchases (3, 4), 2 cancellations (1, 3);
// no cancel year: 0 cancellations (4).
#[tokio::test(flavor = "multi_thread")]
async fn test_view_dimension_built_on_overridden_dimension_two_roles() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - biz.purchases_count
          - biz.cancellations_cancellations
        dimensions:
          - biz.dt_label
        order:
          - id: biz.dt_label
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// A fact dimension next to overridden ones, two roles. Purchased in 2026:
// basic 1 (4), pro 1 (3). Cancelled in 2026: pro 2 (1, 3).
#[tokio::test(flavor = "multi_thread")]
async fn test_plain_dimension_next_to_two_roles() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - biz.purchases_count
          - biz.cancellations_cancellations
        dimensions:
          - biz.subs_plan
        filters:
          - member: biz.dt_year
            operator: equals
            values: ["2026"]
        order:
          - id: biz.subs_plan
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[test]
fn test_dimension_of_overridden_cube_left_without_override_is_rejected() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - biz.cancellations_cancellations
        dimensions:
          - biz.dt_quarter
        filters:
          - member: biz.dt_year
            operator: equals
            values: ["2026"]
    "#};

    let error = ctx.build_sql(query).unwrap_err();
    assert!(
        error.message.contains(
            "Dimension biz.dt_quarter is not overridden for measure biz.cancellations_cancellations, which overrides other dimensions of calendar"
        ),
        "{}",
        error.message
    );
}

// Overrides don't make another fact's dimension reachable.
#[test]
fn test_dimension_of_another_fact_has_no_join_path() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - biz.cancellations_cancellations
        dimensions:
          - biz.carts_id
        filters:
          - member: biz.dt_year
            operator: equals
            values: ["2026"]
    "#};

    let error = ctx.build_sql(query).unwrap_err();
    assert!(
        error.message.contains("Can't find join path"),
        "{}",
        error.message
    );
}

#[test]
fn test_measure_built_on_overridden_measures_is_rejected() {
    let ctx = create_context();
    for measure in [
        "biz.cancellations_per_purchase",
        "biz.cancellations_per_purchase_single_stage",
    ] {
        let query = format!(
            "measures:\n  - {measure}\nfilters:\n  - member: biz.dt_year\n    operator: equals\n    values: [\"2026\"]\n"
        );
        let error = ctx.build_sql(&query).unwrap_err();
        assert!(
            error
                .message
                .starts_with(&format!("Measure {measure} is built on biz."))
                && error
                    .message
                    .contains("whose dimension overrides apply to this query"),
            "{}",
            error.message
        );
    }
}

// Without an overridden dimension in the query nothing is overridden: 3
// cancellations of 4 purchases.
#[tokio::test(flavor = "multi_thread")]
async fn test_measure_built_on_overridden_measures_without_their_dimensions() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - biz.cancellations_per_purchase_single_stage
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[test]
fn test_masked_dimension_override_is_rejected() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - biz.cancellations_cancellations
        dimensions:
          - biz.dt_year_hidden
    "#};

    let error = ctx.build_sql(query).unwrap_err();
    assert!(
        error
            .message
            .contains("Masked dimension biz.dt_year_hidden can't be overridden yet"),
        "{}",
        error.message
    );
}
