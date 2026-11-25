//! Two facts with no join and no shared dimension cube, blended by a view:
//! income and expenses each by their own date and branch.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    TestContext::new(MockSchema::from_yaml_file(
        "common/view_dimension_overrides_unrelated_facts.yaml",
    ))
    .unwrap()
}

const SEED: &str = "view_dimension_overrides_unrelated_facts_tables.sql";

// 2026-01: North 150 / 40, South 70 / 25. 2026-02: East - / 10, North 20 / -,
// South 30 / 60.
#[tokio::test(flavor = "multi_thread")]
async fn test_income_and_expenses_by_month_and_branch() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - finance.income
          - finance.expenses
        dimensions:
          - finance.branch
        time_dimensions:
          - dimension: finance.date
            granularity: month
            dateRange:
              - "2026-01-01"
              - "2026-02-28"
        order:
          - id: finance.date
          - id: finance.branch
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert!(!sql.contains("JOIN  vdo"), "{sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// East - / 10, North 170 / 40, South 100 / 85.
#[tokio::test(flavor = "multi_thread")]
async fn test_income_and_expenses_by_branch() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - finance.income
          - finance.expenses
        dimensions:
          - finance.branch
        order:
          - id: finance.branch
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// One branch, by month: 2026-01: 70 / 25, 2026-02: 30 / 60.
#[tokio::test(flavor = "multi_thread")]
async fn test_one_branch_by_filter() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - finance.income
          - finance.expenses
        time_dimensions:
          - dimension: finance.date
            granularity: month
        filters:
          - member: finance.branch
            operator: equals
            values: ["South"]
        order:
          - id: finance.date
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// The whole period, by the date range alone: 270 / 135.
#[tokio::test(flavor = "multi_thread")]
async fn test_totals_over_a_date_range() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - finance.income
          - finance.expenses
        time_dimensions:
          - dimension: finance.date
            dateRange:
              - "2026-01-01"
              - "2026-02-28"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Income minus expenses, computed over the two facts merged by month and
// branch. 2026-01: North 110, South 45. 2026-02: East -10, North 20, South -30.
#[tokio::test(flavor = "multi_thread")]
async fn test_balance_by_month_and_branch() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - finance.income
          - finance.expenses
          - finance.balance
        dimensions:
          - finance.branch
        time_dimensions:
          - dimension: finance.date
            granularity: month
            dateRange:
              - "2026-01-01"
              - "2026-02-28"
        order:
          - id: finance.date
          - id: finance.branch
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// The balance alone, by branch: East -10, North 130, South 15.
#[tokio::test(flavor = "multi_thread")]
async fn test_balance_by_branch() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - finance.balance
        dimensions:
          - finance.branch
        order:
          - id: finance.branch
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
