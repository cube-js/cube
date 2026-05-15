use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const SEED: &str = "view_default_filters_tables.sql";

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/view_default_filters.yaml");
    TestContext::new(schema).unwrap()
}

// `currency` is a virtual `type: switch` dimension with values
// `[USD, EUR, GBP]`. Without a default filter the planner cross-joins every
// row with every value (5 rows × 3 currencies = 15 cells). The
// unconditional default filter `currency = USD` must collapse the union to
// the USD branch, leaving 5 rows.
//
// This is CORE-357: the customer expected a default value here, not the
// union; without the default filter `amount_in_currency` / `count`
// silently rolls up across all currencies.
#[tokio::test(flavor = "multi_thread")]
async fn test_virtual_switch_default_filter_collapses_union() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders_view.count
        dimensions:
          - orders_view.currency
        order:
          - id: orders_view.currency
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert!(
        sql.contains("'USD' = $"),
        "expected the default switch-value filter `'USD' = ?` in SQL, got: {sql}"
    );
    assert!(
        !sql.contains("'EUR'"),
        "EUR branch must be pruned by the default filter, got: {sql}"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// `unless` is filter-only: pulling the member into a projection does NOT
// release the default filter, because that would make the row count
// silently depend on which columns the user selects. Same view as the
// "with_unless" fixture, currency now in dimensions — and the default
// filter still applies. The user sees a single USD row, just like the
// unconditional view above.
#[tokio::test(flavor = "multi_thread")]
async fn test_unless_does_not_trigger_on_projection_alone() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders_view_with_unless.count
        dimensions:
          - orders_view_with_unless.currency
        order:
          - id: orders_view_with_unless.currency
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert!(
        sql.contains("'USD' = $"),
        "projection alone must not release the default filter, got: {sql}"
    );
    assert!(
        !sql.contains("'EUR'"),
        "EUR branch must stay pruned when only projection touches `currency`, got: {sql}"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// `unless` is filter-only: an explicit filter on the unless-member is the
// only thing that releases the default. The user filters by `EUR`, the
// `USD` default is dropped, and only EUR rows come back.
#[tokio::test(flavor = "multi_thread")]
async fn test_explicit_filter_overrides_default() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders_view_with_unless.count
        dimensions:
          - orders_view_with_unless.currency
        filters:
          - member: orders_view_with_unless.currency
            operator: equals
            values:
              - EUR
        order:
          - id: orders_view_with_unless.currency
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert!(
        !sql.contains("'USD' = $"),
        "an explicit filter on the unless-member must release the default, got: {sql}"
    );
    assert!(
        sql.contains("'EUR' = $"),
        "user-supplied EUR filter must reach the SQL as a `'EUR' = ?` switch filter, got: {sql}"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// `unless: [currency]` — but the user doesn't touch `currency`, so the
// default filter is still in effect. The query groups by `country`, the
// virtual switch is not in dimensions, so there is no cross-join — the
// default filter still fires (visible as `'USD' = $`) but the result
// table is just the per-country counts.
#[tokio::test(flavor = "multi_thread")]
async fn test_unless_keeps_default_filter_when_member_is_not_touched() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders_view_with_unless.count
        dimensions:
          - orders_view_with_unless.country
        order:
          - id: orders_view_with_unless.country
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert!(
        sql.contains("'USD' = $"),
        "default filter must apply when `unless` member is absent, got: {sql}"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// `amount_in_country` is a switch-case measure dispatched by `country`.
// With `country` in dimensions every branch resolves correctly (US/CA →
// USD amounts, DE/FR → EUR amounts, GB falls through to `else`). The
// unconditional default filter on virtual `currency` rides along — it
// must still appear in the SQL to confirm the filter wiring reaches
// switch-case measures the same way it reaches plain counts.
#[tokio::test(flavor = "multi_thread")]
async fn test_switch_case_measure_with_default_filter() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders_view.amount_in_country
        dimensions:
          - orders_view.country
        order:
          - id: orders_view.country
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert!(
        sql.contains("'USD' = $"),
        "default filter must apply alongside a switch-case measure, got: {sql}"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
