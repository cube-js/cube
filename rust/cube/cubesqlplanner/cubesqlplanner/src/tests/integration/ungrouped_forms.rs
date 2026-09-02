//! Ungrouped rendering of measures in the contexts where it combines
//! with other machinery: masking, multiplied measures, ORDER BY of an
//! unselected measure, rolling windows over count-like measures.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const BASIC_SEED: &str = "integration_basic_tables.sql";
const JOINS_SEED: &str = "integration_joins_tables.sql";
const ROLLING_SEED: &str = "integration_rolling_window_tables.sql";

fn create_basic_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_basic.yaml");
    TestContext::new(schema).unwrap()
}

fn create_joins_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_joins.yaml");
    TestContext::new(schema).unwrap()
}

fn create_rolling_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_rolling_window.yaml");
    TestContext::new(schema).unwrap()
}

fn create_masking_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_masking.yaml");
    TestContext::new(schema).unwrap()
}

const MASKED_MEMBERS: &str = indoc! {"
    maskedMembers:
      - member: orders.masked_total
      - member: orders.masked_total_const
"};

// A masked measure in an ungrouped query must stay masked. The mask
// whose SQL references another member takes the row-level masking
// path; the constant mask is the control.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_masked_measures() {
    let ctx = create_masking_context();

    let query = indoc! {"
        measures:
          - orders.masked_total
          - orders.masked_total_const
        dimensions:
          - orders.id
          - orders.status
        order:
          - id: orders.id
        ungrouped: true
    "};
    let query = format!("{}{}", query, MASKED_MEMBERS);

    ctx.build_sql(&query).unwrap();

    if let Some(result) = ctx.try_execute_pg(&query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// A conditional mask (mask filter) on a measure whose mask SQL has
// row-level dependencies, in an ungrouped query. The dependency-
// carrying mask is applied with grouped semantics at the evaluate
// position, where the filter cannot be turned into a CASE WHEN — so
// every row must render the mask value; the original value must not
// leak through rows matching the filter.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_conditional_dep_mask() {
    let ctx = create_masking_context();

    let query = indoc! {"
        measures:
          - orders.masked_total
        dimensions:
          - orders.id
          - orders.status
        order:
          - id: orders.id
        ungrouped: true
        maskedMembers:
          - member: orders.masked_total
            filter:
              member: orders.status
              operator: equals
              values: ['completed']
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Control: a constant-masked measure in a grouped query. The mask
// whose SQL has row-level dependencies is not representable in a
// grouped select and is exercised by the ungrouped test only.
#[tokio::test(flavor = "multi_thread")]
async fn test_grouped_masked_measures_control() {
    let ctx = create_masking_context();

    let query = indoc! {"
        measures:
          - orders.masked_total_const
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "};
    let query = format!("{}{}", query, MASKED_MEMBERS);

    ctx.build_sql(&query).unwrap();

    if let Some(result) = ctx.try_execute_pg(&query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// A multiplied measure (customers.count is multiplied by the join to
// orders) in an ungrouped query: the multiplied subquery must keep its
// distinct aggregation while the outer select emits row-level values.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_multiplied_count() {
    let ctx = create_joins_context();

    let query = indoc! {"
        measures:
          - customers.count
          - orders.count
        dimensions:
          - orders.status
        order:
          - id: orders.status
        ungrouped: true
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, JOINS_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// ORDER BY a measure that is not in the selection list of an
// ungrouped query: the sort key must be the row-level value, not an
// aggregate.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_order_by_unselected_measure() {
    let ctx = create_basic_context();

    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.id
          - orders.status
        order:
          - id: orders.total_amount
            desc: true
          - id: orders.id
        ungrouped: true
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// A rolling count-distinct in an ungrouped query: the leaf emits the
// raw distinct key for the window stage to count, not a not-null
// indicator.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_rolling_count_distinct() {
    let ctx = create_rolling_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_unique_customers_7d
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-10"
              - "2024-01-20"
        ungrouped: true
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, ROLLING_SEED).await {
        insta::assert_snapshot!(result);
    }
}
