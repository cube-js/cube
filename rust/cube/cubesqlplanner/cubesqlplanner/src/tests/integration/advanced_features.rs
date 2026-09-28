use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const BASIC_SEED: &str = "integration_basic_tables.sql";
const GEO_SEED: &str = "integration_geo_tables.sql";

fn create_basic_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_basic.yaml");
    TestContext::new(schema).unwrap()
}

fn create_filtered_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_filtered_measures.yaml");
    TestContext::new(schema).unwrap()
}

fn create_geo_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_geo.yaml");
    TestContext::new(schema).unwrap()
}

// --- Batch 7: Minute/Second Granularity ---

// Count by minute on 2024-01-15: 10:00→1 (order 1), 15:00→1 (order 9)
#[tokio::test(flavor = "multi_thread")]
async fn test_minute_granularity() {
    let ctx = create_basic_context();

    let query = indoc! {"
        measures:
          - orders.count
        time_dimensions:
          - dimension: orders.created_at
            granularity: minute
            dateRange:
              - \"2024-01-15\"
              - \"2024-01-15\"
        order:
          - id: orders.created_at
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Count by second on 2024-01-15
#[tokio::test(flavor = "multi_thread")]
async fn test_second_granularity() {
    let ctx = create_basic_context();

    let query = indoc! {"
        measures:
          - orders.count
        time_dimensions:
          - dimension: orders.created_at
            granularity: second
            dateRange:
              - \"2024-01-15\"
              - \"2024-01-15\"
        order:
          - id: orders.created_at
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// --- Batch 8: Geo Dimensions ---

// Geo query: 3 rows with lat/lng + name
#[tokio::test(flavor = "multi_thread")]
async fn test_geo_query() {
    let ctx = create_geo_context();

    let query = indoc! {"
        dimensions:
          - stores.name
          - stores.location
        order:
          - id: stores.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, GEO_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Geo with measure: total_revenue per store
#[tokio::test(flavor = "multi_thread")]
async fn test_geo_with_measure() {
    let ctx = create_geo_context();

    let query = indoc! {"
        measures:
          - stores.total_revenue
        dimensions:
          - stores.name
          - stores.location
        order:
          - id: stores.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, GEO_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// --- Batch 9: Ungrouped + Filtered/Ratio ---

// Ungrouped + filtered measure: 9 rows, completed→1 each, others→0
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_filtered_measure() {
    let ctx = create_filtered_context();

    let query = indoc! {"
        measures:
          - orders.completed_count
        dimensions:
          - orders.id
          - orders.status
        ungrouped: true
        order:
          - id: orders.id
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Ungrouped + filtered count_distinct
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_filtered_count_distinct() {
    let ctx = create_filtered_context();

    let query = indoc! {"
        measures:
          - orders.unique_completed_customers
        dimensions:
          - orders.id
        ungrouped: true
        order:
          - id: orders.id
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Ungrouped + ratio measure (avg_order_value = total/count)
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_ratio_measure() {
    let ctx = create_filtered_context();

    let query = indoc! {"
        measures:
          - orders.avg_order_value
        dimensions:
          - orders.id
          - orders.status
          - orders.amount
        ungrouped: true
        order:
          - id: orders.id
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// --- Batch 11: String Measures ---

// status_summary: 9 orders > 3 → 'high'
#[tokio::test(flavor = "multi_thread")]
async fn test_string_measure() {
    let ctx = create_basic_context();

    let query = indoc! {"
        measures:
          - orders.status_summary
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// status_summary + city: NY:4→high, SF:2→low, NULL:2→low, Boston:1→low
#[tokio::test(flavor = "multi_thread")]
async fn test_string_measure_with_dim() {
    let ctx = create_basic_context();

    let query = indoc! {"
        measures:
          - orders.status_summary
        dimensions:
          - customers.city
        order:
          - id: customers.city
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}
