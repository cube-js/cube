use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const BASIC_SEED: &str = "integration_basic_tables.sql";
const MULTI_FACT_SEED: &str = "integration_multi_fact_tables.sql";

fn create_filtered_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_filtered_measures.yaml");
    TestContext::new(schema).unwrap()
}

fn create_multi_fact_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_fact.yaml");
    TestContext::new(schema).unwrap()
}

// --- Batch 1: Filtered Measures ---

// completed orders: ids 1,2,4,6,8 → count=5
#[tokio::test(flavor = "multi_thread")]
async fn test_filtered_count() {
    let ctx = create_filtered_context();

    let query = indoc! {"
        measures:
          - orders.completed_count
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// completed orders sum: 100+200+300+150+500=1250
#[tokio::test(flavor = "multi_thread")]
async fn test_filtered_sum() {
    let ctx = create_filtered_context();

    let query = indoc! {"
        measures:
          - orders.completed_total
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// completed orders unique customers: {1,2,4,5} → 4
#[tokio::test(flavor = "multi_thread")]
async fn test_filtered_count_distinct() {
    let ctx = create_filtered_context();

    let query = indoc! {"
        measures:
          - orders.unique_completed_customers
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Regular count=9, filtered completed_count=5
#[tokio::test(flavor = "multi_thread")]
async fn test_filtered_and_regular() {
    let ctx = create_filtered_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.completed_count
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Filtered measure + dimension grouping
#[tokio::test(flavor = "multi_thread")]
async fn test_filtered_with_dimension() {
    let ctx = create_filtered_context();

    let query = indoc! {"
        measures:
          - orders.completed_count
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Filtered measure by month: Jan:2(ids 1,2), Feb:1(id 4), Mar:1(id 6), Apr:1(id 8)
#[tokio::test(flavor = "multi_thread")]
async fn test_filtered_with_time() {
    let ctx = create_filtered_context();

    let query = indoc! {"
        measures:
          - orders.completed_count
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
        order:
          - id: orders.created_at
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Filtered measure + query filter: completed with amount>=200 → ids 2,4,8 → count=3
#[tokio::test(flavor = "multi_thread")]
async fn test_filtered_with_query_filter() {
    let ctx = create_filtered_context();

    let query = indoc! {"
        measures:
          - orders.completed_count
        filters:
          - dimension: orders.amount
            operator: gte
            values:
              - \"200\"
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// --- Batch 2: Cross-Cube Calculated Measures ---

// Cross-cube number measure per city
// NY: Alice(350/1)+Diana(400/0), Boston: Bob(550/2), Chicago: Charlie(0/2)
#[tokio::test(flavor = "multi_thread")]
async fn test_cross_cube_number() {
    let ctx = create_multi_fact_context();

    let query = indoc! {"
        measures:
          - orders.avg_amount_per_return
        dimensions:
          - customers.city
        order:
          - id: customers.city
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, MULTI_FACT_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Per customer: Alice:350/1=350, Bob:550/2=275, Charlie:0/2=0, Diana:400/0=NULL
#[tokio::test(flavor = "multi_thread")]
async fn test_cross_cube_with_dimension() {
    let ctx = create_multi_fact_context();

    let query = indoc! {"
        measures:
          - orders.avg_amount_per_return
        dimensions:
          - customers.name
        order:
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, MULTI_FACT_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Cross-cube number by time + customer hub dimension
#[tokio::test(flavor = "multi_thread")]
async fn test_cross_cube_with_time() {
    let ctx = create_multi_fact_context();

    let query = indoc! {"
        measures:
          - orders.avg_amount_per_return
        dimensions:
          - customers.name
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
        order:
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, MULTI_FACT_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Cross-cube number + filter city='New York' → Alice + Diana
#[tokio::test(flavor = "multi_thread")]
async fn test_cross_cube_with_filter() {
    let ctx = create_multi_fact_context();

    let query = indoc! {"
        measures:
          - orders.avg_amount_per_return
        filters:
          - dimension: customers.city
            operator: equals
            values:
              - New York
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, MULTI_FACT_SEED).await {
        insta::assert_snapshot!(result);
    }
}
