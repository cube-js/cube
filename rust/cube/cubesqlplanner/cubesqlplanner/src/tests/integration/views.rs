use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_views.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_fact_tables.sql";

// 8.1: Basic view query — single cube view
#[tokio::test(flavor = "multi_thread")]
async fn test_view_basic_query() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders_view.count
          - orders_view.total_amount
        dimensions:
          - orders_view.status
        order:
          - id: orders_view.status
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// 8.2: View with join — dims/measures from different cubes
#[tokio::test(flavor = "multi_thread")]
async fn test_view_with_join() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders_with_customers.count
          - orders_with_customers.total_amount
        dimensions:
          - orders_with_customers.name
        order:
          - id: orders_with_customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// 8.3: View with segment
#[tokio::test(flavor = "multi_thread")]
async fn test_view_with_segment() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders_with_customers.count
          - orders_with_customers.total_amount
        dimensions:
          - orders_with_customers.name
        segments:
          - orders_with_customers.ny_customers
        order:
          - id: orders_with_customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// 8.4: View with filter
#[tokio::test(flavor = "multi_thread")]
async fn test_view_with_filter() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders_with_customers.count
          - orders_with_customers.total_amount
        dimensions:
          - orders_with_customers.name
        filters:
          - dimension: orders_with_customers.city
            operator: equals
            values:
              - New York
        order:
          - id: orders_with_customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// 8.5: Multi-fact view — measures from orders + returns through one view
#[tokio::test(flavor = "multi_thread")]
async fn test_view_multi_fact() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - customer_overview.orders_count
          - customer_overview.orders_total_amount
          - customer_overview.returns_count
          - customer_overview.returns_total_refund
        dimensions:
          - customer_overview.name
        order:
          - id: customer_overview.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_with_time_dimension() {
    let ctx = create_context();

    // orders_with_customers by day, dateRange Mar1-Mar4
    // Mar1=1, Mar2=1, Mar3=1, Mar4=1
    let query = indoc! {"
        measures:
          - orders_with_customers.count
        time_dimensions:
          - dimension: orders_with_customers.created_at
            granularity: day
            dateRange:
              - \"2025-03-01\"
              - \"2025-03-04\"
        order:
          - id: orders_with_customers.created_at
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_with_order_and_limit() {
    let ctx = create_context();

    // orders_with_customers by name, order total desc, limit 2
    // Bob=550, Alice=425, Diana=400 → top 2: Bob, Alice
    let query = indoc! {"
        measures:
          - orders_with_customers.count
          - orders_with_customers.total_amount
        dimensions:
          - orders_with_customers.name
        order:
          - id: orders_with_customers.total_amount
            desc: true
        row_limit: \"2\"
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_ungrouped() {
    let ctx = create_context();

    // orders_view ungrouped — all 8 orders as individual rows
    let query = indoc! {"
        measures:
          - orders_view.count
        dimensions:
          - orders_view.id
          - orders_view.status
        ungrouped: true
        order:
          - id: orders_view.id
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

fn tz_conversions_count(sql: &str) -> usize {
    sql.matches("AT TIME ZONE").count()
}

fn raw_time_dimension_query(members_yaml: &str) -> String {
    format!(
        indoc! {"
            {}timezone: \"America/Chicago\"
            convert_tz_for_raw_time_dimension: true
        "},
        members_yaml
    )
}

// A `type: time` dimension asked for without a granularity is converted into the
// query timezone once. A view re-exposing such a dimension is not a second
// conversion site: the value is read, and converted, on the owning cube.
//
// The count pins how many conversions the query applies; the result pins that
// they land on the column, with the offset going the right way.
#[tokio::test(flavor = "multi_thread")]
async fn test_view_raw_time_dimension_converted_once() {
    let ctx = create_context();
    let query = raw_time_dimension_query(indoc! {"
        measures:
          - orders_with_customers.count
        dimensions:
          - orders_with_customers.created_at
        order:
          - id: orders_with_customers.created_at
    "});

    let sql = ctx.build_sql(&query).unwrap();
    assert_eq!(tz_conversions_count(&sql), 1, "got: {sql}");

    if let Some(result) = ctx.try_execute_pg(&query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_raw_and_granular_time_dimension_converted_once_each() {
    let ctx = create_context();
    let query = raw_time_dimension_query(indoc! {"
        measures:
          - orders_with_customers.count
        dimensions:
          - orders_with_customers.created_at
        time_dimensions:
          - dimension: orders_with_customers.created_at
            granularity: day
        order:
          - id: orders_with_customers.created_at
    "});

    let sql = ctx.build_sql(&query).unwrap();
    assert_eq!(tz_conversions_count(&sql), 2, "got: {sql}");

    if let Some(result) = ctx.try_execute_pg(&query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// A view that declares a cube both as a root of its own and as the tail of a
/// longer join path. A query over the members of the independent root alone
/// must stay on that root: the longer path serves the members declared under
/// it, and walking it here would fan the measures out over `locations` and put
/// pre-aggregation matching on the strict, multiplied path.
fn independent_roots_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/view_independent_join_roots.yaml");
    TestContext::new(schema).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_independent_root_query_does_not_walk_the_dotted_path() {
    let ctx = independent_roots_context();

    let query = indoc! {"
        measures:
          - kpi.yield_pct
        dimensions:
          - kpi.board_id
        order:
          - id: kpi.board_id
    "};

    let (sql, pre_aggregations) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();

    assert!(
        !sql.contains("locations"),
        "the query touches no member of locations, got: {sql}"
    );
    assert_eq!(
        pre_aggregations.len(),
        1,
        "the rollup on boards covers the query, got: {sql}"
    );
    assert_eq!(pre_aggregations[0].name(), "boards_rollup");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_dotted_path_query_still_walks_it() {
    let ctx = independent_roots_context();

    let query = indoc! {"
        measures:
          - kpi.count
        dimensions:
          - kpi.product
        order:
          - id: kpi.product
    "};

    let sql = ctx.build_sql(query).unwrap();

    assert!(
        sql.contains("locations"),
        "the query counts locations rows, got: {sql}"
    );
    assert!(sql.contains("boards"), "got: {sql}");
}
