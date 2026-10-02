use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    TestContext::new(MockSchema::from_yaml_file("common/join_aliases.yaml")).unwrap()
}

const SEED: &str = "join_aliases_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_role_playing_aliases() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - orders.count
        dimensions:
          - orders.customer.city
          - orders.manager.city
        order:
          - id: orders.customer.city
          - id: orders.manager.city
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_single_alias() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - orders.count
        dimensions:
          - orders.customer.city
        order:
          - id: orders.customer.city
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_join_below_an_alias() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - orders.count
        dimensions:
          - orders.customer.departments.name
          - orders.manager.departments.name
        order:
          - id: orders.customer.departments.name
          - id: orders.manager.departments.name
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_instance_member_reading_a_joined_cube() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - orders.count
        dimensions:
          - orders.customer.department_name
        order:
          - id: orders.customer.department_name
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_self_join() {
    let ctx = create_context();
    let query = indoc! {r#"
        dimensions:
          - employees.name
          - employees.supervisor.name
        order:
          - id: employees.name
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_alias_in_declaring_cube_member() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - orders.total_amount
        dimensions:
          - orders.customer_city
        order:
          - id: orders.customer_city
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_measure_of_an_instance() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - orders.customer.total_score
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_instance_measure_next_to_root_measure() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - orders.count
          - orders.manager.count
        dimensions:
          - orders.customer.city
        order:
          - id: orders.customer.city
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_filter_on_an_instance() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - orders.total_amount
        dimensions:
          - orders.customer.city
        filters:
          - member: orders.manager.city
            operator: equals
            values:
              - Paris
        order:
          - id: orders.customer.city
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_sub_query_dimension_on_an_instance() {
    let ctx = create_context();
    let query = indoc! {r#"
        dimensions:
          - orders.id
          - orders.customer.review_count
        order:
          - id: orders.id
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_through_an_alias() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - orders_view.count
        dimensions:
          - orders_view.customer_city
        order:
          - id: orders_view.customer_city
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_calendar_instances() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - orders.count
        dimensions:
          - orders.calendar_created.date_val.month
          - orders.calendar_completed.date_val.month
        order:
          - id: orders.calendar_created.date_val.month
          - id: orders.calendar_completed.date_val.month
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_calendar_shift_through_an_alias() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - orders.count
          - orders.count_prev_year
        time_dimensions:
          - dimension: orders.calendar_created.retail_date
            granularity: year
            dateRange:
              - "2025-01-01"
              - "2025-12-31"
        order:
          - id: orders.calendar_created.retail_date
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// The shift of one calendar instance leaves the other calendar's join alone.
#[tokio::test(flavor = "multi_thread")]
async fn test_calendar_shift_through_one_alias_next_to_another() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - orders.count
          - orders.count_prev_year
        dimensions:
          - orders.calendar_completed.date_val.year
        time_dimensions:
          - dimension: orders.calendar_created.retail_date
            granularity: year
            dateRange:
              - "2025-01-01"
              - "2025-12-31"
        order:
          - id: orders.calendar_created.retail_date
          - id: orders.calendar_completed.date_val.year
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert!(
        sql.contains(r#"ON "orders".completed_at = "orders__calendar_completed".date_val"#),
        "{sql}"
    );
    assert!(
        sql.contains(r#"ON "orders".created_at = "orders__calendar_created".date_prev_year"#),
        "{sql}"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
