use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/date_filters.yaml");
    TestContext::new_with_timezone(schema, chrono_tz::America::Los_Angeles).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_in_date_range() {
    let ctx = create_context();

    let query = indoc! {r#"
        dimensions:
          - visitors.created_at
        filters:
          - dimension: visitors.created_at
            operator: inDateRange
            values:
              - "2017-01-01"
              - "2017-01-03"
        order:
          - id: visitors.created_at
        timezone: "America/Los_Angeles"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, "date_filters_tables.sql").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_not_in_date_range() {
    let ctx = create_context();

    let query = indoc! {r#"
        dimensions:
          - visitors.created_at
        filters:
          - dimension: visitors.created_at
            operator: notInDateRange
            values:
              - "2017-01-01"
              - "2017-01-03"
        order:
          - id: visitors.created_at
        timezone: "America/Los_Angeles"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, "date_filters_tables.sql").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_on_the_date() {
    let ctx = create_context();

    let query = indoc! {r#"
        dimensions:
          - visitors.created_at
        filters:
          - dimension: visitors.created_at
            operator: onTheDate
            values:
              - "2017-01-06"
              - "2017-01-06"
        order:
          - id: visitors.created_at
        timezone: "America/Los_Angeles"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, "date_filters_tables.sql").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_before_date() {
    let ctx = create_context();

    let query = indoc! {r#"
        dimensions:
          - visitors.created_at
        filters:
          - dimension: visitors.created_at
            operator: beforeDate
            values:
              - "2017-01-06"
              - "2017-01-06"
        order:
          - id: visitors.created_at
        timezone: "America/Los_Angeles"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, "date_filters_tables.sql").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_after_date() {
    let ctx = create_context();

    let query = indoc! {r#"
        dimensions:
          - visitors.created_at
        filters:
          - dimension: visitors.created_at
            operator: afterDate
            values:
              - "2017-01-06"
              - "2017-01-06"
        order:
          - id: visitors.created_at
        timezone: "America/Los_Angeles"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, "date_filters_tables.sql").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_before_or_on_date() {
    let ctx = create_context();

    let query = indoc! {r#"
        dimensions:
          - visitors.created_at
        filters:
          - dimension: visitors.created_at
            operator: beforeOrOnDate
            values:
              - "2017-01-06"
              - "2017-01-06"
        order:
          - id: visitors.created_at
        timezone: "America/Los_Angeles"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, "date_filters_tables.sql").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_after_or_on_date() {
    let ctx = create_context();

    let query = indoc! {r#"
        dimensions:
          - visitors.created_at
        filters:
          - dimension: visitors.created_at
            operator: afterOrOnDate
            values:
              - "2017-01-06"
              - "2017-01-06"
        order:
          - id: visitors.created_at
        timezone: "America/Los_Angeles"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, "date_filters_tables.sql").await {
        insta::assert_snapshot!(result);
    }
}
