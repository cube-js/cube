//! View dimensions shared through `join_paths`: each fact reads the shared
//! dimension through its own path, aliased or not.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    TestContext::new(MockSchema::from_yaml_file("common/shared_join_paths.yaml")).unwrap()
}

const SEED: &str = "shared_join_paths_tables.sql";

/// cube-js/cube#12424: two facts joining one dimension under the same alias.
#[tokio::test(flavor = "multi_thread")]
async fn test_shared_alias_across_facts() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - activity.order_count
          - activity.refund_count
        time_dimensions:
          - dimension: activity.date
            granularity: day
        order:
          - id: activity.date
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert!(sql.contains("orders__event_day"), "{sql}");
    assert!(sql.contains("refunds__event_day"), "{sql}");
    assert!(
        !sql.contains("orders__event_day\" ON \"refunds\""),
        "each fact reads its own path: {sql}"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// cube-js/cube#12618: an aliased path on one fact, a plain one on the other.
#[tokio::test(flavor = "multi_thread")]
async fn test_aliased_and_plain_paths() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - complaints_and_attendance.complaints_count
          - complaints_and_attendance.attendances_count
        dimensions:
          - complaints_and_attendance.person_name
        order:
          - id: complaints_and_attendance.person_name
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert!(sql.contains("complaints__complainant"), "{sql}");
    assert!(!sql.contains("complaints__handler"), "{sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shared_path_next_to_a_common_dimension() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - complaints_and_attendance.complaints_count
          - complaints_and_attendance.attendances_count
        dimensions:
          - complaints_and_attendance.title
          - complaints_and_attendance.person_name
        order:
          - id: complaints_and_attendance.title
          - id: complaints_and_attendance.person_name
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_single_fact_binds_in_place() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - activity.refund_count
        time_dimensions:
          - dimension: activity.date
            granularity: day
        order:
          - id: activity.date
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert!(sql.contains("refunds__event_day"), "{sql}");
    assert!(!sql.contains("orders__event_day"), "{sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dimensions_only_read_the_first_path() {
    let ctx = create_context();
    let query = indoc! {r#"
        time_dimensions:
          - dimension: activity.date
            granularity: day
        order:
          - id: activity.date
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert!(sql.contains("orders__event_day"), "{sql}");
    assert!(!sql.contains("refunds__event_day"), "{sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_filter_on_a_shared_dimension() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - activity.order_count
          - activity.refund_count
        time_dimensions:
          - dimension: activity.date
            granularity: day
            dateRange:
              - "2024-01-02"
              - "2024-01-03"
        order:
          - id: activity.date
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fact_joined_to_a_path_start() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - activity.item_count
          - activity.refund_count
        time_dimensions:
          - dimension: activity.date
            granularity: day
        order:
          - id: activity.date
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert!(sql.contains("orders__event_day"), "{sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_measure_filter_and_limit_apply_over_the_merge() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - activity.order_count
          - activity.refund_count
        time_dimensions:
          - dimension: activity.date
            granularity: day
        filters:
          - member: activity.refund_count
            operator: gt
            values:
              - "0"
        order:
          - id: activity.date
        limit: 1
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_time_shift_by_a_shared_dimension() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - activity.order_count_prev_day
          - activity.refund_count
        time_dimensions:
          - dimension: activity.date
            granularity: day
            dateRange:
              - "2024-01-01"
              - "2024-01-03"
        order:
          - id: activity.date
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[test]
fn test_fact_without_a_path_is_an_error() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - activity.survey_count
          - activity.order_count
        time_dimensions:
          - dimension: activity.date
            granularity: day
    "#};

    let error = ctx.build_sql(query).unwrap_err();
    assert!(
        error
            .message
            .contains("Add a path starting at 'surveys' to join_paths"),
        "{}",
        error.message
    );
}
