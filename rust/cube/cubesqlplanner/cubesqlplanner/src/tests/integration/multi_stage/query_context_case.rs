//! A QUERY_CONTEXT case is resolved against the query node computing the
//! measure: the query itself, or a multi-stage stage at its own grain.

use crate::cube_bridge::query_context::{QueryContextData, QueryContextTimeDimension};
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_stage.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_stage_tables.sql";

fn aggregations(sql: &str) -> Vec<&'static str> {
    let sql = sql.to_lowercase();
    ["sum(", "avg(", "max("]
        .into_iter()
        .filter(|agg| sql.contains(agg))
        .collect()
}

fn query_context(ctx: &TestContext, query: &str) -> QueryContextData {
    let properties = ctx.create_query_properties(query).unwrap();
    QueryContextData::from_node(&properties, &properties)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_filtered_member_picks_first_branch() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_by_query_context
        dimensions:
          - orders.category
        filters:
          - member: orders.status
            operator: equals
            values:
              - completed
        order:
          - id: orders.category
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(aggregations(&sql), vec!["sum("]);

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_grouped_member_picks_second_branch() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_by_query_context
        dimensions:
          - orders.category
        order:
          - id: orders.category
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(aggregations(&sql), vec!["avg("]);

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_negated_measure_predicate_falls_to_else() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_by_query_context
          - orders.count
        dimensions:
          - orders.category
        order:
          - id: orders.category
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(aggregations(&sql), vec!["max("]);

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_time_dimension_without_granularity_is_not_grouped() {
    let ctx = create_context();

    let grouped = indoc! {r#"
        measures:
          - orders.amount_by_query_context
        dimensions:
          - orders.category
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
    "#};
    assert_eq!(aggregations(&ctx.build_sql(grouped).unwrap()), vec!["avg("]);

    let date_range_only = indoc! {r#"
        measures:
          - orders.amount_by_query_context
        time_dimensions:
          - dimension: orders.created_at
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
    "#};
    assert_eq!(
        aggregations(&ctx.build_sql(date_range_only).unwrap()),
        vec!["max("]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_join_path_and_granularity_suffix_name_the_model_member() {
    let ctx = create_context();

    let joined = indoc! {r#"
        measures:
          - orders.amount_by_joined_grouping
        dimensions:
          - orders.customers.city
    "#};
    assert_eq!(aggregations(&ctx.build_sql(joined).unwrap()), vec!["avg("]);

    let suffixed = indoc! {r#"
        measures:
          - orders.amount_by_joined_grouping
        dimensions:
          - orders.created_at.month
    "#};
    assert_eq!(
        aggregations(&ctx.build_sql(suffixed).unwrap()),
        vec!["sum("]
    );

    let plain = indoc! {r#"
        measures:
          - orders.amount_by_joined_grouping
        dimensions:
          - orders.category
    "#};
    assert_eq!(aggregations(&ctx.build_sql(plain).unwrap()), vec!["max("]);
}

// A measure filter is applied by the query, never by a stage, so it is read from the query.
#[tokio::test(flavor = "multi_thread")]
async fn test_measure_filter_of_the_query_is_seen_by_the_stage() {
    let ctx = create_context();

    let filtered = indoc! {r#"
        measures:
          - orders.amount_by_measure_filter
        filters:
          - member: orders.count
            operator: gt
            values:
              - "0"
    "#};
    assert_eq!(
        aggregations(&ctx.build_sql(filtered).unwrap()),
        vec!["sum("]
    );

    let plain = indoc! {r#"
        measures:
          - orders.amount_by_measure_filter
    "#};
    assert_eq!(aggregations(&ctx.build_sql(plain).unwrap()), vec!["max("]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_segment_picks_the_branch() {
    let ctx = create_context();

    let segmented = indoc! {r#"
        measures:
          - orders.amount_by_segment_context
        segments:
          - orders.completed_orders
    "#};
    assert_eq!(
        aggregations(&ctx.build_sql(segmented).unwrap()),
        vec!["sum("]
    );

    let plain = indoc! {r#"
        measures:
          - orders.amount_by_segment_context
    "#};
    assert_eq!(aggregations(&ctx.build_sql(plain).unwrap()), vec!["max("]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_members_name_the_cube_members() {
    let ctx = create_context();

    let grouped = indoc! {r#"
        measures:
          - orders_context_view.amount_by_query_context
        dimensions:
          - orders_context_view.category
    "#};
    assert_eq!(aggregations(&ctx.build_sql(grouped).unwrap()), vec!["avg("]);

    let filtered = indoc! {r#"
        measures:
          - orders_context_view.amount_by_query_context
        dimensions:
          - orders_context_view.category
        filters:
          - member: orders_context_view.status
            operator: equals
            values:
              - completed
    "#};
    assert_eq!(
        aggregations(&ctx.build_sql(filtered).unwrap()),
        vec!["sum("]
    );

    let with_count = indoc! {r#"
        measures:
          - orders_context_view.amount_by_query_context
          - orders_context_view.count
        dimensions:
          - orders_context_view.category
    "#};
    assert_eq!(
        aggregations(&ctx.build_sql(with_count).unwrap()),
        vec!["max("]
    );

    let joined = indoc! {r#"
        measures:
          - orders_context_view.amount_by_joined_grouping
        dimensions:
          - orders_context_view.customers_city
    "#};
    assert_eq!(aggregations(&ctx.build_sql(joined).unwrap()), vec!["avg("]);
}

// Inside the `reduce_by: category` stage `has_dimension(category)` does not hold;
// the key grid is read from the member at the query grain, so both branches appear.
#[tokio::test(flavor = "multi_thread")]
async fn test_stage_reducing_a_dimension_resolves_at_its_own_grain() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_context_without_category
        dimensions:
          - orders.category
        order:
          - id: orders.category
    "#};
    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(aggregations(&sql), vec!["sum(", "avg(", "max("]);

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// `filter: exclude` drops the status filter inside the stage, so the branch
// reading `has_filter(status)` is not the one the stage computes.
#[tokio::test(flavor = "multi_thread")]
async fn test_stage_dropping_a_filter_resolves_at_its_own_grain() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_context_without_status_filter
        dimensions:
          - orders.category
        filters:
          - member: orders.status
            operator: equals
            values:
              - completed
        order:
          - id: orders.category
    "#};
    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(aggregations(&sql), vec!["sum(", "avg("]);

    let direct = indoc! {r#"
        measures:
          - orders.amount_by_query_context
        dimensions:
          - orders.category
        filters:
          - member: orders.status
            operator: equals
            values:
              - completed
    "#};
    assert_eq!(aggregations(&ctx.build_sql(direct).unwrap()), vec!["sum("]);

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[test]
fn test_query_context_names_the_model_members() {
    let ctx = create_context();

    let query_context = query_context(
        &ctx,
        indoc! {r#"
            measures:
              - orders_context_view.count
            dimensions:
              - orders.customers.city
              - orders.created_at.month
            time_dimensions:
              - dimension: orders_context_view.created_at
                granularity: day
                dateRange:
                  - "2024-01-01"
                  - "2024-03-31"
            filters:
              - member: orders_context_view.status
                operator: equals
                values:
                  - completed
            segments:
              - orders.completed_orders
        "#},
    );

    assert_eq!(query_context.measures, vec!["orders.count"]);
    assert_eq!(query_context.dimensions, vec!["customers.city"]);
    assert_eq!(
        query_context.time_dimensions,
        vec![
            QueryContextTimeDimension {
                dimension: "orders.created_at".to_string(),
                granularity: Some("month".to_string()),
                date_range: None,
            },
            QueryContextTimeDimension {
                dimension: "orders.created_at".to_string(),
                granularity: Some("day".to_string()),
                date_range: Some(vec!["2024-01-01".to_string(), "2024-03-31".to_string()]),
            },
        ]
    );
    let filters = query_context
        .filters
        .iter()
        .map(|f| (f.member.as_str(), f.operator.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(
        filters,
        vec![
            ("orders.created_at", "inDateRange"),
            ("orders.status", "equals")
        ]
    );
    assert_eq!(query_context.segments, vec!["orders.completed_orders"]);
}

#[test]
fn test_query_context_keeps_only_and_leaves() {
    let ctx = create_context();

    let query_context = query_context(
        &ctx,
        indoc! {r#"
            measures:
              - orders.count
            dimensions:
              - orders.status
            filters:
              - member: orders.category
                operator: equals
                values:
                  - books
              - and:
                  - member: orders.status
                    operator: notEquals
                    values:
                      - cancelled
                  - or:
                      - member: orders.category
                        operator: equals
                        values:
                          - toys
                      - member: orders.status
                        operator: equals
                        values:
                          - shipped
        "#},
    );

    let leaves = query_context
        .filters
        .iter()
        .map(|f| (f.member.as_str(), f.operator.as_str(), f.values.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        leaves,
        vec![
            (
                "orders.category",
                "equals",
                Some(vec![Some("books".to_string())])
            ),
            (
                "orders.status",
                "notEquals",
                Some(vec![Some("cancelled".to_string())])
            ),
        ]
    );
}
