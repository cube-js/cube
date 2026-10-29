use crate::cube_bridge::query_context::QueryContextData;
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

#[test]
fn test_query_context_from_options_flattens_filters() {
    let ctx = create_context();

    let options = ctx.create_query_options_from_yaml(indoc! {r#"
        measures:
          - orders.count
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        filters:
          - or:
              - member: orders.category
                operator: equals
                values:
                  - books
              - and:
                  - member: orders.status
                    operator: notEquals
                    values:
                      - cancelled
                  - member: orders.total_amount
                    operator: gt
                    values:
                      - "100"
    "#});

    let query_context = QueryContextData::from_query_options(options.as_ref()).unwrap();

    assert_eq!(query_context.measures, vec!["orders.count"]);
    assert_eq!(query_context.dimensions, vec!["orders.status"]);
    assert_eq!(query_context.time_dimensions.len(), 1);
    assert_eq!(
        query_context.time_dimensions[0].granularity.as_deref(),
        Some("month")
    );
    assert_eq!(
        query_context.time_dimensions[0].date_range,
        Some(vec!["2024-01-01".to_string(), "2024-03-31".to_string()])
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
            (
                "orders.total_amount",
                "gt",
                Some(vec![Some("100".to_string())])
            ),
        ]
    );
}
