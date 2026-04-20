use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const SEED: &str = "integration_multi_fact_tables.sql";

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_fact_pre_aggs.yaml");
    TestContext::new(schema).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_separate_pre_aggs_totals() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
          - returns.count
          - returns.total_refund
    "};

    let (_sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();

    assert_eq!(pre_aggrs.len(), 2, "Expected 2 pre-aggregation usages");
    let names: Vec<&str> = pre_aggrs.iter().map(|u| u.name().as_str()).collect();
    assert!(
        names.contains(&"orders_totals"),
        "Expected orders_totals, got {:?}",
        names
    );
    assert!(
        names.contains(&"returns_totals"),
        "Expected returns_totals, got {:?}",
        names
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_separate_pre_aggs_by_shared_dim() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
          - returns.count
          - returns.total_refund
        dimensions:
          - customers.city
        order:
          - id: customers.city
    "};

    let (_sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();

    assert_eq!(pre_aggrs.len(), 2, "Expected 2 pre-aggregation usages");
    let names: Vec<&str> = pre_aggrs.iter().map(|u| u.name().as_str()).collect();
    assert!(
        names.contains(&"orders_by_customer_city"),
        "Expected orders_by_customer_city, got {:?}",
        names
    );
    assert!(
        names.contains(&"returns_by_customer_city"),
        "Expected returns_by_customer_city, got {:?}",
        names
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_whole_query_single_rollup_match() {
    // Same multi-fact query as test_multi_fact_separate_pre_aggs_by_shared_dim,
    // but the schema offers a single rollup pre-aggregation that covers all
    // four measures and the shared dimension at once. Optimizer must take the
    // simple-match path and use a single pre-aggregation for the whole query.
    let schema = MockSchema::from_yaml_file("common/integration_multi_fact_combined_pre_agg.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
          - returns.count
          - returns.total_refund
        dimensions:
          - customers.city
        order:
          - id: customers.city
    "};

    let (_sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();

    assert_eq!(
        pre_aggrs.len(),
        1,
        "Expected whole query to match a single pre-aggregation; got {:?}",
        pre_aggrs.iter().map(|u| u.name().clone()).collect::<Vec<_>>()
    );
    assert_eq!(pre_aggrs[0].name(), "multi_fact_combined");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_partial_match_rolls_back() {
    // Only the orders pre-agg is enabled. Returns subquery cannot match →
    // optimizer must roll back and use no pre-aggregations at all.
    let schema = MockSchema::from_yaml_file("common/integration_multi_fact_pre_aggs.yaml")
        .only_pre_aggregations(&["orders_by_customer_city"]);
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        dimensions:
          - customers.city
        order:
          - id: customers.city
    "};

    let (_sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();

    assert_eq!(
        pre_aggrs.len(),
        0,
        "Expected rollback when one of multi-fact subqueries cannot match; got {:?}",
        pre_aggrs.iter().map(|u| u.name().clone()).collect::<Vec<_>>()
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
