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
        pre_aggrs
            .iter()
            .map(|u| u.name().clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(pre_aggrs[0].name(), "multi_fact_combined");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fact_plus_multiplied_separate_pre_aggs() {
    // Same query as the fact+multiplied baseline. Two pre-aggs:
    //   - orders.orders_by_status: regular orders subquery
    //   - customers.customers_by_order_status: multiplied customers subquery
    // The two subqueries can't share a single pre-aggregation because of join
    // path differences, so each CTE gets its own usage.
    let schema = MockSchema::from_yaml_file("common/integration_multi_fact_pre_aggs.yaml")
        .only_pre_aggregations(&["orders_by_status", "customers_by_order_status"]);
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {"
        measures:
          - orders.count
          - customers.count
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "};

    let (_sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();

    let names: Vec<&str> = pre_aggrs.iter().map(|u| u.name().as_str()).collect();

    assert_eq!(
        pre_aggrs.len(),
        2,
        "Expected 2 pre-aggregation usages; got {:?}",
        names
    );
    assert!(
        names.contains(&"orders_by_status"),
        "Expected orders_by_status for regular orders subquery; got {:?}",
        names
    );
    assert!(
        names.contains(&"customers_by_order_status"),
        "Expected customers_by_order_status for multiplied customers subquery; got {:?}",
        names
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_whole_query_single_rollup_match() {
    // Same multiplied query as the baseline, but the schema has a rollup
    // pre-aggregation covering both customer measures and the cross-cube
    // dimension. Whole query must be replaced with a single pre-aggregation.
    let schema = MockSchema::from_yaml_file("common/integration_multi_fact_pre_aggs.yaml")
        .only_pre_aggregations(&["customers_by_order_status"]);
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {"
        measures:
          - customers.count
          - customers.total_lifetime_value
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "};

    let (_sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();

    assert_eq!(
        pre_aggrs.len(),
        1,
        "Expected whole multiplied query to match a single pre-aggregation; got {:?}",
        pre_aggrs
            .iter()
            .map(|u| u.name().clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(pre_aggrs[0].name(), "customers_by_order_status");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_plus_multiplied_shared_pre_agg() {
    let schema = MockSchema::from_yaml_file("common/integration_multi_fact_pre_aggs.yaml")
        .only_pre_aggregations(&["customers_and_orders_combo", "returns_by_customer_city"]);
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
          - customers.count
        dimensions:
          - customers.city
        order:
          - id: customers.city
    "};

    let (_sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();

    let names: Vec<&str> = pre_aggrs.iter().map(|u| u.name().as_str()).collect();

    assert_eq!(
        pre_aggrs.len(),
        3,
        "Expected 3 pre-aggregation usages (orders + returns + multiplied customers); got {:?}",
        names
    );
    let combo_count = names
        .iter()
        .filter(|n| **n == "customers_and_orders_combo")
        .count();
    assert_eq!(
        combo_count, 2,
        "Expected customers_and_orders_combo to be used twice (regular orders + multiplied customers); got {:?}",
        names
    );
    assert!(
        names.contains(&"returns_by_customer_city"),
        "Expected returns_by_customer_city usage; got {:?}",
        names
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_regular_plus_two_multiplied_separate_pre_aggs() {
    // Three pre-aggs, each in its own cube:
    //   - addresses.addresses_by_street: regular subquery (own dim)
    //   - customers.customers_by_addr_street: multiplied customers subquery
    //   - orders.orders_by_addr_street: multiplied orders subquery
    //     (path orders → customers → addresses passes through one_to_many)
    let schema = MockSchema::from_yaml_file("common/integration_multi_fact_pre_aggs.yaml")
        .only_pre_aggregations(&[
            "addresses_by_street",
            "customers_by_addr_street",
            "orders_by_addr_street",
        ]);
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {"
        measures:
          - addresses.count
          - customers.count
          - orders.count
        dimensions:
          - addresses.street
        order:
          - id: addresses.street
    "};

    let (_sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();

    let names: Vec<&str> = pre_aggrs.iter().map(|u| u.name().as_str()).collect();

    assert_eq!(
        pre_aggrs.len(),
        3,
        "Expected 3 pre-aggregation usages; got {:?}",
        names
    );
    assert!(
        names.contains(&"addresses_by_street"),
        "Expected addresses_by_street; got {:?}",
        names
    );
    assert!(
        names.contains(&"customers_by_addr_street"),
        "Expected customers_by_addr_street; got {:?}",
        names
    );
    assert!(
        names.contains(&"orders_by_addr_street"),
        "Expected orders_by_addr_street; got {:?}",
        names
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// --- Filtered variants ---
// Each test below uses the same query shape as its non-filtered counterpart
// above but adds a filter on customers.name (not in projection) to verify
// that pre-agg matcher handles filter-only dimensions. Filter value 'Alice'
// narrows the seed to customer id=1 only — measure values change predictably
// so incorrect results are easy to spot.

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_separate_pre_aggs_by_shared_dim_filtered() {
    let schema = MockSchema::from_yaml_file("common/integration_multi_fact_pre_aggs.yaml")
        .only_pre_aggregations(&[
            "orders_by_customer_city_with_name",
            "returns_by_customer_city_with_name",
        ]);
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
          - returns.count
          - returns.total_refund
        dimensions:
          - customers.city
        filters:
          - dimension: customers.name
            operator: equals
            values:
              - Alice
        order:
          - id: customers.city
    "};

    let (_sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();
    let names: Vec<&str> = pre_aggrs.iter().map(|u| u.name().as_str()).collect();
    assert_eq!(pre_aggrs.len(), 2, "Expected 2 usages; got {:?}", names);
    assert!(names.contains(&"orders_by_customer_city_with_name"));
    assert!(names.contains(&"returns_by_customer_city_with_name"));

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_whole_query_single_rollup_match_filtered() {
    let schema = MockSchema::from_yaml_file("common/integration_multi_fact_combined_pre_agg.yaml")
        .only_pre_aggregations(&["multi_fact_combined_with_name"]);
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
          - returns.count
          - returns.total_refund
        dimensions:
          - customers.city
        filters:
          - dimension: customers.name
            operator: equals
            values:
              - Alice
        order:
          - id: customers.city
    "};

    let (_sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();
    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "multi_fact_combined_with_name");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_plus_multiplied_shared_pre_agg_filtered() {
    let schema = MockSchema::from_yaml_file("common/integration_multi_fact_pre_aggs.yaml")
        .only_pre_aggregations(&[
            "customers_and_orders_combo_with_name",
            "returns_by_customer_city_with_name",
        ]);
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
          - customers.count
        dimensions:
          - customers.city
        filters:
          - dimension: customers.name
            operator: equals
            values:
              - Alice
        order:
          - id: customers.city
    "};

    let (_sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();
    let names: Vec<&str> = pre_aggrs.iter().map(|u| u.name().as_str()).collect();
    assert_eq!(pre_aggrs.len(), 3, "Expected 3 usages; got {:?}", names);
    let combo_count = names
        .iter()
        .filter(|n| **n == "customers_and_orders_combo_with_name")
        .count();
    assert_eq!(combo_count, 2);
    assert!(names.contains(&"returns_by_customer_city_with_name"));

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_whole_query_single_rollup_match_filtered() {
    let schema = MockSchema::from_yaml_file("common/integration_multi_fact_pre_aggs.yaml")
        .only_pre_aggregations(&["customers_by_order_status_with_name"]);
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {"
        measures:
          - customers.count
          - customers.total_lifetime_value
        dimensions:
          - orders.status
        filters:
          - dimension: customers.name
            operator: equals
            values:
              - Alice
        order:
          - id: orders.status
    "};

    let (_sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();
    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "customers_by_order_status_with_name");

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
        pre_aggrs
            .iter()
            .map(|u| u.name().clone())
            .collect::<Vec<_>>()
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
