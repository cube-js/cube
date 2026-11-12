//! How measures are split into legs: each case is checked against a
//! hand-written query over the seed, and its rows are snapshotted.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::{table_rows, TestContext};
use indoc::indoc;

const CHAIN_SCHEMA: &str = "common/integration_measure_legs.yaml";
const CHAIN_SEED: &str = "integration_measure_legs_tables.sql";
const INTO_OWNER_SCHEMA: &str = "common/integration_measure_legs_into_owner.yaml";
const INTO_OWNER_SEED: &str = "integration_measure_legs_into_owner_tables.sql";

async fn assert_legs(schema: &str, seed: &str, name: &str, query: &str, oracle_sql: &str) {
    let ctx = TestContext::new(MockSchema::from_yaml_file(schema)).unwrap();
    let sql = ctx.build_sql(query).unwrap();
    let Some(result) = ctx.try_execute_pg(query, seed).await else {
        return;
    };
    let oracle = ctx.try_execute_raw_pg(oracle_sql, seed).await.unwrap();
    assert_eq!(
        table_rows(&result),
        table_rows(&oracle),
        "cube:\n{result}\noracle:\n{oracle}\nsql:\n{sql}"
    );
    insta::assert_snapshot!(name, result);
}

async fn assert_chain(name: &str, query: &str, oracle_sql: &str) {
    assert_legs(CHAIN_SCHEMA, CHAIN_SEED, name, query, oracle_sql).await
}

async fn assert_into_owner(name: &str, query: &str, oracle_sql: &str) {
    assert_legs(INTO_OWNER_SCHEMA, INTO_OWNER_SEED, name, query, oracle_sql).await
}

// Rows of each fact reached from customers, per city.
const BY_CITY: &str = indoc! {"
    cities AS (SELECT DISTINCT city FROM customers),
    o AS (SELECT c.city, o.* FROM orders o JOIN customers c ON c.id = o.customer_id),
    r AS (SELECT o.city, r.* FROM returns r JOIN o ON o.id = r.order_id),
    i AS (SELECT o.city, i.* FROM items i JOIN o ON o.id = i.order_id)
"};

fn by_city(select: &str) -> String {
    format!("WITH {BY_CITY} {select}")
}

// ---- Measures of several facts, no formula ----

#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_three_facts_by_city() {
    assert_chain(
        "three_facts_by_city",
        indoc! {"
            measures:
              - orders.total_amount
              - returns.total_refund
              - items.total_qty
            dimensions:
              - customers.city
            order:
              - id: customers.city
        "},
        &by_city(indoc! {"
            SELECT city,
                   (SELECT sum(amount) FROM o WHERE o.city = cities.city),
                   (SELECT sum(refund) FROM r WHERE r.city = cities.city),
                   (SELECT sum(qty) FROM i WHERE i.city = cities.city)
            FROM cities ORDER BY 1
        "}),
    )
    .await;
}

// A dimension of a fact roots every leg at that fact: the return of no order
// is not counted.
#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_facts_by_status() {
    assert_chain(
        "facts_by_status",
        indoc! {"
            measures:
              - orders.count
              - returns.count
              - items.total_qty
            dimensions:
              - orders.status
            order:
              - id: orders.status
        "},
        indoc! {"
            SELECT o.status,
                   count(*),
                   (SELECT count(*) FROM returns r JOIN orders o2 ON o2.id = r.order_id
                     WHERE o2.status = o.status),
                   (SELECT sum(qty) FROM items i JOIN orders o2 ON o2.id = i.order_id
                     WHERE o2.status = o.status)
            FROM orders o GROUP BY 1 ORDER BY 1
        "},
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_facts_by_region() {
    assert_chain(
        "facts_by_region",
        indoc! {"
            measures:
              - customers.count
              - orders.total_amount
              - returns.total_refund
            dimensions:
              - regions.name
            order:
              - id: regions.name
        "},
        indoc! {"
            SELECT g.name,
                   (SELECT count(*) FROM customers c WHERE c.region_id = g.id),
                   (SELECT sum(o.amount) FROM orders o JOIN customers c ON c.id = o.customer_id
                     WHERE c.region_id = g.id),
                   (SELECT sum(r.refund) FROM returns r JOIN orders o ON o.id = r.order_id
                     JOIN customers c ON c.id = o.customer_id WHERE c.region_id = g.id)
            FROM regions g ORDER BY 1
        "},
    )
    .await;
}

// ---- One side grouped by a dimension of the many side ----

// Orders repeat once per return; every aggregate must count an order once.
// Orders without returns fall into the NULL reason; the orphan return is not
// reached from orders.
#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_aggregates_of_a_multiplied_cube() {
    assert_chain(
        "aggregates_of_a_multiplied_cube",
        indoc! {"
            measures:
              - orders.count
              - orders.count_ids
              - orders.total_amount
              - orders.min_amount
              - orders.max_amount
              - orders.unique_customers
              - orders.avg_amount
            dimensions:
              - returns.reason
            order:
              - id: returns.reason
        "},
        indoc! {"
            WITH j AS (SELECT r.reason, o.* FROM orders o LEFT JOIN returns r ON r.order_id = o.id),
            d AS (SELECT DISTINCT reason, id, customer_id, amount FROM j)
            SELECT reason, count(*), count(id), sum(amount), min(amount), max(amount),
                   count(DISTINCT customer_id), sum(amount) / NULLIF(count(*), 0)
            FROM d GROUP BY 1 ORDER BY 1
        "},
    )
    .await;
}

// Customers repeat once per order and once per return.
#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_aggregates_of_a_cube_multiplied_twice() {
    assert_chain(
        "aggregates_of_a_cube_multiplied_twice",
        indoc! {"
            measures:
              - customers.count
              - customers.max_lifetime
              - customers.total_lifetime
            dimensions:
              - returns.reason
            order:
              - id: returns.reason
        "},
        indoc! {"
            WITH j AS (SELECT DISTINCT r.reason, c.id, c.lifetime_value FROM customers c
                LEFT JOIN orders o ON o.customer_id = c.id
                LEFT JOIN returns r ON r.order_id = o.id)
            SELECT reason, count(*), max(lifetime_value), sum(lifetime_value)
            FROM j GROUP BY 1 ORDER BY 1
        "},
    )
    .await;
}

// A multiplied leg and a leg of the many side over one join.
#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_multiplied_beside_its_multiplier() {
    assert_chain(
        "multiplied_beside_its_multiplier",
        indoc! {"
            measures:
              - orders.total_amount
              - returns.total_refund
            dimensions:
              - returns.reason
            order:
              - id: returns.reason
        "},
        indoc! {"
            WITH j AS (SELECT r.reason, o.id, o.amount FROM orders o
                LEFT JOIN returns r ON r.order_id = o.id),
            reasons AS (SELECT reason FROM j UNION SELECT reason FROM returns)
            SELECT reason,
                   (SELECT sum(amount) FROM (SELECT DISTINCT id, amount FROM j
                     WHERE j.reason IS NOT DISTINCT FROM reasons.reason) d),
                   (SELECT sum(refund) FROM returns r WHERE r.reason = reasons.reason)
            FROM reasons ORDER BY 1
        "},
    )
    .await;
}

// The max of customers survives the orders join, the sum of orders does not
// survive the returns join: three join trees, two of them merged.
#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_partially_merged_nested_trees() {
    assert_chain(
        "partially_merged_nested_trees",
        indoc! {"
            measures:
              - customers.max_lifetime
              - orders.total_amount
              - returns.total_refund
            dimensions:
              - customers.city
            order:
              - id: customers.city
        "},
        &by_city(indoc! {"
            SELECT city,
                   (SELECT max(lifetime_value) FROM customers c WHERE c.city = cities.city),
                   (SELECT sum(amount) FROM o WHERE o.city = cities.city),
                   (SELECT sum(refund) FROM r WHERE r.city = cities.city)
            FROM cities ORDER BY 1
        "}),
    )
    .await;
}

// ---- Calculated measures ----

#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_calculated_across_facts() {
    assert_chain(
        "calculated_across_facts",
        indoc! {"
            measures:
              - orders.amount_minus_refund
            dimensions:
              - customers.city
            order:
              - id: customers.city
        "},
        &by_city(indoc! {"
            SELECT city,
                   (SELECT sum(amount) FROM o WHERE o.city = cities.city)
                   - (SELECT sum(refund) FROM r WHERE r.city = cities.city)
            FROM cities ORDER BY 1
        "}),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_calculated_across_facts_with_its_leaves() {
    assert_chain(
        "calculated_across_facts_with_its_leaves",
        indoc! {"
            measures:
              - orders.amount_minus_refund
              - orders.total_amount
              - returns.total_refund
            dimensions:
              - customers.city
            order:
              - id: customers.city
        "},
        &by_city(indoc! {"
            SELECT city,
                   (SELECT sum(amount) FROM o WHERE o.city = cities.city)
                   - (SELECT sum(refund) FROM r WHERE r.city = cities.city),
                   (SELECT sum(amount) FROM o WHERE o.city = cities.city),
                   (SELECT sum(refund) FROM r WHERE r.city = cities.city)
            FROM cities ORDER BY 1
        "}),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_calculated_within_one_cube_beside_one_across_facts() {
    assert_chain(
        "calculated_within_one_cube_beside_one_across_facts",
        indoc! {"
            measures:
              - orders.avg_amount
              - orders.amount_minus_refund
            dimensions:
              - customers.city
            order:
              - id: customers.city
        "},
        &by_city(indoc! {"
            SELECT city,
                   (SELECT sum(amount) / NULLIF(count(*), 0) FROM o WHERE o.city = cities.city),
                   (SELECT sum(amount) FROM o WHERE o.city = cities.city)
                   - (SELECT sum(refund) FROM r WHERE r.city = cities.city)
            FROM cities ORDER BY 1
        "}),
    )
    .await;
}

// A calculated measure over a calculated measure of its own cube that reads
// another cube.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "leaf collection stops at a measure reading only its own cube"]
async fn test_measure_legs_calculated_over_a_calculated_measure_across_facts() {
    assert_chain(
        "calculated_over_a_calculated_measure_across_facts",
        indoc! {"
            measures:
              - orders.amount_minus_refund_plus_one
            dimensions:
              - customers.city
            order:
              - id: customers.city
        "},
        &by_city(indoc! {"
            SELECT city,
                   (SELECT sum(amount) FROM o WHERE o.city = cities.city)
                   - (SELECT sum(refund) FROM r WHERE r.city = cities.city) + 1
            FROM cities ORDER BY 1
        "}),
    )
    .await;
}

// The same, with a plain measure of its own cube beside the calculated one.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "leaf collection stops at a measure reading only its own cube"]
async fn test_measure_legs_calculated_over_a_calculated_measure_and_a_sibling() {
    assert_chain(
        "calculated_over_a_calculated_measure_and_a_sibling",
        indoc! {"
            measures:
              - orders.amount_minus_refund_plus_fee
            dimensions:
              - customers.city
            order:
              - id: customers.city
        "},
        &by_city(indoc! {"
            SELECT city,
                   (SELECT sum(amount) FROM o WHERE o.city = cities.city)
                   - (SELECT sum(refund) FROM r WHERE r.city = cities.city)
                   + (SELECT sum(fee) FROM o WHERE o.city = cities.city)
            FROM cities ORDER BY 1
        "}),
    )
    .await;
}

// The measure of a cube up the chain: orders and returns are reached through
// customers.
#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_calculated_two_levels_up() {
    assert_chain(
        "calculated_two_levels_up",
        indoc! {"
            measures:
              - customers.net_per_customer
            dimensions:
              - regions.name
            order:
              - id: regions.name
        "},
        indoc! {"
            SELECT g.name,
                   ((SELECT sum(o.amount) FROM orders o JOIN customers c ON c.id = o.customer_id
                      WHERE c.region_id = g.id)
                    - (SELECT sum(r.refund) FROM returns r JOIN orders o ON o.id = r.order_id
                      JOIN customers c ON c.id = o.customer_id WHERE c.region_id = g.id))
                   / NULLIF((SELECT count(*) FROM customers c WHERE c.region_id = g.id), 0)
            FROM regions g ORDER BY 1
        "},
    )
    .await;
}

// A measure of returns reading orders: orders repeat once per return.
#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_calculated_reading_the_one_side() {
    assert_chain(
        "calculated_reading_the_one_side",
        indoc! {"
            measures:
              - returns.orders_per_return
            dimensions:
              - customers.city
            order:
              - id: customers.city
        "},
        &by_city(indoc! {"
            SELECT city,
                   (SELECT count(*) FROM o WHERE o.city = cities.city)
                   / NULLIF((SELECT count(*) FROM r WHERE r.city = cities.city), 0)
            FROM cities ORDER BY 1
        "}),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_calculated_over_a_sibling_fact() {
    assert_chain(
        "calculated_over_a_sibling_fact",
        indoc! {"
            measures:
              - orders.qty_per_order
            dimensions:
              - customers.city
            order:
              - id: customers.city
        "},
        &by_city(indoc! {"
            SELECT city,
                   (SELECT sum(qty) FROM i WHERE i.city = cities.city)
                   / NULLIF((SELECT count(*) FROM o WHERE o.city = cities.city), 0)
            FROM cities ORDER BY 1
        "}),
    )
    .await;
}

// A proxy of another cube's measure keeps its own cube in the join: the
// return of no order is not counted.
#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_proxy_grand_total() {
    assert_chain(
        "proxy_grand_total",
        indoc! {"
            measures:
              - orders.returns_proxy
        "},
        "SELECT count(*) FROM returns r JOIN orders o ON o.id = r.order_id",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_proxy_by_city() {
    assert_chain(
        "proxy_by_city",
        indoc! {"
            measures:
              - orders.returns_proxy
            dimensions:
              - customers.city
            order:
              - id: customers.city
        "},
        &by_city(indoc! {"
            SELECT city, (SELECT count(*) FROM r WHERE r.city = cities.city)
            FROM cities ORDER BY 1
        "}),
    )
    .await;
}

// Each leaf keeps the cube of its measure: returns are counted through
// orders, as the proxy counts them.
#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_calculated_across_facts_grand_total() {
    assert_chain(
        "calculated_across_facts_grand_total",
        indoc! {"
            measures:
              - orders.count_minus_returns
        "},
        indoc! {"
            SELECT (SELECT count(*) FROM orders)
                   - (SELECT count(*) FROM returns r JOIN orders o ON o.id = r.order_id)
        "},
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_calculated_across_facts_by_status() {
    assert_chain(
        "calculated_across_facts_by_status",
        indoc! {"
            measures:
              - orders.count_minus_returns
            dimensions:
              - orders.status
            order:
              - id: orders.status
        "},
        indoc! {"
            SELECT o.status,
                   count(*) - (SELECT count(*) FROM returns r JOIN orders o2 ON o2.id = r.order_id
                     WHERE o2.status = o.status)
            FROM orders o GROUP BY 1 ORDER BY 1
        "},
    )
    .await;
}

const AMOUNT_MINUS_REFUND_BY_MONTH: &str = indoc! {"
    months AS (SELECT DISTINCT date_trunc('month', created_at) AS month FROM orders),
    m AS (SELECT month,
                 (SELECT sum(amount) FROM orders o WHERE date_trunc('month', o.created_at) = months.month)
                 - (SELECT sum(r.refund) FROM returns r JOIN orders o ON o.id = r.order_id
                    WHERE date_trunc('month', o.created_at) = months.month) AS v
          FROM months)
"};

#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_calculated_across_facts_by_month() {
    assert_chain(
        "calculated_across_facts_by_month",
        indoc! {"
            measures:
              - orders.amount_minus_refund
            time_dimensions:
              - dimension: orders.created_at
                granularity: month
            order:
              - id: orders.created_at
        "},
        &format!("WITH {AMOUNT_MINUS_REFUND_BY_MONTH} SELECT month, v FROM m ORDER BY 1"),
    )
    .await;
}

// ---- Filters, segments, order and limit ----

// The filter on returns restricts the orders leg too: only orders with a
// defective return are summed.
#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_calculated_filtered_by_the_many_side() {
    assert_chain(
        "calculated_filtered_by_the_many_side",
        indoc! {"
            measures:
              - orders.amount_minus_refund
            dimensions:
              - customers.city
            filters:
              - member: returns.reason
                operator: equals
                values:
                  - defective
            order:
              - id: customers.city
        "},
        &by_city(indoc! {"
            SELECT city,
                   (SELECT sum(amount) FROM o WHERE o.city = cities.city
                     AND EXISTS (SELECT 1 FROM returns r WHERE r.order_id = o.id AND r.reason = 'defective'))
                   - (SELECT sum(refund) FROM r WHERE r.city = cities.city AND r.reason = 'defective')
            FROM cities
            WHERE EXISTS (SELECT 1 FROM r WHERE r.city = cities.city AND r.reason = 'defective')
            ORDER BY 1
        "}),
    )
    .await;
}

// The segment on orders restricts the returns leg too.
#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_calculated_with_a_segment_of_the_one_side() {
    assert_chain(
        "calculated_with_a_segment_of_the_one_side",
        indoc! {"
            measures:
              - orders.amount_minus_refund
            dimensions:
              - customers.city
            segments:
              - orders.completed
            order:
              - id: customers.city
        "},
        &by_city(indoc! {"
            SELECT city,
                   (SELECT sum(amount) FROM o WHERE o.city = cities.city AND o.status = 'completed')
                   - (SELECT sum(r.refund) FROM r JOIN orders o2 ON o2.id = r.order_id
                     WHERE r.city = cities.city AND o2.status = 'completed')
            FROM cities
            WHERE EXISTS (SELECT 1 FROM o WHERE o.city = cities.city AND o.status = 'completed')
            ORDER BY 1
        "}),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_calculated_measure_filter() {
    assert_chain(
        "calculated_measure_filter",
        indoc! {"
            measures:
              - orders.amount_minus_refund
            dimensions:
              - customers.city
            filters:
              - member: orders.amount_minus_refund
                operator: gt
                values:
                  - \"300\"
            order:
              - id: customers.city
        "},
        &by_city(indoc! {"
            SELECT * FROM (
              SELECT city,
                     (SELECT sum(amount) FROM o WHERE o.city = cities.city)
                     - (SELECT sum(refund) FROM r WHERE r.city = cities.city) AS v
              FROM cities) t
            WHERE v > 300 ORDER BY 1
        "}),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_calculated_ordered_with_limit_and_offset() {
    assert_chain(
        "calculated_ordered_with_limit_and_offset",
        indoc! {"
            measures:
              - orders.amount_minus_refund
            dimensions:
              - customers.city
            order:
              - id: orders.amount_minus_refund
            row_limit: \"1\"
            offset: \"1\"
        "},
        &by_city(indoc! {"
            SELECT city,
                   (SELECT sum(amount) FROM o WHERE o.city = cities.city)
                   - (SELECT sum(refund) FROM r WHERE r.city = cities.city)
            FROM cities ORDER BY 2 LIMIT 1 OFFSET 1
        "}),
    )
    .await;
}

// ---- Views ----

// A view measure has no cube of its own: its leaves are joined along the
// paths of the view.
#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_calculated_view_measure_across_facts() {
    assert_chain(
        "calculated_view_measure_across_facts",
        indoc! {"
            measures:
              - sales.net_amount
              - sales.count
            dimensions:
              - sales.city
            order:
              - id: sales.city
        "},
        &by_city(indoc! {"
            SELECT city,
                   (SELECT sum(amount) FROM o WHERE o.city = cities.city)
                   - (SELECT sum(refund) FROM r WHERE r.city = cities.city),
                   (SELECT count(*) FROM o WHERE o.city = cities.city)
            FROM cities ORDER BY 1
        "}),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_view_measures_by_a_dimension_of_the_many_side() {
    assert_chain(
        "view_measures_by_a_dimension_of_the_many_side",
        indoc! {"
            measures:
              - sales.count
              - sales.total_amount
              - sales.total_refund
            dimensions:
              - sales.reason
            order:
              - id: sales.reason
        "},
        indoc! {"
            WITH j AS (SELECT r.reason, r.refund, o.id, o.amount FROM customers c
                LEFT JOIN orders o ON o.customer_id = c.id
                LEFT JOIN returns r ON r.order_id = o.id)
            SELECT reason,
                   count(DISTINCT id),
                   (SELECT sum(amount) FROM (SELECT DISTINCT id, amount FROM j j2
                     WHERE j2.reason IS NOT DISTINCT FROM j.reason) d),
                   sum(refund)
            FROM j GROUP BY 1 ORDER BY 1
        "},
    )
    .await;
}

// ---- Ungrouped and multi-stage ----

#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_ungrouped_over_two_facts() {
    assert_chain(
        "ungrouped_over_two_facts",
        indoc! {"
            measures:
              - orders.total_amount
              - returns.total_refund
            dimensions:
              - customers.city
              - orders.id
              - returns.id
            ungrouped: true
            order:
              - id: customers.city
              - id: orders.id
              - id: returns.id
        "},
        indoc! {"
            SELECT c.city, o.id, r.id, o.amount, r.refund FROM customers c
            LEFT JOIN orders o ON o.customer_id = c.id
            LEFT JOIN returns r ON r.order_id = o.id
            ORDER BY 1, 2, 3
        "},
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "the shift reaches the keys leg of the measure but not its regular leg"]
async fn test_measure_legs_time_shift_of_a_calculated_measure_across_facts() {
    assert_chain(
        "time_shift_of_a_calculated_measure_across_facts",
        indoc! {"
            measures:
              - orders.amount_minus_refund
              - orders.amount_minus_refund_prior_month
            time_dimensions:
              - dimension: orders.created_at
                granularity: month
                dateRange:
                  - \"2025-01-01\"
                  - \"2025-03-31\"
            order:
              - id: orders.created_at
        "},
        &format!(
            "WITH {AMOUNT_MINUS_REFUND_BY_MONTH} \
             SELECT month, v, (SELECT v FROM m p WHERE p.month = m.month - interval '1 month') \
             FROM m ORDER BY 1"
        ),
    )
    .await;
}

// ---- Facts joined into the cube of the measure ----

// The orders leg reads orders alone, the payments leg is rooted at payments:
// an order without payments is counted, a payment of no order lands in the
// NULL status.
#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_into_owner_calculated_by_status() {
    assert_into_owner(
        "into_owner_calculated_by_status",
        indoc! {"
            measures:
              - orders.unpaid
            dimensions:
              - orders.status
            order:
              - id: orders.status
        "},
        indoc! {"
            WITH s AS (SELECT DISTINCT o.status FROM payments p LEFT JOIN orders o ON o.id = p.order_id
                UNION SELECT status FROM orders)
            SELECT s.status,
                   (SELECT sum(amount) FROM orders o WHERE o.status = s.status)
                   - (SELECT sum(p.amount) FROM payments p LEFT JOIN orders o ON o.id = p.order_id
                     WHERE o.status IS NOT DISTINCT FROM s.status)
            FROM s ORDER BY 1
        "},
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_into_owner_calculated_grand_total() {
    assert_into_owner(
        "into_owner_calculated_grand_total",
        indoc! {"
            measures:
              - orders.unpaid
        "},
        "SELECT (SELECT sum(amount) FROM orders) - (SELECT sum(amount) FROM payments)",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_into_owner_calculated_by_a_dimension_beyond_the_owner() {
    assert_into_owner(
        "into_owner_calculated_by_a_dimension_beyond_the_owner",
        indoc! {"
            measures:
              - orders.unpaid
            dimensions:
              - customers.city
            order:
              - id: customers.city
        "},
        indoc! {"
            WITH op AS (SELECT c.city, p.amount FROM payments p
                LEFT JOIN orders o ON o.id = p.order_id LEFT JOIN customers c ON c.id = o.customer_id),
            oo AS (SELECT c.city, o.amount FROM orders o LEFT JOIN customers c ON c.id = o.customer_id),
            s AS (SELECT city FROM op UNION SELECT city FROM oo)
            SELECT s.city,
                   (SELECT sum(amount) FROM oo WHERE oo.city IS NOT DISTINCT FROM s.city)
                   - (SELECT sum(amount) FROM op WHERE op.city IS NOT DISTINCT FROM s.city)
            FROM s ORDER BY 1
        "},
    )
    .await;
}

// No cube reaches both payments and shipments: the measure has no join of
// its own, its leaves have one each.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measure is joined as a whole, and its facts share no root"]
async fn test_measure_legs_into_owner_facts_without_a_common_root() {
    assert_into_owner(
        "into_owner_facts_without_a_common_root",
        indoc! {"
            measures:
              - orders.paid_minus_shipped
            dimensions:
              - orders.status
            order:
              - id: orders.status
        "},
        indoc! {"
            WITH s AS (SELECT DISTINCT o.status FROM payments p LEFT JOIN orders o ON o.id = p.order_id
                UNION SELECT DISTINCT o.status FROM shipments sh LEFT JOIN orders o ON o.id = sh.order_id)
            SELECT s.status,
                   (SELECT sum(p.amount) FROM payments p LEFT JOIN orders o ON o.id = p.order_id
                     WHERE o.status IS NOT DISTINCT FROM s.status)
                   - (SELECT count(*) FROM shipments sh LEFT JOIN orders o ON o.id = sh.order_id
                     WHERE o.status IS NOT DISTINCT FROM s.status)
            FROM s ORDER BY 1
        "},
    )
    .await;
}

// ---- Rollups ----

const ROLLUPS_SCHEMA: &str = "common/integration_measure_legs_rollups.yaml";

// The rollups the query reads, as `cube.rollup`, sorted.
async fn assert_rollup_legs(name: &str, query: &str, rollups: &[&str], oracle_sql: &str) {
    let ctx = TestContext::new(MockSchema::from_yaml_file(ROLLUPS_SCHEMA)).unwrap();
    let (sql, usages) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();
    let mut used = usages
        .iter()
        .map(|u| {
            format!(
                "{}.{}",
                u.pre_aggregation.cube_name(),
                u.pre_aggregation.name()
            )
        })
        .collect::<Vec<_>>();
    used.sort();
    assert_eq!(used, rollups, "{sql}");
    assert_legs(ROLLUPS_SCHEMA, CHAIN_SEED, name, query, oracle_sql).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_rollups_serve_each_leaf_of_a_calculated_measure() {
    assert_rollup_legs(
        "rollups_serve_each_leaf_of_a_calculated_measure",
        indoc! {"
            measures:
              - orders.amount_minus_refund
            dimensions:
              - customers.city
            order:
              - id: customers.city
        "},
        &["orders.by_city", "returns.by_city"],
        &by_city(indoc! {"
            SELECT city,
                   (SELECT sum(amount) FROM o WHERE o.city = cities.city)
                   - (SELECT sum(refund) FROM r WHERE r.city = cities.city)
            FROM cities ORDER BY 1
        "}),
    )
    .await;
}

// Orders have a rollup by status, returns do not: a query is read from rollups
// only when all its leaves are, so it is read from the source.
#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_rollup_serves_one_leaf_of_a_calculated_measure() {
    assert_rollup_legs(
        "rollup_serves_one_leaf_of_a_calculated_measure",
        indoc! {"
            measures:
              - orders.count_minus_returns
            dimensions:
              - orders.status
            order:
              - id: orders.status
        "},
        &[],
        indoc! {"
            SELECT o.status,
                   count(*) - (SELECT count(*) FROM returns r JOIN orders o2 ON o2.id = r.order_id
                     WHERE o2.status = o.status)
            FROM orders o GROUP BY 1 ORDER BY 1
        "},
    )
    .await;
}

// Customers have no rollup, so the facts are not read from theirs either.
#[tokio::test(flavor = "multi_thread")]
async fn test_measure_legs_rollups_beside_a_nested_tree() {
    assert_rollup_legs(
        "rollups_beside_a_nested_tree",
        indoc! {"
            measures:
              - customers.max_lifetime
              - orders.total_amount
              - returns.total_refund
            dimensions:
              - customers.city
            order:
              - id: customers.city
        "},
        &[],
        &by_city(indoc! {"
            SELECT city,
                   (SELECT max(lifetime_value) FROM customers c WHERE c.city = cities.city),
                   (SELECT sum(amount) FROM o WHERE o.city = cities.city),
                   (SELECT sum(refund) FROM r WHERE r.city = cities.city)
            FROM cities ORDER BY 1
        "}),
    )
    .await;
}
