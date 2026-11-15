use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::{table_rows, TestContext};
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_fact.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_fact_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_totals_no_dimensions() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
          - returns.count
          - returns.total_refund
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_counts_by_customer_name() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        dimensions:
          - customers.name
        order:
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_sums_by_customer_name() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.total_amount
          - returns.total_refund
        dimensions:
          - customers.name
        order:
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_grouped_by_city() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        dimensions:
          - customers.city
        order:
          - id: customers.city
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_filter_on_shared_dimension() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
          - returns.count
          - returns.total_refund
        dimensions:
          - customers.name
        filters:
          - dimension: customers.city
            operator: equals
            values:
              - New York
        order:
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_filter_on_fact_dimension() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
          - returns.count
          - returns.total_refund
        dimensions:
          - customers.name
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - completed
        order:
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_count_distinct_control() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.unique_customers
          - returns.count
          - returns.unique_customers
        dimensions:
          - customers.city
        order:
          - id: customers.city
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_three_fact_tables() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
          - reviews.count
        dimensions:
          - customers.name
        order:
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_hub_measure_by_fact_dimension() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - customers.count
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_hub_and_fact_measures() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - customers.count
          - orders.count
          - orders.total_amount
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_with_segment() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        dimensions:
          - customers.name
        segments:
          - orders.completed_orders
        order:
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_with_measure_filter() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        dimensions:
          - customers.name
        filters:
          - member: orders.count
            operator: gt
            values:
              - \"1\"
        order:
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_measure_filter_on_second_fact() {
    let ctx = create_context();

    // HAVING returns.count > 1 in multi-fact context
    // returns by customer: Alice=1, Bob=2, Charlie=2, Diana=0
    // After filter: Bob(orders=3, returns=2), Charlie(orders=0/NULL, returns=2)
    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        dimensions:
          - customers.name
        filters:
          - member: returns.count
            operator: gt
            values:
              - \"1\"
        order:
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// ORDER BY a filtered measure that is not selected: it is absent from the
/// schema, so the sort key must be read from the aggregate like the HAVING.
#[tokio::test(flavor = "multi_thread")]
async fn test_order_by_filtered_measure_outside_selection() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - customers.name
        filters:
          - member: returns.count
            operator: gt
            values:
              - \"1\"
        order:
          - id: returns.count
            desc: true
          - id: customers.name
    "};

    let sql = ctx.build_sql(query).unwrap();

    let order_by = sql
        .rsplit_once("ORDER BY")
        .map(|(_, tail)| tail.to_string())
        .unwrap_or_default();
    assert!(
        order_by.contains("returns__count"),
        "Expected the filtered measure to survive as a sort key:\n{}",
        sql
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

async fn assert_rows_match_oracle(ctx: &TestContext, query: &str, seed: &str, oracle_sql: &str) {
    let Some(result) = ctx.try_execute_pg(query, seed).await else {
        return;
    };
    let oracle = ctx.try_execute_raw_pg(oracle_sql, seed).await.unwrap();
    assert_eq!(
        table_rows(&result),
        table_rows(&oracle),
        "cube:\n{result}\noracle:\n{oracle}"
    );
}

/// ORDER BY a measure of another fact that is not selected: it is computed
/// in that fact's aggregate, not in the outer select.
#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_order_by_other_fact_measure_outside_selection() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - returns.count
        dimensions:
          - customers.name
        order:
          - id: orders.total_amount
            desc: true
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    assert_rows_match_oracle(
        &ctx,
        query,
        SEED,
        indoc! {"
            SELECT c.name, COALESCE(r.cnt, 0)
            FROM customers c
            LEFT JOIN (SELECT customer_id, count(*) AS cnt FROM returns GROUP BY 1) r
              ON r.customer_id = c.id
            LEFT JOIN (SELECT customer_id, sum(amount) AS total FROM orders GROUP BY 1) o
              ON o.customer_id = c.id
            ORDER BY o.total DESC, c.name
        "},
    )
    .await;
}

/// ORDER BY a non-selected measure of a fact whose other measures are selected.
#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_order_by_same_fact_measure_outside_selection() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        dimensions:
          - customers.name
        order:
          - id: orders.total_amount
            desc: true
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    assert_rows_match_oracle(
        &ctx,
        query,
        SEED,
        indoc! {"
            SELECT c.name, COALESCE(o.cnt, 0), COALESCE(r.cnt, 0)
            FROM customers c
            LEFT JOIN (SELECT customer_id, count(*) AS cnt FROM returns GROUP BY 1) r
              ON r.customer_id = c.id
            LEFT JOIN (SELECT customer_id, count(*) AS cnt, sum(amount) AS total FROM orders GROUP BY 1) o
              ON o.customer_id = c.id
            ORDER BY o.total DESC, c.name
        "},
    )
    .await;
}

/// ORDER BY a non-selected measure whose join multiplies the selected one.
#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_order_by_fact_measure_outside_selection() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - customers.count
        dimensions:
          - customers.city
        order:
          - id: orders.count
          - id: customers.city
    "};

    ctx.build_sql(query).unwrap();

    assert_rows_match_oracle(
        &ctx,
        query,
        SEED,
        indoc! {"
            SELECT c.city, count(*) AS cnt
            FROM customers c
            GROUP BY c.city
            ORDER BY (SELECT count(*) FROM orders o JOIN customers c2 ON c2.id = o.customer_id
                      WHERE c2.city = c.city), c.city
        "},
    )
    .await;
}

/// ORDER BY a non-selected measure on a single cube.
#[tokio::test(flavor = "multi_thread")]
async fn test_single_cube_order_by_measure_outside_selection() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.status
        order:
          - id: orders.total_amount
    "};

    ctx.build_sql(query).unwrap();

    assert_rows_match_oracle(
        &ctx,
        query,
        SEED,
        indoc! {"
            SELECT status, count(*) AS cnt
            FROM orders
            GROUP BY status
            ORDER BY sum(amount)
        "},
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_with_time_granularity() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        dimensions:
          - customers.name
        time_dimensions:
          - dimension: customers.created_at
            granularity: month
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_with_time_and_daterange() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        dimensions:
          - customers.name
        time_dimensions:
          - dimension: customers.created_at
            granularity: month
            dateRange:
              - \"2025-01-01\"
              - \"2025-02-28\"
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_with_time_and_filter() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        time_dimensions:
          - dimension: customers.created_at
            granularity: month
        filters:
          - dimension: customers.city
            operator: equals
            values:
              - New York
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_with_time_and_dimension() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        dimensions:
          - customers.city
        time_dimensions:
          - dimension: customers.created_at
            granularity: month
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_full_combo() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        dimensions:
          - customers.name
        time_dimensions:
          - dimension: customers.created_at
            granularity: month
            dateRange:
              - \"2025-01-01\"
              - \"2025-03-31\"
        segments:
          - orders.completed_orders
        filters:
          - dimension: customers.city
            operator: equals
            values:
              - New York
        order:
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_with_order_and_limit() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        dimensions:
          - customers.name
        order:
          - id: orders.count
            desc: true
        row_limit: \"2\"
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_aggregate_hub_sum_measure() {
    let ctx = create_context();

    // customers.total_lifetime_value (SUM) by orders.status
    // customers is multiplied in customers→orders join (one_to_many)
    // SUM is not additive in multiplied context → AggregateMultipliedSubquery
    // Measure references only customers → source = Cube (no MeasureSubquery)
    let query = indoc! {"
        measures:
          - customers.total_lifetime_value
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_aggregate_filtered_count_on_pk() {
    let ctx = create_context();

    // customers.active_count (type: count, sql: "{CUBE}.id", filters: [...])
    // grouped by orders.status.
    // customers→orders is one_to_many, so customers is multiplied → AggregateMultipliedSubquery
    // The measure references only customers → source = Cube branch.
    // Regression: the Cube branch used to render the right side of the FK-aggregate
    // rejoin via Expr::Member, which the select's reference substitutions read
    // back from the inner `keys` subquery alias — yielding a tautological
    // `ON keys.<pk> = keys.<pk>` that cross-joined the source table and over-counted.
    let query = indoc! {"
        measures:
          - customers.active_count
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "};

    let sql = ctx.build_sql(query).unwrap();

    // The buggy output contained `keys.customers__id = keys.customers__id`. Flatten
    // the SQL so the assertion survives any future line-wrapping in the planner's
    // formatter, then walk every ON predicate and assert no tautological equality
    // survives. The snapshot below is the airtight guard; this is a targeted
    // structural sanity check pinned to the bug shape.
    let flat = sql.replace('\n', " ");
    let flat_upper = flat.to_ascii_uppercase();
    let mut cursor = 0;
    while let Some(rel) = flat_upper[cursor..].find(" ON ") {
        let on_end = cursor + rel + 4;
        let after = &flat[on_end..];
        let after_upper = &flat_upper[on_end..];
        let bound = [
            " GROUP BY ",
            " ORDER BY ",
            " LEFT JOIN ",
            " INNER JOIN ",
            " RIGHT JOIN ",
            " UNION ",
        ]
        .iter()
        .filter_map(|kw| after_upper.find(kw))
        .min()
        .unwrap_or(after.len());
        for chunk in after[..bound].split(" AND ") {
            if let Some((lhs, rhs)) = chunk.split_once('=') {
                let lhs = lhs.trim().trim_matches('(').trim();
                let rhs = rhs.trim().trim_matches(')').trim();
                assert!(
                    lhs != rhs,
                    "tautological join condition `{lhs} = {rhs}` in:\n{sql}"
                );
            }
        }
        cursor = on_end;
    }

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_aggregate_with_measure_subquery() {
    let ctx = create_context();

    // customers.total_lifetime_value_for_east (SUM with filter {regions.name} = 'East')
    // customers is multiplied in customers→orders join → AggregateMultipliedSubquery
    // Measure filter references regions cube → MeasureSubquery (customers→regions join,
    // where customers is NOT multiplied since many_to_one)
    let query = indoc! {"
        measures:
          - customers.total_lifetime_value_for_east
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_non_multiplied_multi_join() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
        dimensions:
          - customers.name
          - customers.city
        order:
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_aggregate_grouped_by_own_primary_key() {
    let ctx = create_context();

    // customers.total_lifetime_value is multiplied by the customers→orders
    // join, so it is read through the keys subquery and re-joined to customers
    // by customers' primary key. That key is also a query dimension here, so it
    // plays both roles at once and the keys subquery has to project it exactly
    // once - two columns under one alias make every reference to it from the
    // re-join ambiguous.
    let query = indoc! {"
        measures:
          - customers.total_lifetime_value
          - orders.count
        dimensions:
          - customers.id
          - orders.status
        order:
          - id: customers.id
          - id: orders.status
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_view_two_facts_with_measure_filter() {
    let schema = MockSchema::from_yaml_file("common/integration_multi_fact_view.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {"
        measures:
          - activity.impressions_total_clicks
        dimensions:
          - activity.date_id
        filters:
          - member: activity.total_amount
            operator: set
        order:
          - id: activity.date_id
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx
        .try_execute_pg(query, "integration_multi_fact_view_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

/// The same through a view: the sort-only measure is a view member.
#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_view_order_by_measure_outside_selection() {
    let schema = MockSchema::from_yaml_file("common/integration_multi_fact_view.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {"
        measures:
          - activity.impressions_total_clicks
        dimensions:
          - activity.date_id
        order:
          - id: activity.total_amount
            desc: true
    "};

    ctx.build_sql(query).unwrap();

    assert_rows_match_oracle(
        &ctx,
        query,
        "integration_multi_fact_view_tables.sql",
        indoc! {"
            SELECT p.date_id,
                   (SELECT sum(clicks) FROM impressions i WHERE i.date_id = p.date_id)
            FROM periods p
            ORDER BY (SELECT sum(amount) FROM sales s WHERE s.date_id = p.date_id) DESC
        "},
    )
    .await;
}

// The oracle of a calculated measure over facts of one customer: every leaf is
// aggregated over the rows of its own cube in the joint join, each row once.
const JOINT: &str = indoc! {"
    WITH j AS (
      SELECT c.id AS cid, c.name, c.city, c.lifetime_value, g.name AS region,
             o.id AS oid, o.amount, o.status, o.customer_id AS ocid,
             date_trunc('month', o.created_at) AS omonth,
             r.id AS rid, r.refund_amount, r.reason, r.customer_id AS rcid
      FROM customers c
      JOIN regions g ON g.id = c.region_id
      LEFT JOIN orders o ON o.customer_id = c.id
      LEFT JOIN returns r ON r.customer_id = c.id)
"};

struct Joint<'a> {
    dims: &'a [&'a str],
    filter: &'a str,
}

impl Joint<'_> {
    fn group(&self) -> String {
        self.dims
            .iter()
            .map(|d| format!("j2.{d} IS NOT DISTINCT FROM j.{d}"))
            .chain(std::iter::once(self.filter.to_string()))
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    fn sum(&self, key: &str, value: &str) -> String {
        format!(
            "(SELECT sum(v) FROM (SELECT DISTINCT j2.{key}, {value} AS v FROM j j2 \
             WHERE j2.{key} IS NOT NULL AND {}) d)",
            self.group()
        )
    }

    fn count(&self, key: &str) -> String {
        format!(
            "(SELECT count(DISTINCT j2.{key}) FROM j j2 WHERE {})",
            self.group()
        )
    }

    fn query(&self, measures: &[String], order: &str) -> String {
        let measures = measures.join(", ");
        if self.dims.is_empty() {
            return format!("{JOINT} SELECT {measures} FROM (SELECT 1) j");
        }
        let dims = self.dims.join(", ");
        format!(
            "{JOINT} SELECT {dims}, {measures} FROM j WHERE {} GROUP BY {dims} ORDER BY {order}",
            self.filter
        )
    }
}

fn by(dims: &'static [&'static str]) -> Joint<'static> {
    Joint {
        dims,
        filter: "TRUE",
    }
}

fn amount(j: &Joint) -> String {
    j.sum("oid", "j2.amount")
}

fn refund(j: &Joint) -> String {
    j.sum("rid", "j2.refund_amount")
}

fn amount_minus_refund(j: &Joint) -> String {
    format!("{} - {}", amount(j), refund(j))
}

async fn assert_calculated(name: &str, query: &str, oracle_sql: &str) {
    let ctx = create_context();
    let sql = ctx.build_sql(query).unwrap();
    let Some(result) = ctx.try_execute_pg(query, SEED).await else {
        return;
    };
    let oracle = ctx.try_execute_raw_pg(oracle_sql, SEED).await.unwrap();
    assert_eq!(
        table_rows(&result),
        table_rows(&oracle),
        "cube:\n{result}\noracle:\n{oracle}\nsql:\n{sql}"
    );
    insta::assert_snapshot!(name, result);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_calculated_measure_with_its_ingredient() {
    let j = by(&["city"]);
    assert_calculated(
        "multi_fact_calculated_measure_with_its_ingredient",
        indoc! {"
            measures:
              - orders.avg_amount_per_return
              - orders.total_amount
            dimensions:
              - customers.city
            order:
              - id: customers.city
        "},
        &j.query(
            &[
                format!("{} / NULLIF({}, 0)", amount(&j), j.count("rid")),
                amount(&j),
            ],
            "1",
        ),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_calculated_sum_minus_sum_of_another_fact() {
    let j = by(&["city"]);
    assert_calculated(
        "multi_fact_calculated_sum_minus_sum_of_another_fact",
        indoc! {"
            measures:
              - orders.amount_minus_refund
            dimensions:
              - customers.city
            order:
              - id: customers.city
        "},
        &j.query(&[amount_minus_refund(&j)], "1"),
    )
    .await;
}

// A leaf of orders summing a dimension of customers, once per order.
#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_calculated_with_dimension_only_leaf() {
    let j = by(&["city"]);
    assert_calculated(
        "multi_fact_calculated_with_dimension_only_leaf",
        indoc! {"
            measures:
              - orders.lifetime_minus_refund
            dimensions:
              - customers.city
            order:
              - id: customers.city
        "},
        &j.query(
            &[format!(
                "{} - {}",
                j.sum("oid", "j2.lifetime_value"),
                refund(&j)
            )],
            "1",
        ),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "selected beside the calculated measure, the leaf is summed over customers without orders"]
async fn test_multi_fact_calculated_with_dimension_only_leaf_and_its_ingredient() {
    let j = by(&["city"]);
    assert_calculated(
        "multi_fact_calculated_with_dimension_only_leaf_and_its_ingredient",
        indoc! {"
            measures:
              - orders.lifetime_minus_refund
              - orders.customer_lifetime_per_order
            dimensions:
              - customers.city
            order:
              - id: customers.city
        "},
        &j.query(
            &[
                format!("{} - {}", j.sum("oid", "j2.lifetime_value"), refund(&j)),
                j.sum("oid", "j2.lifetime_value"),
            ],
            "1",
        ),
    )
    .await;
}

// A leaf over a sub_query dimension of customers: the order total of each
// customer, once per customer.
#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_calculated_with_sub_query_leaf() {
    let j = by(&["city"]);
    assert_calculated(
        "multi_fact_calculated_with_sub_query_leaf",
        indoc! {"
            measures:
              - returns.refund_share_of_orders
            dimensions:
              - customers.city
            order:
              - id: customers.city
        "},
        &j.query(
            &[format!(
                "{} / NULLIF({}, 0)",
                refund(&j),
                j.sum(
                    "cid",
                    "(SELECT sum(amount) FROM orders WHERE customer_id = j2.cid)"
                )
            )],
            "1",
        ),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_calculated_over_a_nested_cube() {
    let j = by(&["city"]);
    assert_calculated(
        "multi_fact_calculated_over_a_nested_cube",
        indoc! {"
            measures:
              - orders.amount_per_customer
            dimensions:
              - customers.city
            order:
              - id: customers.city
        "},
        &j.query(
            &[format!("{} / NULLIF({}, 0)", amount(&j), j.count("cid"))],
            "1",
        ),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_calculated_of_one_fact_beside_another_fact() {
    let j = by(&["city"]);
    assert_calculated(
        "multi_fact_calculated_of_one_fact_beside_another_fact",
        indoc! {"
            measures:
              - orders.avg_amount
              - returns.count
            dimensions:
              - customers.city
            order:
              - id: customers.city
        "},
        &j.query(
            &[
                format!("{} / NULLIF({}, 0)", amount(&j), j.count("oid")),
                j.count("rid"),
            ],
            "1",
        ),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_calculated_filtered_and_ordered() {
    let j = by(&["city"]);
    let avg = format!("{} / NULLIF({}, 0)", amount(&j), j.count("rid"));
    assert_calculated(
        "multi_fact_calculated_filtered_and_ordered",
        indoc! {"
            measures:
              - orders.avg_amount_per_return
              - orders.amount_minus_refund
            dimensions:
              - customers.city
            filters:
              - member: orders.avg_amount_per_return
                operator: gt
                values:
                  - \"100\"
            order:
              - id: orders.amount_minus_refund
                desc: true
        "},
        &format!(
            "SELECT * FROM ({}) t WHERE avg > 100 ORDER BY 3 DESC",
            j.query(&[format!("{avg} AS avg"), amount_minus_refund(&j)], "1")
        ),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_calculated_semantics_count_by_customer() {
    let j = by(&["name"]);
    assert_calculated(
        "multi_fact_calculated_semantics_count_by_customer",
        indoc! {"
            measures:
              - orders.count_minus_returns
            dimensions:
              - customers.name
            order:
              - id: customers.name
        "},
        &j.query(&[format!("{} - {}", j.count("oid"), j.count("rid"))], "1"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_calculated_semantics_count_distinct_by_city() {
    let j = by(&["city"]);
    assert_calculated(
        "multi_fact_calculated_semantics_count_distinct_by_city",
        indoc! {"
            measures:
              - orders.unique_customers_minus_returning
            dimensions:
              - customers.city
            order:
              - id: customers.city
        "},
        &j.query(&[format!("{} - {}", j.count("ocid"), j.count("rcid"))], "1"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_calculated_semantics_by_fact_dimension() {
    let j = by(&["city", "status"]);
    assert_calculated(
        "multi_fact_calculated_semantics_by_fact_dimension",
        indoc! {"
            measures:
              - orders.amount_minus_refund
            dimensions:
              - customers.city
              - orders.status
            order:
              - id: customers.city
              - id: orders.status
        "},
        &j.query(&[amount_minus_refund(&j)], "1, 2"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_calculated_semantics_by_other_fact_dimension() {
    let j = by(&["city", "reason"]);
    assert_calculated(
        "multi_fact_calculated_semantics_by_other_fact_dimension",
        indoc! {"
            measures:
              - orders.amount_minus_refund
            dimensions:
              - customers.city
              - returns.reason
            order:
              - id: customers.city
              - id: returns.reason
        "},
        &j.query(&[amount_minus_refund(&j)], "1, 2"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_calculated_semantics_by_region() {
    let j = by(&["city", "region"]);
    assert_calculated(
        "multi_fact_calculated_semantics_by_region",
        indoc! {"
            measures:
              - orders.amount_minus_refund
            dimensions:
              - customers.city
              - regions.name
            order:
              - id: customers.city
              - id: regions.name
        "},
        &j.query(&[amount_minus_refund(&j)], "1, 2"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_calculated_semantics_by_fact_and_other_fact_dimension() {
    let j = by(&["status", "reason", "city"]);
    assert_calculated(
        "multi_fact_calculated_semantics_by_fact_and_other_fact_dimension",
        indoc! {"
            measures:
              - orders.amount_minus_refund
            dimensions:
              - orders.status
              - returns.reason
              - customers.city
            order:
              - id: orders.status
              - id: returns.reason
              - id: customers.city
        "},
        &j.query(&[amount_minus_refund(&j)], "1, 2, 3"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_calculated_semantics_grand_total() {
    let j = Joint {
        dims: &[],
        filter: "j2.city IS NOT NULL",
    };
    assert_calculated(
        "multi_fact_calculated_semantics_grand_total",
        indoc! {"
            measures:
              - orders.amount_minus_refund
            filters:
              - member: customers.city
                operator: set
        "},
        &j.query(&[amount_minus_refund(&j)], ""),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_calculated_semantics_filtered_by_one_fact() {
    let j = Joint {
        dims: &["city"],
        filter: "status = 'completed'",
    };
    assert_calculated(
        "multi_fact_calculated_semantics_filtered_by_one_fact",
        indoc! {"
            measures:
              - orders.amount_minus_refund
            dimensions:
              - customers.city
            filters:
              - member: orders.status
                operator: equals
                values:
                  - completed
            order:
              - id: customers.city
        "},
        &j.query(&[amount_minus_refund(&j)], "1"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_calculated_semantics_filtered_by_other_fact() {
    let j = Joint {
        dims: &["city"],
        filter: "reason = 'defective'",
    };
    assert_calculated(
        "multi_fact_calculated_semantics_filtered_by_other_fact",
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
        &j.query(&[amount_minus_refund(&j)], "1"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_calculated_semantics_segment_of_one_fact() {
    let j = Joint {
        dims: &["city"],
        filter: "status = 'completed'",
    };
    assert_calculated(
        "multi_fact_calculated_semantics_segment_of_one_fact",
        indoc! {"
            measures:
              - orders.amount_minus_refund
            dimensions:
              - customers.city
            segments:
              - orders.completed_orders
            order:
              - id: customers.city
        "},
        &j.query(&[amount_minus_refund(&j)], "1"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_calculated_semantics_by_month() {
    let j = by(&["city", "omonth"]);
    assert_calculated(
        "multi_fact_calculated_semantics_by_month",
        indoc! {"
            measures:
              - orders.amount_minus_refund
            dimensions:
              - customers.city
            time_dimensions:
              - dimension: orders.created_at
                granularity: month
            order:
              - id: customers.city
              - id: orders.created_at
        "},
        &j.query(&[amount_minus_refund(&j)], "1, 2"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_calculated_semantics_with_a_leaf_of_a_nested_cube() {
    let j = by(&["city"]);
    assert_calculated(
        "multi_fact_calculated_semantics_with_a_leaf_of_a_nested_cube",
        indoc! {"
            measures:
              - orders.amount_minus_refund_per_customer
              - customers.count
            dimensions:
              - customers.city
            order:
              - id: customers.city
        "},
        &j.query(
            &[
                format!(
                    "({}) / NULLIF({}, 0)",
                    amount_minus_refund(&j),
                    j.count("cid")
                ),
                j.count("cid"),
            ],
            "1",
        ),
    )
    .await;
}

// Raw rows of the whole join of the measure, each one computing the formula.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "one leg yields raw rows, the other aggregates through a keys subquery"]
async fn test_multi_fact_calculated_ungrouped() {
    let query = indoc! {"
        measures:
          - orders.avg_amount_per_return
        dimensions:
          - customers.id
        order:
          - id: customers.id
        ungrouped: true
    "};
    let ctx = create_context();
    let sql = ctx.build_sql(query).unwrap();
    let Some(result) = ctx.try_execute_pg(query, SEED).await else {
        return;
    };
    let oracle = ctx
        .try_execute_raw_pg(
            indoc! {"
                SELECT c.id, o.amount / NULLIF(CASE WHEN r.id IS NOT NULL THEN 1 END, 0)
                FROM customers c
                LEFT JOIN orders o ON o.customer_id = c.id
                LEFT JOIN returns r ON r.customer_id = c.id
            "},
            SEED,
        )
        .await
        .unwrap();
    let sorted = |table: &str| {
        let mut rows = table_rows(table);
        rows.sort();
        rows
    };
    assert_eq!(
        sorted(&result),
        sorted(&oracle),
        "cube:\n{result}\noracle:\n{oracle}\nsql:\n{sql}"
    );
}

fn create_two_paths_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_fact_two_paths.yaml");
    TestContext::new(schema).unwrap()
}

// `returns` reaches `regions` directly and through `customers`, which assign
// its rows to different regions. The joint join is rooted at customers when
// `customers` is among the members of the query, at `returns` otherwise.
const TWO_PATHS_FROM_CUSTOMERS: &str = indoc! {"
    WITH j AS (
      SELECT c.city, g.name AS region, o.id AS oid, o.amount, r.id AS rid, r.refund_amount
      FROM customers c
      LEFT JOIN regions g ON g.id = c.region_id
      LEFT JOIN orders o ON o.customer_id = c.id
      LEFT JOIN returns r ON r.customer_id = c.id)
"};

const TWO_PATHS_FROM_RETURNS: &str = indoc! {"
    WITH j AS (
      SELECT c.city, g.name AS region, o.id AS oid, o.amount, r.id AS rid, r.refund_amount
      FROM returns r
      LEFT JOIN regions g ON g.id = r.customer_id
      LEFT JOIN customers c ON c.id = r.customer_id
      LEFT JOIN orders o ON o.customer_id = c.id)
"};

async fn assert_two_paths(name: &str, query: &str, oracle_sql: &str) {
    let ctx = create_two_paths_context();
    let sql = ctx.build_sql(query).unwrap();
    let Some(result) = ctx.try_execute_pg(query, SEED).await else {
        return;
    };
    let oracle = ctx.try_execute_raw_pg(oracle_sql, SEED).await.unwrap();
    assert_eq!(
        table_rows(&result),
        table_rows(&oracle),
        "cube:\n{result}\noracle:\n{oracle}\nsql:\n{sql}"
    );
    insta::assert_snapshot!(name, result);
}

fn two_paths(join: &str, dims: &[&str], filter: &str, measures: &[&str]) -> String {
    let group = dims
        .iter()
        .map(|d| format!("j2.{d} IS NOT DISTINCT FROM j.{d}"))
        .chain(std::iter::once(filter.to_string()))
        .collect::<Vec<_>>()
        .join(" AND ");
    let amount = format!(
        "(SELECT sum(v) FROM (SELECT DISTINCT j2.oid, j2.amount AS v FROM j j2 \
         WHERE j2.oid IS NOT NULL AND {group}) d)"
    );
    let refund = format!(
        "(SELECT sum(v) FROM (SELECT DISTINCT j2.rid, j2.refund_amount AS v FROM j j2 \
         WHERE j2.rid IS NOT NULL AND {group}) d)"
    );
    let measures = measures
        .iter()
        .map(|m| m.replace("AMOUNT", &amount).replace("REFUND", &refund))
        .collect::<Vec<_>>()
        .join(", ");
    let dims_list = dims.join(", ");
    let order = (1..=dims.len())
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "{join} SELECT {dims_list}, {measures} FROM j WHERE {filter} \
         GROUP BY {dims_list} ORDER BY {order}"
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_calculated_two_paths_with_its_leaves() {
    assert_two_paths(
        "multi_fact_calculated_two_paths_with_its_leaves",
        indoc! {"
        measures:
          - orders.amount_minus_refund
          - orders.total_amount
          - returns.total_refund
        dimensions:
          - customers.city
          - regions.name
        order:
          - id: customers.city
          - id: regions.name
    "},
        &two_paths(
            TWO_PATHS_FROM_CUSTOMERS,
            &["city", "region"],
            "TRUE",
            &["AMOUNT - REFUND", "AMOUNT", "REFUND"],
        ),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_calculated_two_paths_by_city_and_region() {
    assert_two_paths(
        "multi_fact_calculated_two_paths_by_city_and_region",
        indoc! {"
        measures:
          - orders.amount_minus_refund
        dimensions:
          - customers.city
          - regions.name
        order:
          - id: customers.city
          - id: regions.name
    "},
        &two_paths(
            TWO_PATHS_FROM_CUSTOMERS,
            &["city", "region"],
            "TRUE",
            &["AMOUNT - REFUND"],
        ),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_calculated_two_paths_refund_first_by_region_and_city() {
    assert_two_paths(
        "multi_fact_calculated_two_paths_refund_first_by_region_and_city",
        indoc! {"
        measures:
          - returns.refund_minus_amount
          - returns.total_refund
        dimensions:
          - regions.name
          - customers.city
        order:
          - id: regions.name
          - id: customers.city
    "},
        &two_paths(
            TWO_PATHS_FROM_CUSTOMERS,
            &["region", "city"],
            "TRUE",
            &["REFUND - AMOUNT", "REFUND"],
        ),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_calculated_two_paths_refund_first_by_region() {
    assert_two_paths(
        "multi_fact_calculated_two_paths_refund_first_by_region",
        indoc! {"
        measures:
          - returns.refund_minus_amount
        dimensions:
          - regions.name
        order:
          - id: regions.name
    "},
        &two_paths(
            TWO_PATHS_FROM_RETURNS,
            &["region"],
            "TRUE",
            &["REFUND - AMOUNT"],
        ),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_calculated_two_paths_by_region() {
    assert_two_paths(
        "multi_fact_calculated_two_paths_by_region",
        indoc! {"
        measures:
          - orders.amount_minus_refund
        dimensions:
          - regions.name
        order:
          - id: regions.name
    "},
        &two_paths(
            TWO_PATHS_FROM_RETURNS,
            &["region"],
            "TRUE",
            &["AMOUNT - REFUND"],
        ),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_calculated_two_paths_by_region_with_a_leaf() {
    assert_two_paths(
        "multi_fact_calculated_two_paths_by_region_with_a_leaf",
        indoc! {"
        measures:
          - returns.refund_minus_amount
          - returns.total_refund
        dimensions:
          - regions.name
        order:
          - id: regions.name
    "},
        &two_paths(
            TWO_PATHS_FROM_RETURNS,
            &["region"],
            "TRUE",
            &["REFUND - AMOUNT", "REFUND"],
        ),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_calculated_two_paths_by_region_filtered_by_city() {
    assert_two_paths(
        "multi_fact_calculated_two_paths_by_region_filtered_by_city",
        indoc! {"
        measures:
          - returns.refund_minus_amount
          - returns.total_refund
        dimensions:
          - regions.name
        filters:
          - member: customers.city
            operator: set
        order:
          - id: regions.name
    "},
        &two_paths(
            TWO_PATHS_FROM_CUSTOMERS,
            &["region"],
            "city IS NOT NULL",
            &["REFUND - AMOUNT", "REFUND"],
        ),
    )
    .await;
}
