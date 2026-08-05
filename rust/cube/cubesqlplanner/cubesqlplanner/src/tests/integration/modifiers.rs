use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_basic_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_basic.yaml");
    TestContext::new(schema).unwrap()
}

fn create_multi_fact_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_fact.yaml");
    TestContext::new(schema).unwrap()
}

const BASIC_SEED: &str = "integration_basic_tables.sql";
const MULTI_FACT_SEED: &str = "integration_multi_fact_tables.sql";

/// Body of the first `<name> AS ( ... )` block, in text order, whose text
/// contains `marker`, so an assertion can be aimed at one CTE instead of the
/// whole statement. `marker` must be lower-case.
fn cte_body_containing(sql: &str, marker: &str) -> Option<String> {
    let lower = sql.to_lowercase();
    let mut from = 0;
    while let Some(rel) = lower[from..].find(" as (") {
        let open = from + rel + " as (".len() - 1;
        let mut depth = 0;
        let mut close = None;
        for (offset, ch) in sql[open..].char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        close = Some(open + offset + 1);
                        break;
                    }
                }
                _ => {}
            }
        }
        let close = close?;
        let body = &sql[open..close];
        if body.to_lowercase().contains(marker) {
            return Some(body.to_string());
        }
        from = open + 1;
    }
    None
}

// 9.1: ORDER BY dimension ASC, measure DESC
#[tokio::test(flavor = "multi_thread")]
async fn test_order_by_dimension_and_measure() {
    let ctx = create_basic_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
        dimensions:
          - customers.city
        order:
          - id: customers.city
          - id: orders.total_amount
            desc: true
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// 9.2: LIMIT with OFFSET
#[tokio::test(flavor = "multi_thread")]
async fn test_limit_with_offset() {
    let ctx = create_basic_context();

    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.status
        order:
          - id: orders.status
        row_limit: \"2\"
        offset: \"1\"
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// 9.3: Ungrouped — single cube, all rows without GROUP BY
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_single_cube() {
    let ctx = create_basic_context();

    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.status
          - orders.amount
        ungrouped: true
        order:
          - id: orders.id
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// 9.4: Ungrouped with join — dim from joined cube
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_with_join() {
    let ctx = create_basic_context();

    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.status
          - orders.amount
          - customers.name
        ungrouped: true
        order:
          - id: orders.id
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// 9.5: Ungrouped multi-fact — measures from different fact tables
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_multi_fact() {
    let ctx = create_multi_fact_context();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        dimensions:
          - customers.name
        ungrouped: true
        order:
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, MULTI_FACT_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// 9.6: Ungrouped with multiplied measure
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_multiplied() {
    let ctx = create_multi_fact_context();

    let query = indoc! {"
        measures:
          - customers.count
        dimensions:
          - orders.status
          - customers.name
        ungrouped: true
        order:
          - id: orders.id
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, MULTI_FACT_SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_limit_only_no_offset() {
    let ctx = create_basic_context();

    // Limit 2 without offset: cancelled(1), completed(5)
    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.status
        order:
          - id: orders.status
        row_limit: \"2\"
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_order_by_measure_only() {
    let ctx = create_basic_context();

    // Order by count desc only (no dimension in ORDER)
    // completed=5, pending=3, cancelled=1
    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.status
        order:
          - id: orders.count
            desc: true
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_order_by_time_dimension() {
    let ctx = create_basic_context();

    // Order by time dimension desc: Apr(1), Mar(3), Feb(2), Jan(3)
    let query = indoc! {"
        measures:
          - orders.count
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
        order:
          - id: orders.created_at
            desc: true
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_with_filter() {
    let ctx = create_basic_context();

    // Ungrouped + filter status=completed → orders 1,2,4,6,8
    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.id
          - orders.status
          - orders.amount
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - completed
        ungrouped: true
        order:
          - id: orders.id
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_with_time_dimension() {
    let ctx = create_basic_context();

    // Ungrouped + time dimension dateRange Jan → orders 1,2,9
    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.id
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            dateRange:
              - \"2024-01-01\"
              - \"2024-01-31\"
        ungrouped: true
        order:
          - id: orders.id
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

const UNGROUPED_DUP_SEED: &str = "ungrouped_multiplied_dup_tables.sql";

// A measure whose own sql already aggregates cannot be projected row-level: the
// select it lands in must group, or the SQL is not valid at all. This holds for
// an ungrouped request too, so the multiplied CTE keeps its GROUP BY here.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_multiplied_keeps_group_by_for_aggregate_sql_measure() {
    let schema = MockSchema::from_yaml_file("common/ungrouped_multiplied_dup.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {"
        measures:
          - dup_customers.ltv_agg_number
        dimensions:
          - dup_customers.name
          - dup_orders.status
        ungrouped: true
    "};

    let sql = ctx.build_sql(query).unwrap();
    let lower = sql.to_lowercase();

    assert!(
        lower.contains("select distinct"),
        "expected the keys subquery of a multiplied measure, got:\n{sql}"
    );
    assert!(
        lower.contains("group by"),
        "a measure that aggregates in its own sql needs the enclosing select to \
         group, got:\n{sql}"
    );

    // Executing is the real assertion: without the grouping the engine rejects
    // the statement outright.
    if let Some(result) = ctx.try_execute_pg(query, UNGROUPED_DUP_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// A CTE hoisted out of a multi-stage leaf is consumed by the stage above it,
// which re-aggregates its rows. It projects row-level values for that reason, so
// it has to keep grouping to the leaf's own grain no matter what the request
// asked for — an ungrouped request does not make this CTE a raw-row scan.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_request_keeps_group_by_in_hoisted_multi_stage_leaf() {
    let schema =
        MockSchema::from_yaml_file("common/integration_multi_stage_multiplied_pre_agg.yaml")
            .only_pre_aggregations(&[]);
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {"
        measures:
          - customers.total_lifetime_value_prev_month_by_returns
        time_dimensions:
          - dimension: returns.created_at
            granularity: month
        ungrouped: true
    "};

    let sql = ctx.build_sql(query).unwrap();

    // The hoisted leaf is the CTE holding the multiplied-measure keys subquery.
    let leaf = cte_body_containing(&sql, "as \"keys\"").unwrap_or_else(|| {
        panic!("expected a hoisted keys subquery, got:\n{sql}");
    });

    assert!(
        leaf.to_lowercase().contains("group by"),
        "the hoisted leaf feeds a stage that re-aggregates it, so it must stay \
         grouped, got:\n{leaf}"
    );
}
