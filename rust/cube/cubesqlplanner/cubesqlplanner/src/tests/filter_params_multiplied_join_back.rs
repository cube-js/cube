//! Reproduction for cube-js/cube#11740.
//!
//! A calculated measure queried together with a dimension of a `one_to_many`
//! joined cube is planned as a full-key aggregate: a keys subquery joined back
//! to a second copy of the fact source by primary key. The keys-side copy is
//! planned with the query's filters, so the cube's `FILTER_PARAMS` render as
//! real predicates. The measure-side copy is planned as a bare cube with no
//! filter context at all (`aggregate_subquery_plan` builds it as `pk_cube`),
//! so every `FILTER_PARAMS` binding falls back to always-true and the database
//! builds the join against the whole unfiltered fact table.
//!
//! Both copies read the same fact rows over the same columns and the join back
//! is by primary key, so pushing the predicates into the measure side can only
//! shrink the build - it cannot change the result.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn schema() -> MockSchema {
    MockSchema::from_yaml(indoc! {"
        cubes:
            - name: orders
              sql: \"SELECT * FROM orders WHERE {FILTER_PARAMS_COLUMN:orders.tenant_id:tenant_id} AND {FILTER_PARAMS_COLUMN:orders.created_at:created_at}\"
              joins:
                  - name: order_tags
                    sql: \"{CUBE}.id = {order_tags}.order_id\"
                    relationship: one_to_many
              dimensions:
                  - name: id
                    type: number
                    sql: id
                    primary_key: true
                  - name: tenant_id
                    type: string
                    sql: tenant_id
                  - name: created_at
                    type: time
                    sql: created_at
              measures:
                  - name: count
                    type: count
                  - name: buyers
                    type: count_distinct
                    sql: user_id
                  - name: orders_per_buyer
                    type: number
                    sql: \"{count} / nullif({buyers}, 0)\"

            - name: order_tags
              sql: \"SELECT * FROM order_tags\"
              dimensions:
                  - name: id
                    type: number
                    sql: id
                    primary_key: true
                  - name: order_id
                    type: number
                    sql: order_id
                  - name: tag
                    type: string
                    sql: tag
    "})
    .unwrap()
}

const QUERY: &str = indoc! {"
    measures:
      - orders.orders_per_buyer
    dimensions:
      - order_tags.tag
    time_dimensions:
      - dimension: orders.created_at
        dateRange:
          - \"2026-07-29\"
          - \"2026-08-27\"
    filters:
      - member: orders.tenant_id
        operator: equals
        values:
          - \"t1\"
"};

// The keys side already works - it anchors the comparison, so a regression that
// drops the pushdown everywhere is not mistaken for a fix of the measure side.
#[test]
fn keys_side_copy_carries_the_pushed_down_predicates() {
    let ctx = TestContext::new(schema()).unwrap();

    let (sql, _) = ctx.build_sql_and_params(QUERY).unwrap();

    assert!(
        sql.contains(
            "SELECT * FROM orders WHERE (tenant_id = $1) AND \
             (created_at >= $2::timestamptz AND created_at <= $3::timestamptz)"
        ),
        "the keys-side fact copy must keep both pushed-down predicates\nsql: {}",
        sql
    );
}

#[test]
fn measure_side_copy_carries_the_pushed_down_predicates() {
    let ctx = TestContext::new(schema()).unwrap();

    let (sql, _) = ctx.build_sql_and_params(QUERY).unwrap();

    assert!(
        !sql.contains("SELECT * FROM orders WHERE 1 = 1 AND 1 = 1"),
        "the measure-side fact copy must not fall back to always-true, or the \
         join is built against the entire unfiltered fact table\nsql: {}",
        sql
    );
}
