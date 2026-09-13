//! A measure that needs the full-key plan is aggregated over a keys subquery
//! joined back to a second copy of the fact source by primary key. Both copies
//! read the same fact rows over the same columns, so both must render the
//! cube's `FILTER_PARAMS` bindings as the query's real predicates - otherwise
//! the database builds the join against the whole unfiltered fact table.
//!
//! The join back is by primary key and the keys side already applies the same
//! predicates, so filtering the measure side can only shrink the build, never
//! change the result. The Postgres tests state that in numbers: the same model
//! with and without the bindings answers the same.

use crate::cube_bridge::base_query_options::FilterValue;
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const SEED: &str = "filter_params_multiplied_join_back_tables.sql";

const PUSHED_DOWN_SCAN: &str = "SELECT * FROM fpmjb_orders WHERE \
    {FILTER_PARAMS_COLUMN:fpmjb_orders.tenant_id:tenant_id} AND \
    {FILTER_PARAMS_COLUMN:fpmjb_orders.created_at:created_at}";
const PLAIN_SCAN: &str = "SELECT * FROM fpmjb_orders";

fn schema(fact_sql: &str) -> MockSchema {
    MockSchema::from_yaml(&format!(
        r#"
cubes:
    - name: fpmjb_orders
      sql: "{fact_sql}"
      joins:
          - name: fpmjb_order_tags
            sql: "{{CUBE}}.id = {{fpmjb_order_tags}}.order_id"
            relationship: one_to_many
          - name: fpmjb_users
            sql: "{{CUBE}}.user_id = {{fpmjb_users}}.id"
            relationship: many_to_one
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
            sql: "{{count}} / nullif({{buyers}}, 0)"
          - name: vip_amount
            type: sum
            sql: amount
            filters:
                - sql: "{{fpmjb_users.is_vip}} = true"

    - name: fpmjb_users
      sql: "SELECT * FROM fpmjb_users"
      dimensions:
          - name: id
            type: number
            sql: id
            primary_key: true
          - name: is_vip
            type: boolean
            sql: is_vip

    - name: fpmjb_order_tags
      sql: "SELECT * FROM fpmjb_order_tags"
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
"#
    ))
    .unwrap()
}

fn query_for(measure: &str) -> String {
    format!(
        indoc! {r#"
            measures:
              - {}
            dimensions:
              - fpmjb_order_tags.tag
            time_dimensions:
              - dimension: fpmjb_orders.created_at
                dateRange:
                  - "2026-07-29"
                  - "2026-08-27"
            filters:
              - member: fpmjb_orders.tenant_id
                operator: equals
                values:
                  - "t1"
            order:
              - id: fpmjb_order_tags.tag
        "#},
        measure
    )
}

const CUBE_SQL_HEAD: &str = "SELECT * FROM fpmjb_orders WHERE ";

/// The rendered body of every copy of the fact cube's `sql`, with parameter
/// placeholders blanked out so that copies bound to different parameters
/// compare equal.
fn fact_copies(sql: &str) -> Vec<String> {
    sql.match_indices(CUBE_SQL_HEAD)
        .map(|(start, _)| {
            let mut depth = 0usize;
            let mut body = String::new();
            let mut chars = sql[start + CUBE_SQL_HEAD.len()..].chars().peekable();
            while let Some(ch) = chars.next() {
                match ch {
                    '(' => depth += 1,
                    ')' if depth == 0 => break,
                    ')' => depth -= 1,
                    '$' => {
                        while chars.peek().is_some_and(|c| c.is_ascii_digit()) {
                            chars.next();
                        }
                    }
                    _ => {}
                }
                body.push(ch);
            }
            body
        })
        .collect()
}

const EXPECTED_SCAN_PREDICATES: &str = "(tenant_id = $) AND \
    (created_at >= $::timestamptz AND created_at <= $::timestamptz)";

// A calculated measure over other measures needs the full-key plan, and its
// values come straight from a second copy of the cube.
#[test]
fn a_bare_cube_measure_source_carries_the_pushed_down_predicates() {
    let ctx = TestContext::new(schema(PUSHED_DOWN_SCAN)).unwrap();

    let (sql, params) = ctx
        .build_sql_and_params(&query_for("fpmjb_orders.orders_per_buyer"))
        .unwrap();

    let copies = fact_copies(&sql);
    assert_eq!(copies.len(), 2, "expected two fact copies\nsql: {}", sql);
    assert_eq!(
        copies[0], EXPECTED_SCAN_PREDICATES,
        "the keys-side copy must keep both pushed-down predicates\nsql: {}",
        sql
    );
    assert_eq!(
        copies[1], EXPECTED_SCAN_PREDICATES,
        "the measure-side copy must keep both pushed-down predicates, or the \
         join is built against the entire unfiltered fact table\nsql: {}",
        sql
    );
    assert_eq!(
        params[6..9],
        params[0..3],
        "both copies must be bound to the same values\nparams: {:?}",
        params
    );
    assert_eq!(params[0], FilterValue::Str("t1".to_string()));
}

// A measure reaching another cube is aggregated over a measure subquery
// instead, and that subquery carries its own copy of the cube.
#[test]
fn a_measure_subquery_source_carries_the_pushed_down_predicates() {
    let ctx = TestContext::new(schema(PUSHED_DOWN_SCAN)).unwrap();

    let (sql, params) = ctx
        .build_sql_and_params(&query_for("fpmjb_orders.vip_amount"))
        .unwrap();

    let copies = fact_copies(&sql);
    assert_eq!(copies.len(), 2, "expected two fact copies\nsql: {}", sql);
    assert_eq!(
        copies[0], EXPECTED_SCAN_PREDICATES,
        "the keys-side copy must keep both pushed-down predicates\nsql: {}",
        sql
    );
    assert_eq!(
        copies[1], EXPECTED_SCAN_PREDICATES,
        "the measure-subquery copy must keep both pushed-down predicates\nsql: {}",
        sql
    );
    assert_eq!(
        params[6..9],
        params[0..3],
        "both copies must be bound to the same values\nparams: {:?}",
        params
    );
}

// A plain count is rewritten to a distinct count over a single filtered copy,
// so it never reaches the join back - it anchors the two cases above.
#[test]
fn a_count_measure_needs_no_second_copy() {
    let ctx = TestContext::new(schema(PUSHED_DOWN_SCAN)).unwrap();

    let (sql, _) = ctx
        .build_sql_and_params(&query_for("fpmjb_orders.count"))
        .unwrap();

    assert_eq!(fact_copies(&sql).len(), 1, "sql: {}", sql);
}

async fn assert_pushdown_is_result_neutral(measure: &str) -> Option<String> {
    let query = query_for(measure);
    let pushed_down = TestContext::new(schema(PUSHED_DOWN_SCAN))
        .unwrap()
        .try_execute_pg(&query, SEED)
        .await?;
    let full_scan = TestContext::new(schema(PLAIN_SCAN))
        .unwrap()
        .try_execute_pg(&query, SEED)
        .await
        .expect("the plain model runs wherever the pushdown one does");

    assert_eq!(pushed_down, full_scan);
    Some(pushed_down)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bare_cube_measure_source_answers_the_same_as_a_full_scan() {
    let Some(result) = assert_pushdown_is_result_neutral("fpmjb_orders.orders_per_buyer").await
    else {
        return;
    };
    insta::assert_snapshot!(result);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_measure_subquery_source_answers_the_same_as_a_full_scan() {
    let Some(result) = assert_pushdown_is_result_neutral("fpmjb_orders.vip_amount").await else {
        return;
    };
    insta::assert_snapshot!(result);
}
