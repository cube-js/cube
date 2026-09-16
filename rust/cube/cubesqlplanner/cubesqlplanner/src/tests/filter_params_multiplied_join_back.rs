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
const FILTER_GROUP_SCAN: &str = "SELECT * FROM fpmjb_orders WHERE \
    {FILTER_GROUP|\
    FILTER_PARAMS_COLUMN:fpmjb_orders.tenant_id:tenant_id|\
    FILTER_PARAMS_COLUMN:fpmjb_orders.created_at:created_at}";

fn schema(fact_sql: &str) -> MockSchema {
    schema_with_key(fact_sql, "id")
}

fn schema_with_key(fact_sql: &str, id_sql: &str) -> MockSchema {
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
            sql: "{id_sql}"
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

// One rendered copy of the fact cube's `sql`.
struct FactCopy {
    // The predicates, with parameter placeholders blanked out so that copies
    // bound to different parameters compare equal.
    predicates: String,
    // The placeholders this copy referenced, in order, as indices into the
    // statement's parameter list.
    param_indices: Vec<usize>,
}

impl FactCopy {
    fn values(&self, params: &[FilterValue]) -> Vec<FilterValue> {
        self.param_indices
            .iter()
            .map(|index| params[*index].clone())
            .collect()
    }
}

fn fact_copies(sql: &str) -> Vec<FactCopy> {
    sql.match_indices(CUBE_SQL_HEAD)
        .map(|(start, _)| {
            let mut depth = 0usize;
            let mut predicates = String::new();
            let mut param_indices = Vec::new();
            let mut chars = sql[start + CUBE_SQL_HEAD.len()..].chars().peekable();
            while let Some(ch) = chars.next() {
                match ch {
                    '(' => depth += 1,
                    ')' if depth == 0 => break,
                    ')' => depth -= 1,
                    '$' => {
                        let mut number = String::new();
                        while let Some(digit) = chars.peek().filter(|c| c.is_ascii_digit()) {
                            number.push(*digit);
                            chars.next();
                        }
                        // Placeholders are one-based.
                        param_indices.push(number.parse::<usize>().unwrap() - 1);
                    }
                    _ => {}
                }
                predicates.push(ch);
            }
            FactCopy {
                predicates,
                param_indices,
            }
        })
        .collect()
}

// Both copies of the fact source render the same predicates bound to the same
// values - the pushdown is only result-neutral while that holds.
fn assert_copies_agree(sql: &str, params: &[FilterValue], expected: &str) -> Vec<FactCopy> {
    let copies = fact_copies(sql);
    assert_eq!(copies.len(), 2, "expected two fact copies\nsql: {}", sql);
    for (side, copy) in ["keys-side", "measure-side"].iter().zip(copies.iter()) {
        assert_eq!(
            copy.predicates, expected,
            "the {} copy must carry the pushed-down predicates, or the join is \
             built against the entire unfiltered fact table\nsql: {}",
            side, sql
        );
    }
    assert_eq!(
        copies[0].values(params),
        copies[1].values(params),
        "both copies must be bound to the same values\nparams: {:?}",
        params
    );
    copies
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

    let copies = assert_copies_agree(&sql, &params, EXPECTED_SCAN_PREDICATES);
    assert_eq!(
        copies[1].values(&params)[0],
        FilterValue::Str("t1".to_string())
    );
}

// A measure reaching another cube is aggregated over a measure subquery
// instead, and that subquery carries its own copy of the cube.
#[test]
fn a_measure_subquery_source_carries_the_pushed_down_predicates() {
    let ctx = TestContext::new(schema(PUSHED_DOWN_SCAN)).unwrap();

    let (sql, params) = ctx
        .build_sql_and_params(&query_for("fpmjb_orders.vip_amount"))
        .unwrap();

    assert_copies_agree(&sql, &params, EXPECTED_SCAN_PREDICATES);
}

// A `FILTER_GROUP` renders its items through the filter subtree search rather
// than one binding at a time, so it reaches the measure side by its own path.
#[test]
fn a_filter_group_binding_reaches_the_measure_side_too() {
    let ctx = TestContext::new(schema(FILTER_GROUP_SCAN)).unwrap();

    let (sql, params) = ctx
        .build_sql_and_params(&query_for("fpmjb_orders.orders_per_buyer"))
        .unwrap();

    assert_copies_agree(
        &sql,
        &params,
        "((created_at >= $::timestamptz AND created_at <= $::timestamptz) \
         AND (tenant_id = $))",
    );
}

// The subtree search only keeps an OR group when every one of its items names
// a bound member, so an OR reaching another cube renders as always-true. Both
// copies still have to agree - that is what keeps the join back matching.
#[test]
fn an_or_filter_across_cubes_stays_always_true_on_both_copies() {
    let ctx = TestContext::new(schema(PUSHED_DOWN_SCAN)).unwrap();

    let (sql, params) = ctx
        .build_sql_and_params(indoc! {r#"
            measures:
              - fpmjb_orders.orders_per_buyer
            dimensions:
              - fpmjb_order_tags.tag
            filters:
              - or:
                  - member: fpmjb_orders.tenant_id
                    operator: equals
                    values:
                      - "t1"
                  - member: fpmjb_order_tags.tag
                    operator: equals
                    values:
                      - "a"
        "#})
        .unwrap();

    assert_copies_agree(&sql, &params, "1 = 1 AND 1 = 1");
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

// The join back compares the key the keys side projected against the key the
// measure side renders. A binding anywhere in the primary key's own `sql` has
// to resolve on both sides, or the two stop being the same expression and the
// join matches nothing.
#[test]
fn the_join_key_renders_the_same_expression_on_both_sides() {
    let ctx = TestContext::new(schema_with_key(
        PUSHED_DOWN_SCAN,
        "CASE WHEN {FILTER_PARAMS_COLUMN:fpmjb_orders.tenant_id:tenant_id} THEN id END",
    ))
    .unwrap();

    let (sql, _) = ctx
        .build_sql_and_params(&query_for("fpmjb_orders.orders_per_buyer"))
        .unwrap();

    let (_, join_back) = sql.rsplit_once(" ON ").expect("a join back\nsql: {sql}");
    let on_clause = join_back.lines().next().unwrap();
    assert!(
        !on_clause.contains("1 = 1"),
        "the measure side of the join key must resolve its binding\non: {}\nsql: {}",
        on_clause,
        sql
    );
    assert!(
        on_clause.contains("CASE WHEN (tenant_id = $"),
        "the measure side of the join key must render the predicate\non: {}\nsql: {}",
        on_clause,
        sql
    );
}
