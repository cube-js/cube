//! A filter template splices the member's rendered SQL next to an operator of
//! its own, so the member has to arrive as a single operand. These tests pin the
//! parenthesization per operator: a compound member is wrapped, an atomic one is
//! left alone, and a trailing line comment forces the wrap on its own line.

use super::assert_filter;
use crate::cube_bridge::base_query_options::FilterValue;
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn build(filter_yaml: &str) -> (String, Vec<FilterValue>) {
    let schema = MockSchema::from_yaml_file("common/filter_operand.yaml");
    let ctx = TestContext::new(schema).unwrap();
    let query = format!("measures:\n  - orders.count\n{}", filter_yaml);
    ctx.build_filter_sql(&query)
        .expect("Should generate filter SQL")
}

const AND_MEMBER: &str = r#"("orders".amount > 50 AND "orders".flag)"#;
const NUM_MEMBER: &str = r#"("orders".amount + 1)"#;
const STR_MEMBER: &str = r#"("orders".note || '-tag')"#;
const TS_MEMBER: &str = r#"("orders".created_at + INTERVAL '1 day')"#;

// ── equality ────────────────────────────────────────────────────────────────

#[test]
fn test_equals_compound_member() {
    let result = build(indoc! {"
        filters:
          - dimension: orders.big_and_flag
            operator: equals
            values:
              - \"true\"
    "});
    assert_filter(
        &result,
        &format!("({AND_MEMBER} = $_0_$::boolean)"),
        &["true"],
    );
}

#[test]
fn test_not_equals_compound_member() {
    let result = build(indoc! {"
        filters:
          - dimension: orders.big_and_flag
            operator: notEquals
            values:
              - \"true\"
    "});
    assert_filter(
        &result,
        &format!("({AND_MEMBER} <> $_0_$::boolean OR {AND_MEMBER} IS NULL)"),
        &["true"],
    );
}

#[test]
fn test_in_list_compound_member() {
    let result = build(indoc! {"
        filters:
          - dimension: orders.big_and_flag
            operator: equals
            values:
              - \"true\"
              - \"false\"
    "});
    assert_filter(
        &result,
        &format!("({AND_MEMBER} IN ($_0_$::boolean, $_1_$::boolean))"),
        &["true", "false"],
    );
}

#[test]
fn test_not_in_list_compound_member() {
    let result = build(indoc! {"
        filters:
          - dimension: orders.big_and_flag
            operator: notEquals
            values:
              - \"true\"
              - \"false\"
    "});
    assert_filter(
        &result,
        &format!("({AND_MEMBER} NOT IN ($_0_$::boolean, $_1_$::boolean) OR {AND_MEMBER} IS NULL)"),
        &["true", "false"],
    );
}

// ── nullability ─────────────────────────────────────────────────────────────

#[test]
fn test_set_compound_member() {
    let result = build(indoc! {"
        filters:
          - dimension: orders.big_and_flag
            operator: set
    "});
    assert_filter(&result, &format!("({AND_MEMBER} IS NOT NULL)"), &[]);
}

#[test]
fn test_not_set_compound_member() {
    let result = build(indoc! {"
        filters:
          - dimension: orders.big_or_flag
            operator: notSet
    "});
    assert_filter(
        &result,
        r#"(("orders".amount > 50 OR "orders".flag) IS NULL)"#,
        &[],
    );
}

// ── comparison ──────────────────────────────────────────────────────────────

#[test]
fn test_gt_compound_member() {
    let result = build(indoc! {"
        filters:
          - dimension: orders.amount_plus
            operator: gt
            values:
              - \"10\"
    "});
    assert_filter(
        &result,
        &format!("({NUM_MEMBER} > $_0_$::numeric)"),
        &["10"],
    );
}

#[test]
fn test_lte_compound_member() {
    let result = build(indoc! {"
        filters:
          - dimension: orders.amount_plus
            operator: lte
            values:
              - \"10\"
    "});
    assert_filter(
        &result,
        &format!("({NUM_MEMBER} <= $_0_$::numeric)"),
        &["10"],
    );
}

// ── like ────────────────────────────────────────────────────────────────────

#[test]
fn test_contains_compound_member() {
    let result = build(indoc! {"
        filters:
          - dimension: orders.note_tagged
            operator: contains
            values:
              - alpha
    "});
    assert_filter(
        &result,
        &format!("(({STR_MEMBER} ILIKE '%' || $_0_$|| '%'))"),
        &["alpha"],
    );
}

#[test]
fn test_starts_with_compound_member() {
    let result = build(indoc! {"
        filters:
          - dimension: orders.note_tagged
            operator: startsWith
            values:
              - alpha
    "});
    assert_filter(
        &result,
        &format!("(({STR_MEMBER} ILIKE $_0_$|| '%'))"),
        &["alpha"],
    );
}

// ── date ────────────────────────────────────────────────────────────────────

#[test]
fn test_in_date_range_compound_member() {
    let result = build(indoc! {"
        filters:
          - dimension: orders.shifted_at
            operator: inDateRange
            values:
              - '2024-01-01T00:00:00.000'
              - '2024-01-31T23:59:59.999'
    "});
    assert_filter(
        &result,
        &format!("({TS_MEMBER} >= $_0_$::timestamptz AND {TS_MEMBER} <= $_1_$::timestamptz)"),
        &["2024-01-01T00:00:00.000", "2024-01-31T23:59:59.999"],
    );
}

// ── HAVING: measures ────────────────────────────────────────────────────────

// The reported model: unparenthesized this is `sum(...) IS NOT NULL = ...`,
// which Trino and Athena reject.
#[test]
fn test_equals_compound_measure() {
    let result = build(indoc! {"
        filters:
          - member: orders.total_is_set
            operator: equals
            values:
              - \"true\"
    "});
    assert_filter(
        &result,
        r#"((sum("orders".amount) IS NOT NULL) = $_0_$::boolean)"#,
        &["true"],
    );
}

#[test]
fn test_equals_comparison_measure() {
    let result = build(indoc! {"
        filters:
          - member: orders.total_over_150
            operator: equals
            values:
              - \"true\"
    "});
    assert_filter(
        &result,
        r#"((sum("orders".amount) > 150) = $_0_$::boolean)"#,
        &["true"],
    );
}

// ── members that must NOT be wrapped ────────────────────────────────────────

#[test]
fn test_atomic_dimension_is_not_wrapped() {
    let result = build(indoc! {"
        filters:
          - dimension: orders.amount
            operator: equals
            values:
              - \"100\"
    "});
    assert_filter(&result, r#"("orders".amount = $_0_$::numeric)"#, &["100"]);
}

#[test]
fn test_aggregate_measure_is_not_wrapped() {
    let result = build(indoc! {"
        filters:
          - member: orders.total
            operator: gt
            values:
              - \"100\"
    "});
    assert_filter(
        &result,
        r#"(sum("orders".amount) > $_0_$::numeric)"#,
        &["100"],
    );
}

// ── trailing line comment ───────────────────────────────────────────────────

// The closing parenthesis, not precedence, is what the comment threatens, so an
// atomic member needs the wrap too — with the parenthesis on its own line.
#[test]
fn test_atomic_member_ending_in_line_comment() {
    let result = build(indoc! {"
        filters:
          - dimension: orders.amount_commented
            operator: gt
            values:
              - \"50\"
    "});
    assert_filter(
        &result,
        "((\"orders\".amount -- as is\n) > $_0_$::numeric)",
        &["50"],
    );
}
