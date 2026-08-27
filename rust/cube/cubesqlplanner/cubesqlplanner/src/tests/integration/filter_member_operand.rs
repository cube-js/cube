//! A filter template splices the member's rendered SQL next to an operator of
//! its own. Left unparenthesized, a member whose own top-level operator binds
//! weaker re-associates: the filter operator captures only the tail of the
//! member expression. These tests check the rows rather than the SQL, because
//! the dangerous form of the mis-parse is valid SQL over a different row set —
//! the emitted text alone cannot tell that one from the intended reading. The
//! parentheses themselves are pinned per operator in
//! `tests/filter/member_operand.rs`.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const SEED: &str = "integration_basic_tables.sql";

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_basic.yaml");
    TestContext::new(schema).unwrap()
}

// `is_big_completed` = `amount > 100 AND (status = 'completed')` is false for
// orders 1, 3, 5, 7, 9 → count=5. The mis-parse reads `amount > 100 AND
// ((status = 'completed') = false)`, i.e. big-but-not-completed, which matches
// nothing — valid SQL, silently zero rows.
#[tokio::test(flavor = "multi_thread")]
async fn test_equals_false_on_and_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.count
        filters:
          - member: orders.is_big_completed
            operator: equals
            values:
              - "false"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// `is_ny_or_alice` = `city = 'New York' OR name LIKE 'Alice%'` is NULL only for
// Charlie Brown, whose city is NULL and whose name does not match → customer 3.
// The mis-parse reads `city = 'New York' OR (name LIKE 'Alice%') IS NULL`, whose
// right side is never NULL, so it degenerates to the New York customers.
#[tokio::test(flavor = "multi_thread")]
async fn test_not_set_on_or_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        dimensions:
          - customers.id
        filters:
          - member: customers.is_ny_or_alice
            operator: notSet
        order:
          - id: customers.id
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// `is_big` = `amount > 100`. Unparenthesized this renders
// `amount > 100 = $1::boolean`, which Postgres rejects outright — comparison
// operators do not associate — so reaching any rows at all is the assertion.
#[tokio::test(flavor = "multi_thread")]
async fn test_equals_true_on_comparison_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.count
        filters:
          - member: orders.is_big
            operator: equals
            values:
              - "true"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// The HAVING counterpart of the reported model: `sum(...) IS NOT NULL` compared
// to a boolean, which is what Trino and Athena reject. Postgres happens to parse
// the unparenthesized form the intended way, so this one cannot tell the two
// apart on rows — it guards the shape against a future regression that would
// reach further than precedence, and the operator matrix in
// `tests/filter/member_operand.rs` is what pins the parentheses here.
#[tokio::test(flavor = "multi_thread")]
async fn test_equals_on_calculated_boolean_measure() {
    let ctx = create_context();

    let query = indoc! {r#"
        dimensions:
          - orders.status
        filters:
          - member: orders.total_amount_is_set
            operator: equals
            values:
              - "true"
        order:
          - id: orders.status
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
