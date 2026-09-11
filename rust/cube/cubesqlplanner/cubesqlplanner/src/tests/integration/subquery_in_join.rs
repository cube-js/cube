use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const SEED: &str = "integration_subquery_in_join_tables.sql";

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_subquery_in_join.yaml");
    TestContext::new(schema).unwrap()
}

// JS sub-query-dimensions.test.ts:128 — "inserted at the right place of a join".
//
// Schema: A — base cube, B joins A (one-to-one), B joins C (one-to-many).
// B has a sub_query dim `foo_id` defined via A.max_foo_id. The B → C join
// condition references that sub_query dim (`{B.foo_id} > 3`), so the DSQ
// must be wired into the join chain BEFORE C — otherwise the filter on the
// join condition can't resolve and C rows leak through.
//
// Query: sum of C.important_value sliced by B.id. Expected:
//   B.id=100,101,102 → null (B.foo_id ≤ 3, B→C filtered out)
//   B.id=103 → 5.6 + 5.6 = 11.2
//   B.id=104 → 38.0 + 43.5 = 81.5
#[tokio::test(flavor = "multi_thread")]
async fn test_sub_query_dim_in_join_condition() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - C.important_value
        dimensions:
          - B.id
        order:
          - id: B.id
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// A multiplied measure reads its rows from the source cube joined beside the
// keys subquery, and a sub-query dimension the join condition names is joined
// there too. That dimension's join matches on the primary key, so the key has
// to resolve against the cube joined for the measure — resolving it anywhere
// that is not in scope at that point makes the whole select unrunnable.
//
// `B.sum_foo_id` multiplies over the one-to-many join to C, and the B → C
// condition names the sub_query dim `B.foo_id`, which is what puts the
// dimension sub-query into that same join.
#[tokio::test(flavor = "multi_thread")]
async fn test_sub_query_dim_join_key_for_multiplied_measure() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - B.sum_foo_id
        dimensions:
          - B.id
          - C.bar_id
        order:
          - id: B.id
    "#};

    let sql = ctx.build_sql(query).unwrap();

    let subquery_join = sql
        .split("AS \"B_foo_id_subquery\" ON")
        .nth(2)
        .unwrap_or_default()
        .split('\n')
        .next()
        .unwrap_or_default()
        .to_string();
    assert!(
        subquery_join.contains("\"b_key_b\".id"),
        "Expected the sub-query join to match on the key of the cube joined for the measure:\n{}",
        sql
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
