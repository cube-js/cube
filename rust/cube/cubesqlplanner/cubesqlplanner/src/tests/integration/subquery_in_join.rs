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
