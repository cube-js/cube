use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/calc_groups_cross_join.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "calc_groups_cross_join_tables.sql";

// FIXME: case-switch multi-stage dim children project disjoint
// column sets (each branch CTE has only its own source's columns —
// `source_a__product_category` vs `source_b__product_category`), so
// the FullKeyAggregate UNION ALL on `pk_aggregate_keys_source` fails
// with `column "source_b__product_category" does not exist`.
// Mirrors the JS `Calc-Groups › source product_category cross join`
// case. Children should project a unified dim column (the case
// result), not their sub-dep symbols, or the keys-source needs NULL-
// padding for missing columns.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn test_source_product_category_cross_join() {
    let ctx = create_context();

    let query = indoc! {r#"
        dimensions:
          - source.product_category
        order:
          - id: source.product_category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
