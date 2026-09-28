use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const SEED: &str = "integration_chained_subquery_tables.sql";

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_chained_subquery.yaml");
    TestContext::new(schema).unwrap()
}

// JS sql-generation-logic.test.ts:683 — "where filter with operators OR & AND".
// Schema: visitors → visitor_checkins (sub_query dim `cards_count` over cards).
// Query: count() over visitors + cross-cube dim visitor_checkins.cards_count,
// filtered by OR(AND(source, cards_count), AND(source, cards_count)).
//
// Triggers the multiplied-measure pipeline (visitor_count is in visitors but
// the query selects visitor_checkins.cards_count which goes through DSQ).
// In master the measure CTE projected `visitors.id` with a GROUP BY pk; under
// the new pipeline the GROUP BY disappeared and Postgres rejected the SELECT.
#[tokio::test(flavor = "multi_thread")]
async fn test_or_and_filter_with_cross_cube_sub_query_dim() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - visitors.visitor_count
        dimensions:
          - visitors.source
          - visitor_checkins.cards_count
        filters:
          - or:
              - and:
                  - dimension: visitors.source
                    operator: equals
                    values:
                      - some
                  - dimension: visitor_checkins.cards_count
                    operator: equals
                    values:
                      - "0"
              - and:
                  - dimension: visitors.source
                    operator: equals
                    values:
                      - google
                  - dimension: visitor_checkins.cards_count
                    operator: equals
                    values:
                      - "1"
        order:
          - id: visitors.visitor_count
            desc: true
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
