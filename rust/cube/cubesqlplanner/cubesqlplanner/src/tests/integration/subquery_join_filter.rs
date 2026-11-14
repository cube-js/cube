use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::{table_rows, TestContext};
use indoc::indoc;

const SEED: &str = "integration_subquery_join_filter_tables.sql";

// `Orders` joins `Labels` on its own sub_query dimension `Orders.total`, which
// propagates filters. A filter on `Orders.label` (= `Labels.label`) cannot be
// pushed into the `total` sub-query: evaluating it there needs `total` again.
fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_subquery_join_filter.yaml");
    TestContext::new(schema).unwrap()
}

#[cfg(feature = "integration-postgres")]
async fn execute_plain_sql(sql: &str) -> Vec<Vec<String>> {
    use crate::test_fixtures::test_utils::{integration_context, pg_service};

    let client = pg_service::connect_and_seed(SEED).await;
    let messages = client.simple_query(sql).await.unwrap();
    table_rows(&integration_context::format_simple_query_results(&messages))
}

#[cfg(not(feature = "integration-postgres"))]
async fn execute_plain_sql(_sql: &str) -> Vec<Vec<String>> {
    unreachable!("query results exist only with integration-postgres")
}

async fn assert_matches_oracle(query: &str, oracle_sql: &str) {
    let ctx = create_context();
    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        let expected = execute_plain_sql(oracle_sql).await;
        assert!(!expected.is_empty());
        assert_eq!(table_rows(&result), expected);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_set_filter_on_member_joined_by_sub_query_dimension() {
    let query = indoc! {r#"
        dimensions:
          - Orders.id
          - Orders.label
        filters:
          - member: Orders.label
            operator: set
        order:
          - id: Orders.id
    "#};

    assert_matches_oracle(
        query,
        "SELECT o.id, l.label FROM sq_orders o \
         JOIN sq_labels l ON l.amount = o.amount ORDER BY o.id",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_equals_filter_on_member_joined_by_sub_query_dimension() {
    let query = indoc! {r#"
        dimensions:
          - Orders.id
          - Orders.total
          - Orders.label
        filters:
          - member: Orders.label
            operator: equals
            values:
              - twenty
        order:
          - id: Orders.id
    "#};

    assert_matches_oracle(
        query,
        "SELECT o.id, o.amount, l.label FROM sq_orders o \
         JOIN sq_labels l ON l.amount = o.amount WHERE l.label = 'twenty' ORDER BY o.id",
    )
    .await;
}

// The filter on `Orders.id` needs no join and must still reach the sub-query
// next to the skipped `Orders.label` one; the outer WHERE hides a dropped one.
#[tokio::test(flavor = "multi_thread")]
async fn test_join_free_filter_still_propagates_next_to_skipped_one() {
    let query = indoc! {r#"
        dimensions:
          - Orders.id
          - Orders.total
          - Orders.label
        filters:
          - member: Orders.label
            operator: set
          - member: Orders.id
            operator: equals
            values:
              - "2"
        order:
          - id: Orders.id
    "#};

    let sql = create_context().build_sql(query).unwrap();
    let sub_query = sql
        .split("AS \"Orders_total_subquery\"")
        .next()
        .and_then(|head| head.rsplit("LEFT JOIN (").next())
        .unwrap();
    assert!(
        sub_query.contains("\"orders\".id ="),
        "Expected the id filter in the sub-query:\n{sql}"
    );
    assert!(
        !sub_query.contains("label"),
        "Expected no label filter in the sub-query:\n{sql}"
    );

    assert_matches_oracle(
        query,
        "SELECT o.id, o.amount, l.label FROM sq_orders o \
         JOIN sq_labels l ON l.amount = o.amount WHERE o.id = 2 ORDER BY o.id",
    )
    .await;
}
