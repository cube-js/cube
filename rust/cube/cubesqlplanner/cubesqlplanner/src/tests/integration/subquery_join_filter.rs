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

// `Orders` and `Customers` are joined only through `Shipments`, which the
// second filter brings in; both filters still reach the sub-query.
#[tokio::test(flavor = "multi_thread")]
async fn test_filters_joined_through_another_filters_cube_propagate() {
    let query = indoc! {r#"
        dimensions:
          - Orders.id
          - Orders.total
        filters:
          - member: Customers.name
            operator: equals
            values:
              - alice
          - member: Shipments.status
            operator: equals
            values:
              - sent
        order:
          - id: Orders.id
    "#};

    let sql = create_context().build_sql(query).unwrap();
    assert!(
        sql.matches("sq_customers").count() >= 2,
        "Expected the customers filter in the sub-query too:\n{sql}"
    );

    assert_matches_oracle(
        query,
        "SELECT o.id, o.amount FROM sq_orders o \
         JOIN sq_shipments s ON s.order_id = o.id \
         JOIN sq_customers c ON c.id = s.customer_id \
         WHERE c.name = 'alice' AND s.status = 'sent' ORDER BY o.id",
    )
    .await;
}

// The sub-query's measure lives on `Hub`, the only cube joining `Spoke` to the
// filter's `Tag`, so the filter reaches the sub-query: `total` of spoke 1 is
// 10, not 110.
#[tokio::test(flavor = "multi_thread")]
async fn test_filter_joined_through_measure_cube_propagates() {
    let query = indoc! {r#"
        dimensions:
          - Hub.kind
          - Spoke.total
        filters:
          - member: Tag.name
            operator: equals
            values:
              - a
        order:
          - id: Hub.kind
    "#};

    assert_matches_oracle(
        query,
        "SELECT h.kind, t.total FROM sq_hub h \
         JOIN sq_tag g ON g.id = h.tag_id \
         JOIN (SELECT h2.spoke_id, sum(h2.amount) total FROM sq_hub h2 \
               JOIN sq_tag g2 ON g2.id = h2.tag_id WHERE g2.name = 'a' GROUP BY 1) t \
           ON t.spoke_id = h.spoke_id \
         WHERE g.name = 'a' GROUP BY 1, 2 ORDER BY 1",
    )
    .await;
}

// `Purchases → Regions` reads `Users.purchases_count`, but the sub-query's own
// join is rooted at `Users` and reaches `Regions` directly, so the filter group
// still reaches the sub-query and counts only matching purchases.
#[tokio::test(flavor = "multi_thread")]
async fn test_filter_kept_when_sub_query_join_avoids_cyclic_edge() {
    let query = indoc! {r#"
        dimensions:
          - Users.id
          - Users.purchases_count
        filters:
          - or:
              - member: Regions.name
                operator: equals
                values:
                  - a
              - member: Channels.code
                operator: equals
                values:
                  - "1"
        order:
          - id: Users.id
    "#};

    assert_matches_oracle(
        query,
        "SELECT u.id, count(p.id) FROM sq_users u \
         JOIN sq_regions r ON r.id = u.region_id \
         LEFT JOIN sq_purchases p ON p.user_id = u.id \
         LEFT JOIN sq_channels c ON c.id = p.channel_id \
         WHERE r.name = 'a' OR c.code = 1 GROUP BY 1 ORDER BY 1",
    )
    .await;
}

// A multi-stage sub-query is planned on its leaf's join, which reaches
// `MsRegions` through `MsFacts`, not through the edge reading the dimension;
// the filter stays in the sub-query, so `ms_tot` of order 1 is 10, not 110.
#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_sub_query_keeps_filter_off_cyclic_edge() {
    let query = indoc! {r#"
        dimensions:
          - MsOrders.id
          - MsOrders.ms_tot
        filters:
          - member: MsRegions.name
            operator: equals
            values:
              - r
        order:
          - id: MsOrders.id
    "#};

    assert_matches_oracle(
        query,
        "SELECT o.id, (SELECT sum(f.amt) FROM sq_ms_facts f \
             JOIN sq_ms_regions fr ON fr.id = f.r_id \
             WHERE f.o_id = o.id AND fr.name = 'r') \
         FROM sq_ms_orders o JOIN sq_ms_regions r ON r.id = o.r_id \
         WHERE r.name = 'r' ORDER BY o.id",
    )
    .await;
}

// The `Orders.label` filter makes the sub-query need `Orders.total`, so filters
// are re-added one by one. `Customers` alone has no join path from `Orders`; it
// is kept on a later pass, once `Shipments` links them.
#[tokio::test(flavor = "multi_thread")]
async fn test_filter_needing_another_filter_kept_after_cycle() {
    let query = indoc! {r#"
        dimensions:
          - Orders.id
          - Orders.total
        filters:
          - member: Customers.name
            operator: equals
            values:
              - alice
          - member: Orders.label
            operator: set
          - member: Shipments.status
            operator: equals
            values:
              - sent
        order:
          - id: Orders.id
    "#};

    let sql = create_context().build_sql(query).unwrap();
    // Everything up to the sub-query's alias: its CTE and the joins before it.
    let sub_query = sql.split("AS \"Orders_total_subquery\"").next().unwrap();
    assert!(
        sub_query.contains("sq_customers") && !sub_query.contains("sq_labels"),
        "Expected the customers filter but not the label one in the sub-query:\n{sql}"
    );

    assert_matches_oracle(
        query,
        "SELECT o.id, o.amount FROM sq_orders o \
         JOIN sq_labels l ON l.amount = o.amount \
         JOIN sq_shipments s ON s.order_id = o.id \
         JOIN sq_customers c ON c.id = s.customer_id \
         WHERE c.name = 'alice' AND s.status = 'sent' ORDER BY o.id",
    )
    .await;
}

// `ReentryA.d`'s sub-query joins `ReentryC` on `e`, whose sub-query (no filters
// propagated) joins `ReentryX` on `d` again. That inner `d` has no filters, so
// planning ends and the filter group stays in the outer `d` sub-query.
#[tokio::test(flavor = "multi_thread")]
async fn test_sub_query_dimension_reentered_with_other_filters_keeps_them() {
    let query = indoc! {r#"
        dimensions:
          - ReentryA.id
          - ReentryA.d
        filters:
          - or:
              - member: ReentryC.name
                operator: equals
                values:
                  - x
              - member: ReentryB.kind
                operator: equals
                values:
                  - m
        order:
          - id: ReentryA.id
    "#};

    assert_matches_oracle(
        query,
        "SELECT a.id, (SELECT count(*) FROM sq_reentry_b b2 \
             JOIN sq_reentry_a a2 ON a2.id = b2.a_id \
             JOIN sq_reentry_c c2 ON c2.id = a2.c_id \
             WHERE b2.a_id = a.id AND (c2.name = 'x' OR b2.kind = 'm')) d \
         FROM sq_reentry_a a JOIN sq_reentry_c c ON c.id = a.c_id \
         LEFT JOIN sq_reentry_b b ON b.a_id = a.id \
         WHERE c.name = 'x' OR b.kind = 'm' GROUP BY 1 ORDER BY 1",
    )
    .await;
}

// `SelfJoined.total` reads a measure joined in only on `total` itself, so its
// sub-query needs it with no filters at all: a clear error, not a stack overflow.
#[test]
fn test_sub_query_dimension_needing_itself_is_a_user_error() {
    let query = indoc! {r#"
        dimensions:
          - SelfJoined.id
          - SelfJoined.total
    "#};

    let error = create_context().build_sql(query).unwrap_err();
    assert!(
        error.message.contains("can't be computed"),
        "Unexpected error: {}",
        error.message
    );
}
