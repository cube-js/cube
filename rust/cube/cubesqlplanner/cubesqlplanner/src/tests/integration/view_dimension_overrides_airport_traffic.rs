//! One fact blended with itself: a view reads flights as departures by their
//! origin and as arrivals by their destination, on one airport axis.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    TestContext::new(MockSchema::from_yaml_file(
        "common/view_dimension_overrides_airport_traffic.yaml",
    ))
    .unwrap()
}

const SEED: &str = "view_dimension_overrides_airport_traffic_tables.sql";

// JFK: 2 / 2, LHR: 1 / 2, SFO: 3 / 2.
#[tokio::test(flavor = "multi_thread")]
async fn test_departures_and_arrivals_by_airport() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - airport_traffic.departures_count
          - airport_traffic.arrivals_count
        dimensions:
          - airport_traffic.code
        order:
          - id: airport_traffic.code
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// SFO: 3 departures, 2 arrivals.
#[tokio::test(flavor = "multi_thread")]
async fn test_one_airport_by_filter() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - airport_traffic.departures_count
          - airport_traffic.arrivals_count
        filters:
          - member: airport_traffic.code
            operator: equals
            values: ["SFO"]
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// A coarser dimension of the airport: GB: 1 / 2, US: 5 / 4.
#[tokio::test(flavor = "multi_thread")]
async fn test_departures_and_arrivals_by_country() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - airport_traffic.departures_count
          - airport_traffic.arrivals_count
        dimensions:
          - airport_traffic.country
        order:
          - id: airport_traffic.country
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Grouped by one overridden dimension, filtered by another: US airports only,
// JFK: 2 / 2, SFO: 3 / 2.
#[tokio::test(flavor = "multi_thread")]
async fn test_group_by_airport_filter_by_country() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - airport_traffic.departures_count
          - airport_traffic.arrivals_count
        dimensions:
          - airport_traffic.code
        filters:
          - member: airport_traffic.country
            operator: equals
            values: ["US"]
        order:
          - id: airport_traffic.code
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// The same sum under both roles: out of GB 120 passengers, into GB 380.
#[tokio::test(flavor = "multi_thread")]
async fn test_passengers_out_and_in() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - airport_traffic.departures_passengers
          - airport_traffic.arrivals_passengers
        filters:
          - member: airport_traffic.country
            operator: equals
            values: ["GB"]
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// The busiest airport by arriving passengers: LHR, 380 in and 120 out.
#[tokio::test(flavor = "multi_thread")]
async fn test_top_airport_by_arriving_passengers() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - airport_traffic.departures_passengers
          - airport_traffic.arrivals_passengers
        dimensions:
          - airport_traffic.code
        order:
          - id: airport_traffic.arrivals_passengers
            desc: true
        limit: 1
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// No airports cube and no join: the airport is the flight's own column, origin
// for departures and destination for arrivals. JFK: 2 / 2, LHR: 1 / 2, SFO: 3 / 2.
#[tokio::test(flavor = "multi_thread")]
async fn test_own_columns_by_airport() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - route_ends.departures_count
          - route_ends.arrivals_count
        dimensions:
          - route_ends.airport
        order:
          - id: route_ends.airport
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert!(!sql.contains("vdo_airports"), "{sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// LHR: 1 departure, 2 arrivals.
#[tokio::test(flavor = "multi_thread")]
async fn test_own_columns_by_filter() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - route_ends.departures_count
          - route_ends.arrivals_count
        filters:
          - member: route_ends.airport
            operator: equals
            values: ["LHR"]
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
