use crate::planner::collectors::{collect_boundaries, BoundaryKind};
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    TestContext::new(
        MockSchema::from_yaml(indoc! {"
            cubes:
                - name: orders
                  sql: \"SELECT * FROM orders\"
                  dimensions:
                      - name: id
                        type: number
                        sql: id
                        primary_key: true
                      - name: created_at
                        type: time
                        sql: created_at
                      - name: lifetime_amount
                        type: number
                        sql: \"{CUBE.amount}\"
                        sub_query: true
                      - name: settled_at
                        type: time
                        sql: \"{CUBE.created_at} + {CUBE.lifetime_amount} * interval '1 day'\"
                        multi_stage: true
                  measures:
                      - name: amount
                        type: sum
                        sql: amount
                      - name: rolling_amount
                        type: sum
                        sql: amount
                        rolling_window:
                            trailing: 7 day
                      - name: amount_share
                        type: number
                        sql: \"{CUBE.amount} / {CUBE.rolling_amount}\"
        "})
        .unwrap(),
    )
    .unwrap()
}

fn kinds(symbol: &std::rc::Rc<crate::planner::MemberSymbol>) -> Vec<(BoundaryKind, String)> {
    collect_boundaries(symbol)
        .unwrap()
        .into_iter()
        .map(|b| (b.kind, b.symbol.full_name()))
        .collect()
}

#[test]
fn a_measure_over_a_rolling_window_crosses_it() {
    let ctx = create_context();
    let measure = ctx.create_measure("orders.amount_share").unwrap();
    assert_eq!(
        kinds(&measure),
        vec![(
            BoundaryKind::Cumulative,
            "orders.rolling_amount".to_string()
        )]
    );
}

#[test]
fn a_subquery_dimension_is_a_boundary_its_reads_are_walked() {
    let ctx = create_context();
    let dimension = ctx.create_dimension("orders.lifetime_amount").unwrap();
    assert_eq!(
        kinds(&dimension),
        vec![(
            BoundaryKind::SubQueryDimension,
            "orders.lifetime_amount".to_string()
        )]
    );
}

#[test]
fn a_time_dimension_over_a_multi_stage_dimension_stops_at_it() {
    let ctx = create_context();
    let time_dimension = ctx
        .create_time_dimension("orders.settled_at", Some("day"))
        .unwrap();
    assert_eq!(
        kinds(&time_dimension),
        vec![(BoundaryKind::MultiStage, "orders.settled_at".to_string())]
    );
}
