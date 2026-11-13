//! The measure groups query analysis returns.

use crate::planner::{JoinTree, QueryAnalysis};
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;
use std::rc::Rc;

fn analyze(query: &str) -> Rc<QueryAnalysis> {
    let ctx = TestContext::new(MockSchema::from_yaml_file(
        "common/integration_measure_legs.yaml",
    ))
    .unwrap();
    QueryAnalysis::try_new(ctx.create_query_properties(query).unwrap()).unwrap()
}

// A join as `root>joined,joined`.
fn tree(join: &JoinTree) -> String {
    let joined = join
        .joins()
        .iter()
        .map(|item| item.cube().cube_id().to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!("{}>{joined}", join.root().cube_id())
}

// Each regular group as `join: measures`, each multiplied one as
// `key cube @ join: measures`.
fn groups(analysis: &QueryAnalysis) -> (Vec<String>, Vec<String>) {
    let groups = analysis.measure_groups();
    let names = |measures: &[Rc<crate::planner::MemberSymbol>]| {
        measures
            .iter()
            .map(|m| m.full_name())
            .collect::<Vec<_>>()
            .join(", ")
    };
    (
        groups
            .regular()
            .iter()
            .map(|g| format!("{}: {}", tree(g.join()), names(g.measures())))
            .collect(),
        groups
            .multiplied()
            .iter()
            .map(|g| {
                format!(
                    "{} @ {}: {}",
                    g.key_cube(),
                    tree(g.join()),
                    names(g.measures())
                )
            })
            .collect(),
    )
}

#[test]
fn a_query_of_one_join_is_simple() {
    let analysis = analyze(indoc! {"
        measures:
          - orders.total_amount
        dimensions:
          - customers.city
    "});

    assert!(analysis.is_simple());
}

#[test]
fn measures_of_several_facts_are_regular_groups_of_their_joins() {
    let analysis = analyze(indoc! {"
        measures:
          - orders.total_amount
          - returns.total_refund
          - items.total_qty
        dimensions:
          - customers.city
    "});

    assert!(!analysis.is_simple());
    assert_eq!(
        groups(&analysis),
        (
            vec![
                "customers>orders: orders.total_amount".to_string(),
                "customers>orders,returns: returns.total_refund".to_string(),
                "customers>orders,items: items.total_qty".to_string(),
            ],
            vec![],
        )
    );
}

// Returns multiply the rows of orders: their count is taken as a distinct
// count of keys, their sum is aggregated once per key.
#[test]
fn a_multiplied_cube_is_a_multiplied_group_keyed_by_it() {
    let analysis = analyze(indoc! {"
        measures:
          - orders.total_amount
          - orders.count
        dimensions:
          - returns.reason
    "});

    assert_eq!(
        groups(&analysis),
        (
            vec!["orders>returns: orders.count".to_string()],
            vec!["orders @ orders>returns: orders.total_amount".to_string()],
        )
    );
}

// Each leaf is joined by the cubes of the measures it is reached through, and
// judged multiplied in that join: orders are not multiplied by their own join.
#[test]
fn the_leaves_of_a_calculated_measure_across_facts_are_grouped_by_their_joins() {
    let analysis = analyze(indoc! {"
        measures:
          - orders.amount_minus_refund
        dimensions:
          - customers.city
    "});

    assert_eq!(
        groups(&analysis),
        (
            vec![
                "customers>orders: orders.total_amount".to_string(),
                "customers>orders,returns: returns.total_refund".to_string(),
            ],
            vec![],
        )
    );
}

// The leaves of a measure over a measure across facts are its aggregates.
#[test]
fn a_calculated_measure_over_one_across_facts_is_split_into_its_aggregates() {
    let analysis = analyze(indoc! {"
        measures:
          - orders.amount_minus_refund_plus_fee
        dimensions:
          - customers.city
    "});

    assert_eq!(
        groups(&analysis),
        (
            vec![
                "customers>orders: orders.total_amount, orders.total_fee".to_string(),
                "customers>orders,returns: returns.total_refund".to_string(),
            ],
            vec![],
        )
    );
}

// Read directly, returns are joined from themselves; read through orders, from
// orders: two joins, two columns.
#[test]
fn a_measure_read_over_two_joins_is_two_leaves() {
    let analysis = analyze(indoc! {"
        measures:
          - returns.total_refund
          - orders.amount_minus_refund
        dimensions:
          - returns.reason
    "});

    assert_eq!(
        groups(&analysis),
        (
            vec![
                "returns>: returns.total_refund".to_string(),
                "orders>returns: returns.total_refund~1".to_string(),
            ],
            vec!["orders @ orders>returns: orders.total_amount".to_string()],
        )
    );
}

// A distinct count survives the returns join, so its group folds into the
// wider one and the query is simple.
#[test]
fn a_leaf_immune_to_multiplication_folds_into_a_wider_join() {
    let analysis = analyze(indoc! {"
        measures:
          - orders.unique_customers
          - returns.total_refund
        dimensions:
          - customers.city
    "});

    assert!(analysis.is_simple());
    assert_eq!(
        groups(&analysis),
        (
            vec![
                "customers>orders,returns: orders.unique_customers, returns.total_refund"
                    .to_string()
            ],
            vec![],
        )
    );
}

// Every leaf aggregates over the measure's own join, orders in their distinct
// form, so the measure stays whole and the query is simple.
#[test]
fn a_calculated_measure_whose_leaves_fit_its_own_join_stays_whole() {
    let analysis = analyze(indoc! {"
        measures:
          - orders.count_minus_returns
        dimensions:
          - orders.status
    "});

    assert!(analysis.is_simple());
    assert_eq!(
        groups(&analysis),
        (
            vec!["orders>returns: orders.count, returns.count".to_string()],
            vec![],
        )
    );
}
