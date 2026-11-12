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
    let groups = analysis.measure_groups().unwrap();
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

// Each leaf is judged multiplied in the join of its whole measure, and grouped
// by the join of the leaf alone.
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
            vec!["customers>orders,returns: returns.total_refund".to_string()],
            vec!["orders @ customers>orders: orders.total_amount".to_string()],
        )
    );
}
