use crate::planner::collectors::collect_multiplied_measures;
use crate::planner::{JoinTree, MemberSymbol};
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;
use std::rc::Rc;

fn ctx() -> TestContext {
    TestContext::new(MockSchema::from_yaml_file(
        "common/integration_calculated_multi_fact.yaml",
    ))
    .unwrap()
}

/// The measure and the join tree the planner would classify it against: the
/// query's join hints plus the measure's own, the way
/// `full_key_aggregate_measures` builds them.
fn measure_and_join(query: &str) -> (Rc<MemberSymbol>, Rc<JoinTree>) {
    let ctx = ctx();
    let props = ctx.create_query_properties(query).unwrap();
    let measure = props.measures()[0].clone();
    let groups = props
        .compute_join_multi_fact_groups_with_measures(std::slice::from_ref(&measure))
        .unwrap();
    let join = groups.groups()[0].0.clone();
    (measure, join)
}

fn collected_names(measure: &Rc<MemberSymbol>, join: &Rc<JoinTree>) -> Vec<String> {
    let mut names = collect_multiplied_measures(measure, join)
        .unwrap()
        .iter()
        .map(|item| item.measure.full_name())
        .collect::<Vec<_>>();
    names.sort();
    names
}

/// Multiplied by the join and reaching past its own cube: the measure-join
/// subquery would drop the aggregation around its components, so the components
/// are collected in its place.
#[test]
fn test_calculated_measure_decomposed_when_multiplied() {
    let (measure, join) = measure_and_join(indoc! {"
        measures:
          - payments.rate_vs_commission
        dimensions:
          - payment_meta.value
    "});

    assert!(join.is_multiplied(&measure.cube_name()));
    assert_eq!(
        collected_names(&measure, &join),
        vec!["payments.commissioned_value", "payments.converted_value"]
    );
}

/// The same measure against a join that multiplies nothing. It is read off a
/// leaf-measure query that keeps its aggregate, so it stays whole.
#[test]
fn test_calculated_measure_kept_whole_when_not_multiplied() {
    let (measure, join) = measure_and_join(indoc! {"
        measures:
          - payments.rate_vs_commission
        dimensions:
          - payments.status
    "});

    assert!(!join.is_multiplied(&measure.cube_name()));
    assert_eq!(
        collected_names(&measure, &join),
        vec!["payments.rate_vs_commission"]
    );
}

/// Components on the measure's own cube need no measure-join subquery, so being
/// multiplied is not on its own a reason to split them.
#[test]
fn test_calculated_measure_kept_whole_when_it_reaches_no_other_cube() {
    let (measure, join) = measure_and_join(indoc! {"
        measures:
          - payments.success_rate
        dimensions:
          - payment_meta.value
    "});

    assert!(join.is_multiplied(&measure.cube_name()));
    assert_eq!(
        collected_names(&measure, &join),
        vec!["payments.success_rate"]
    );
}

/// A dimension of another cube cannot be read once the components have moved
/// out, so the measure stays whole even though it is multiplied and reaches.
#[test]
fn test_calculated_measure_kept_whole_when_it_reaches_past_its_components() {
    let (measure, join) = measure_and_join(indoc! {"
        measures:
          - payments.amount_over_fx
        dimensions:
          - payment_meta.value
    "});

    assert!(join.is_multiplied(&measure.cube_name()));
    assert_eq!(
        collected_names(&measure, &join),
        vec!["payments.amount_over_fx"]
    );
}

/// Same for a raw read of the cube's own table, which travels through
/// `get_cube_refs` rather than through the dependency tree.
#[test]
fn test_calculated_measure_kept_whole_when_it_reads_its_own_cube_directly() {
    let (measure, join) = measure_and_join(indoc! {"
        measures:
          - payments.converted_per_max_amount
        dimensions:
          - payment_meta.value
    "});

    assert!(join.is_multiplied(&measure.cube_name()));
    assert_eq!(
        collected_names(&measure, &join),
        vec!["payments.converted_per_max_amount"]
    );
}
