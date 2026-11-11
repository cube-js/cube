//! Tests for references to plan inputs in the nodes reading them.

use crate::logical_plan::*;
use crate::planner::symbols::transforms::inline_input_refs;
use crate::planner::{InputId, MemberSymbol, RefTarget};
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use std::rc::Rc;

struct Members {
    status: Rc<MemberSymbol>,
    total_amount: Rc<MemberSymbol>,
    count: Rc<MemberSymbol>,
    amount_per_count: Rc<MemberSymbol>,
}

fn members() -> Members {
    let context = TestContext::new(MockSchema::from_yaml_file(
        "common/pre_aggregation_matching_test.yaml",
    ))
    .unwrap();
    Members {
        status: context.create_dimension("orders.status").unwrap(),
        total_amount: context.create_measure("orders.total_amount").unwrap(),
        count: context.create_measure("orders.count").unwrap(),
        amount_per_count: context.create_measure("orders.amount_per_count").unwrap(),
    }
}

fn leg(
    id: usize,
    dimensions: &[&Rc<MemberSymbol>],
    measures: &[&Rc<MemberSymbol>],
) -> Rc<SubqueryRef> {
    let schema = LogicalSchema::default()
        .set_dimensions(dimensions.iter().map(|m| (*m).clone()).collect())
        .set_measures(measures.iter().map(|m| (*m).clone()).collect());
    Rc::new(
        SubqueryRef::builder()
            .id(InputId::new(id))
            .name(format!("cte_{id}"))
            .schema(schema.into_rc())
            .build(),
    )
}

fn query_over_legs(legs: Vec<Rc<SubqueryRef>>, schema: Rc<LogicalSchema>) -> Rc<Query> {
    let merge = Merge::builder()
        .schema(LogicalSchema::default().into_rc())
        .subqueries(legs)
        .build();
    Rc::new(
        Query::builder()
            .schema(schema)
            .filter(Rc::new(LogicalFilter::default()))
            .modifers(Rc::new(LogicalQueryModifiers {
                offset: None,
                limit: None,
                ungrouped: false,
                distinct: false,
                order_by: vec![],
            }))
            .source(QuerySource::new(Rc::new(merge)))
            .build(),
    )
}

fn root(query: Rc<Query>) -> Rc<RootQuery> {
    Rc::new(RootQuery::builder().subqueries(vec![]).query(query).build())
}

// The inputs `symbol` reads, in reading order; a key reads all it merges.
fn read_inputs(symbol: &Rc<MemberSymbol>) -> Vec<InputId> {
    if let MemberSymbol::Ref(reference) = symbol.as_ref() {
        let inputs = reference.inputs();
        if !inputs.is_empty() {
            return inputs;
        }
    }
    symbol
        .get_dependencies()
        .iter()
        .flat_map(read_inputs)
        .collect()
}

fn placed(placement: &InputPlacement, schema: &LogicalSchema) -> Rc<LogicalSchema> {
    Rc::new(schema.map_symbols(&|s| placement.place(s)).unwrap())
}

fn keyed_by(key: &Rc<MemberSymbol>) -> LogicalSchema {
    LogicalSchema::default().set_dimensions(vec![key.clone()])
}

#[test]
fn a_computed_measure_reads_its_dependencies_from_the_legs() {
    let m = members();
    let legs = vec![
        leg(1, &[&m.status], &[&m.total_amount]),
        leg(2, &[&m.status], &[&m.count]),
    ];
    let placement = InputPlacement::over_merge(&legs, &keyed_by(&m.status));

    let measure = placement.place(&m.amount_per_count).unwrap();
    assert!(!measure.is_reference());
    assert_eq!(
        read_inputs(&measure),
        vec![InputId::new(1), InputId::new(2)]
    );
    let key = placement.place(&m.status).unwrap();
    assert!(matches!(
        key.as_ref(),
        MemberSymbol::Ref(r) if matches!(r.target(), RefTarget::Key { .. })
    ));
    assert_eq!(read_inputs(&key), vec![InputId::new(1), InputId::new(2)]);
}

#[test]
fn forwarding_repoints_the_references() {
    let m = members();
    let legs = vec![
        leg(1, &[&m.status], &[&m.total_amount]),
        leg(2, &[&m.status], &[&m.count]),
    ];
    let schema = LogicalSchema::default()
        .set_dimensions(vec![m.status.clone()])
        .set_measures(vec![m.amount_per_count.clone()]);
    let schema = placed(
        &InputPlacement::over_merge(&legs, &keyed_by(&m.status)),
        &schema,
    );
    let plan = root(query_over_legs(legs, schema));
    plan.check_input_refs(&|_| false).unwrap();

    let to = leg(3, &[&m.status], &[&m.total_amount]);
    let forwarded = plan.forward(InputId::new(1), &to).unwrap();
    forwarded.check_input_refs(&|_| false).unwrap();
    let schema = forwarded.query().schema();
    assert_eq!(
        read_inputs(&schema.dimensions[0]),
        vec![InputId::new(3), InputId::new(2)]
    );
    assert_eq!(
        read_inputs(&schema.measures[0]),
        vec![InputId::new(3), InputId::new(2)]
    );
}

#[test]
fn forwarding_to_a_subquery_without_a_read_member_fails() {
    let m = members();
    let legs = vec![leg(1, &[&m.status], &[&m.total_amount])];
    let schema = LogicalSchema::default().set_measures(vec![m.total_amount.clone()]);
    let schema = placed(
        &InputPlacement::over_merge(&legs, &keyed_by(&m.status)),
        &schema,
    );
    let plan = root(query_over_legs(legs, schema));

    let to = leg(3, &[&m.status], &[&m.count]);
    assert!(plan.forward(InputId::new(1), &to).is_err());
}

#[test]
fn the_check_rejects_a_member_read_from_a_cube() {
    let m = members();
    let legs = vec![leg(1, &[&m.status], &[&m.total_amount])];
    let schema = LogicalSchema::default()
        .set_measures(vec![m.total_amount.clone()])
        .into_rc();
    let plan = root(query_over_legs(legs, schema));

    assert!(plan.check_input_refs(&|_| false).is_err());
}

#[test]
fn the_check_rejects_a_reference_to_an_input_not_read() {
    let m = members();
    let schema = LogicalSchema::default()
        .set_measures(vec![MemberSymbol::new_input_ref(
            &m.total_amount,
            InputId::new(2),
        )])
        .into_rc();
    let plan = root(query_over_legs(
        vec![leg(1, &[&m.status], &[&m.total_amount])],
        schema,
    ));

    assert!(plan.check_input_refs(&|_| false).is_err());
}

#[test]
fn inlining_restores_the_members_read() {
    let m = members();
    let legs = vec![
        leg(1, &[&m.status], &[&m.total_amount]),
        leg(2, &[&m.status], &[&m.count]),
    ];
    let resolved = InputPlacement::over_merge(&legs, &keyed_by(&m.status))
        .place(&m.amount_per_count)
        .unwrap();

    let inlined = inline_input_refs(&resolved).unwrap();
    assert!(read_inputs(&inlined).is_empty());
    assert_eq!(inlined.id(), m.amount_per_count.id());
}

#[test]
fn a_key_of_legs_collapsing_onto_one_reads_it_once() {
    let m = members();
    let legs = vec![
        leg(1, &[&m.status], &[&m.total_amount]),
        leg(2, &[&m.status], &[&m.count]),
    ];
    let schema = LogicalSchema::default().set_dimensions(vec![m.status.clone()]);
    let schema = placed(
        &InputPlacement::over_merge(&legs, &keyed_by(&m.status)),
        &schema,
    );
    let plan = root(query_over_legs(legs, schema));

    let to = leg(2, &[&m.status], &[&m.total_amount, &m.count]);
    let forwarded = plan.forward(InputId::new(1), &to).unwrap();
    forwarded.check_input_refs(&|_| false).unwrap();
    assert_eq!(
        read_inputs(&forwarded.query().schema().dimensions[0]),
        vec![InputId::new(2)]
    );
}
