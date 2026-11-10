//! Tests for repointing plan inputs and checking the plan's links.

use crate::logical_plan::*;
use crate::planner::planners::CommonUtils;
use crate::planner::{CubeId, InputId, MemberSymbol};
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use std::rc::Rc;

fn subquery_ref(id: usize) -> Rc<SubqueryRef> {
    Rc::new(
        SubqueryRef::builder()
            .id(InputId::new(id))
            .name(format!("cte_{id}"))
            .schema(LogicalSchema::default().into_rc())
            .build(),
    )
}

fn query_over(source: Rc<QuerySource>) -> Rc<Query> {
    Rc::new(
        Query::builder()
            .schema(LogicalSchema::default().into_rc())
            .filter(Rc::new(LogicalFilter::default()))
            .modifers(Rc::new(LogicalQueryModifiers {
                offset: None,
                limit: None,
                ungrouped: false,
                distinct: false,
                order_by: vec![],
            }))
            .source(source)
            .build(),
    )
}

fn merge_of(ids: &[usize]) -> Rc<Merge> {
    Rc::new(
        Merge::builder()
            .schema(LogicalSchema::default().into_rc())
            .subqueries(ids.iter().map(|id| subquery_ref(*id)).collect())
            .build(),
    )
}

/// A subquery reading the subqueries `reads` through a merge.
fn subquery(id: usize, reads: &[usize]) -> Rc<Subquery> {
    let body = query_over(QuerySource::new(merge_of(reads)));
    Rc::new(Subquery::new(
        InputId::new(id),
        format!("cte_{id}"),
        SubqueryBody::Query(body),
    ))
}

fn root(subqueries: Vec<Rc<Subquery>>, query: Rc<Query>) -> Rc<RootQuery> {
    Rc::new(
        RootQuery::builder()
            .subqueries(subqueries)
            .query(query)
            .build(),
    )
}

fn root_merge_ids(root: &RootQuery) -> Vec<InputId> {
    let InputBase::Merge(merge) = root.query().source().base() else {
        panic!("the root query must read a merge");
    };
    merge.subqueries().iter().map(|r| r.id()).collect()
}

#[test]
fn forward_collapses_merge_legs_landing_on_one_subquery() {
    let plan = root(
        vec![subquery(0, &[]), subquery(1, &[]), subquery(2, &[])],
        query_over(QuerySource::new(merge_of(&[1, 0, 2]))),
    );

    let to = subquery_ref(0);
    let forwarded = plan.forward(InputId::new(2), &to).unwrap();

    assert_eq!(
        root_merge_ids(&forwarded),
        vec![InputId::new(1), InputId::new(0)],
        "the leg forwarded onto #0 must collapse into the one already there"
    );
    let InputBase::Merge(merge) = forwarded.query().source().base() else {
        unreachable!()
    };
    assert!(
        Rc::ptr_eq(&merge.subqueries()[1], &to),
        "the collapsed leg must read through the forwarded ref, not the older one"
    );
}

#[test]
fn forward_leaves_merges_not_reading_the_source_alone() {
    let plan = root(
        vec![subquery(0, &[]), subquery(1, &[]), subquery(2, &[0])],
        query_over(QuerySource::new(merge_of(&[1, 2]))),
    );

    let to = subquery_ref(0);
    let forwarded = plan.forward(InputId::new(1), &to).unwrap();

    let SubqueryBody::Query(reader) = &forwarded.subqueries()[2].body else {
        panic!("the reader must stay a leaf");
    };
    let InputBase::Merge(merge) = reader.source().base() else {
        panic!("the reader must read a merge");
    };
    assert!(
        !Rc::ptr_eq(&merge.subqueries()[0], &to),
        "a merge reading only the target must keep its own ref"
    );
}

fn dimension_input(id: usize, keys_kind: DimensionInputKeys) -> DimensionInput {
    DimensionInput {
        subquery: subquery_ref(id),
        keys: vec![],
        keys_kind,
    }
}

#[test]
fn forward_reaches_every_dimension_input() {
    let source = QuerySource::new(merge_of(&[])).add_dimension_inputs(vec![
        dimension_input(0, DimensionInputKeys::PrimaryKeys),
        dimension_input(1, DimensionInputKeys::Grid),
    ]);
    let plan = root(
        vec![subquery(0, &[]), subquery(1, &[]), subquery(2, &[])],
        query_over(source),
    );

    let forwarded = plan.forward(InputId::new(1), &subquery_ref(2)).unwrap();

    let ids = forwarded
        .query()
        .source()
        .dimension_inputs()
        .iter()
        .map(|input| input.subquery.id())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![InputId::new(0), InputId::new(2)]);
}

#[test]
fn integrity_rejects_a_dimension_input_defined_after_its_reader() {
    let reader = Rc::new(Subquery::new(
        InputId::new(0),
        "cte_0".to_string(),
        SubqueryBody::Query(query_over(
            QuerySource::new(merge_of(&[]))
                .add_dimension_inputs(vec![dimension_input(1, DimensionInputKeys::PrimaryKeys)]),
        )),
    ));
    let inline = Rc::new(Subquery::new_inline(
        InputId::new(1),
        "dim_subquery".to_string(),
        SubqueryBody::Query(query_over(QuerySource::new(merge_of(&[])))),
    ));
    let plan = root(
        vec![reader, inline],
        query_over(QuerySource::new(merge_of(&[0]))),
    );
    assert!(plan.check_integrity().is_err());
}

#[test]
fn forward_reaches_readers_inside_subqueries() {
    let plan = root(
        vec![subquery(0, &[]), subquery(1, &[]), subquery(2, &[0])],
        query_over(QuerySource::new(merge_of(&[2]))),
    );

    let forwarded = plan.forward(InputId::new(0), &subquery_ref(1)).unwrap();

    let SubqueryBody::Query(reader) = &forwarded.subqueries()[2].body else {
        panic!("the reader must stay a leaf");
    };
    let InputBase::Merge(merge) = reader.source().base() else {
        panic!("the reader must read a merge");
    };
    assert_eq!(merge.subqueries()[0].id(), InputId::new(1));
}

#[test]
fn integrity_accepts_inputs_defined_before_their_readers() {
    let plan = root(
        vec![subquery(0, &[]), subquery(1, &[0])],
        query_over(QuerySource::new(merge_of(&[0, 1]))),
    );
    plan.check_integrity().unwrap();
}

#[test]
fn integrity_rejects_a_read_of_an_undefined_input() {
    let plan = root(
        vec![subquery(0, &[])],
        query_over(QuerySource::new(merge_of(&[5]))),
    );
    assert!(plan.check_integrity().is_err());
}

#[test]
fn integrity_rejects_a_read_before_the_definition() {
    let plan = root(
        vec![subquery(0, &[1]), subquery(1, &[])],
        query_over(QuerySource::new(merge_of(&[0]))),
    );
    assert!(plan.check_integrity().is_err());
}

#[test]
fn integrity_rejects_a_subquery_defined_twice() {
    let plan = root(
        vec![subquery(0, &[]), subquery(0, &[])],
        query_over(QuerySource::new(merge_of(&[0]))),
    );
    assert!(plan.check_integrity().is_err());
}

fn inline_subquery(id: usize) -> Rc<Subquery> {
    Rc::new(Subquery::new_inline(
        InputId::new(id),
        format!("dim_{id}"),
        SubqueryBody::Query(query_over(QuerySource::new(merge_of(&[])))),
    ))
}

#[test]
fn unread_inline_subqueries_are_dropped_with_their_readers() {
    let reader = |id: usize, reads: usize| {
        Rc::new(Subquery::new(
            InputId::new(id),
            format!("cte_{id}"),
            SubqueryBody::Query(query_over(
                QuerySource::new(merge_of(&[])).add_dimension_inputs(vec![dimension_input(
                    reads,
                    DimensionInputKeys::PrimaryKeys,
                )]),
            )),
        ))
    };
    // dim_2 lost its reader, as when its leaf is folded into another one; an
    // unread CTE stays, and so does what it reads.
    let plan = root(
        vec![
            inline_subquery(0),
            reader(1, 0),
            inline_subquery(2),
            inline_subquery(3),
            reader(4, 3),
        ],
        query_over(QuerySource::new(merge_of(&[1]))),
    );

    let pruned = plan.without_unread_inline_subqueries();

    let ids = pruned
        .subqueries()
        .iter()
        .map(|subquery| subquery.id)
        .collect::<Vec<_>>();
    assert_eq!(
        ids,
        vec![
            InputId::new(0),
            InputId::new(1),
            InputId::new(3),
            InputId::new(4)
        ]
    );
}

fn multiplied_subquery(keys: usize, values: Option<usize>, dimensions: &[usize]) -> Rc<Subquery> {
    let schema = MockSchema::from_yaml_file("common/subquery_dimensions.yaml");
    let context = TestContext::new(schema).unwrap();
    let pk_cube = CommonUtils::new(context.query_tools().clone())
        .cube_from_path(&CubeId::cube("Customers"))
        .unwrap();
    let aggregate = AggregateMultipliedSubquery {
        schema: LogicalSchema::default().into_rc(),
        keys: subquery_ref(keys),
        pk_cube,
        primary_keys_dimensions: vec![],
        measure_values: values.map(subquery_ref),
        dimension_inputs: dimensions
            .iter()
            .map(|id| dimension_input(*id, DimensionInputKeys::PrimaryKeys))
            .collect(),
        evaluation_context: None,
    };
    Rc::new(Subquery::new(
        InputId::new(9),
        "cte_9".to_string(),
        SubqueryBody::AggregateMultipliedSubquery(Rc::new(aggregate)),
    ))
}

#[test]
fn forward_reaches_every_input_of_a_multiplied_subquery() {
    let plan = root(
        vec![
            inline_subquery(0),
            inline_subquery(1),
            inline_subquery(2),
            inline_subquery(3),
            multiplied_subquery(0, Some(1), &[2]),
        ],
        query_over(QuerySource::new(merge_of(&[9]))),
    );

    let forwarded = plan
        .forward(InputId::new(1), &subquery_ref(3))
        .unwrap()
        .forward(InputId::new(2), &subquery_ref(3))
        .unwrap();

    let SubqueryBody::AggregateMultipliedSubquery(aggregate) = &forwarded.subqueries()[4].body
    else {
        panic!("the reader must stay a multiplied subquery");
    };
    assert_eq!(aggregate.keys.id(), InputId::new(0));
    assert_eq!(
        aggregate.measure_values.as_ref().unwrap().id(),
        InputId::new(3)
    );
    assert_eq!(aggregate.dimension_inputs[0].subquery.id(), InputId::new(3));
}

#[test]
fn integrity_rejects_keys_defined_after_their_multiplied_subquery() {
    let plan = root(
        vec![multiplied_subquery(0, None, &[]), inline_subquery(0)],
        query_over(QuerySource::new(merge_of(&[9]))),
    );
    assert!(plan.check_integrity().is_err());
}

#[test]
fn keys_and_values_go_with_a_multiplied_subquery_replaced_by_a_query() {
    let aggregate = multiplied_subquery(0, Some(1), &[]);
    let plan = root(
        vec![inline_subquery(0), inline_subquery(1), aggregate.clone()],
        query_over(QuerySource::new(merge_of(&[9]))),
    );
    assert_eq!(
        plan.without_unread_inline_subqueries().subqueries().len(),
        3
    );

    let replaced = aggregate.with_body(SubqueryBody::Query(query_over(QuerySource::new(
        merge_of(&[]),
    ))));
    let plan = root(
        vec![inline_subquery(0), inline_subquery(1), replaced],
        query_over(QuerySource::new(merge_of(&[9]))),
    );
    let ids = plan
        .without_unread_inline_subqueries()
        .subqueries()
        .iter()
        .map(|subquery| subquery.id)
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![InputId::new(9)]);
}

fn keys_grid(id: usize, sources: &[usize]) -> Rc<Subquery> {
    let grid = KeysGrid::builder()
        .schema(LogicalSchema::default().into_rc())
        .sources(sources.iter().map(|id| subquery_ref(*id)).collect())
        .build();
    Rc::new(Subquery::new_inline(
        InputId::new(id),
        format!("cte_{id}_keys"),
        SubqueryBody::KeysGrid(Rc::new(grid)),
    ))
}

fn lookup_calculation(id: usize, grid: usize, legs: &[usize]) -> Rc<Subquery> {
    let lookup = GridLookup::builder()
        .grid(subquery_ref(grid))
        .legs(
            legs.iter()
                .map(|id| LookupLeg::new(subquery_ref(*id), &LogicalSchema::default()))
                .collect(),
        )
        .build();
    let calculation = MultiStageMeasureCalculation::builder()
        .schema(LogicalSchema::default().into_rc())
        .is_ungrouped(false)
        .calculation_type(MultiStageCalculationType::Aggregate)
        .window_function_to_use(MultiStageCalculationWindowFunction::None)
        .source(Rc::new(lookup).into())
        .build();
    Rc::new(Subquery::new(
        InputId::new(id),
        format!("cte_{id}"),
        SubqueryBody::MultiStageMeasureCalculation(Rc::new(calculation)),
    ))
}

#[test]
fn forward_reaches_the_grid_sources_and_the_lookup_legs() {
    let plan = root(
        vec![
            subquery(0, &[]),
            subquery(1, &[]),
            subquery(2, &[]),
            keys_grid(3, &[1, 2]),
            subquery(4, &[]),
            lookup_calculation(5, 3, &[0, 4]),
        ],
        query_over(QuerySource::new(merge_of(&[5]))),
    );
    plan.check_integrity().unwrap();

    let forwarded = plan
        .forward(InputId::new(2), &subquery_ref(1))
        .unwrap()
        .forward(InputId::new(0), &subquery_ref(4))
        .unwrap();

    let SubqueryBody::KeysGrid(grid) = &forwarded.subqueries()[3].body else {
        panic!("the grid must stay a keys grid");
    };
    let sources = grid.sources().iter().map(|r| r.id()).collect::<Vec<_>>();
    assert_eq!(sources, vec![InputId::new(1)]);
    let SubqueryBody::MultiStageMeasureCalculation(calculation) = &forwarded.subqueries()[5].body
    else {
        panic!("the reader must stay a measure calculation");
    };
    let CalculationSource::GridLookup(lookup) = calculation.source() else {
        panic!("the calculation must read a grid lookup");
    };
    assert_eq!(lookup.grid().id(), InputId::new(3));
    let legs = lookup
        .legs()
        .iter()
        .map(|leg| leg.subquery().id())
        .collect::<Vec<_>>();
    assert_eq!(legs, vec![InputId::new(4)]);
}

#[test]
fn a_grid_read_by_a_lookup_is_kept() {
    let plan = root(
        vec![
            subquery(0, &[]),
            keys_grid(1, &[0]),
            subquery(2, &[]),
            lookup_calculation(3, 1, &[2]),
        ],
        query_over(QuerySource::new(merge_of(&[3]))),
    );

    let pruned = plan.without_unread_inline_subqueries();

    assert!(pruned.subqueries().iter().any(|s| s.id == InputId::new(1)));
}

fn subquery_ref_with_dimensions(id: usize, dimensions: Vec<Rc<MemberSymbol>>) -> Rc<SubqueryRef> {
    Rc::new(
        SubqueryRef::builder()
            .id(InputId::new(id))
            .name(format!("cte_{id}"))
            .schema(
                LogicalSchema::default()
                    .set_dimensions(dimensions)
                    .into_rc(),
            )
            .build(),
    )
}

// Legs landing on one subquery keep their keys: the leg left joins by all of them.
#[test]
fn legs_collapsing_onto_one_subquery_keep_their_keys() {
    let context = TestContext::new(MockSchema::from_yaml_file(
        "common/subquery_dimensions.yaml",
    ))
    .unwrap();
    let id = context.create_dimension("Customers.id").unwrap();
    let spend = context.create_dimension("Customers.totalSpend").unwrap();
    let grid_schema = LogicalSchema::default()
        .set_dimensions(vec![id.clone(), spend.clone()])
        .into_rc();
    let coarse = subquery_ref_with_dimensions(1, vec![id.clone()]);
    let fine = subquery_ref_with_dimensions(2, vec![id.clone(), spend.clone()]);
    let wide = subquery_ref_with_dimensions(3, vec![id.clone(), spend.clone()]);
    let lookup = Rc::new(
        GridLookup::builder()
            .grid(Rc::new(
                SubqueryRef::builder()
                    .id(InputId::new(0))
                    .name("cte_0_keys".to_string())
                    .schema(grid_schema.clone())
                    .build(),
            ))
            .legs(vec![
                LookupLeg::new(coarse, &grid_schema),
                LookupLeg::new(fine, &grid_schema),
            ])
            .build(),
    );

    let kept = lookup
        .clone()
        .with_forwarded_input(InputId::new(1), &wide)
        .unwrap();
    assert_eq!(kept.legs()[0].subquery().id(), InputId::new(3));
    assert_eq!(kept.legs()[0].on().len(), 1);

    let collapsed = kept.with_forwarded_input(InputId::new(2), &wide).unwrap();
    let legs = collapsed.legs();
    assert_eq!(legs.len(), 1);
    assert_eq!(legs[0].subquery().id(), InputId::new(3));
    assert_eq!(legs[0].on().len(), 2);
}
