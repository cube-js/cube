//! Tests for repointing plan inputs and checking the plan's links.

use crate::logical_plan::*;
use crate::planner::InputId;
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
        SubqueryBody::LeafMeasure(body),
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

    let SubqueryBody::LeafMeasure(reader) = &forwarded.subqueries()[2].body else {
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
        SubqueryBody::LeafMeasure(query_over(
            QuerySource::new(merge_of(&[]))
                .add_dimension_inputs(vec![dimension_input(1, DimensionInputKeys::PrimaryKeys)]),
        )),
    ));
    let inline = Rc::new(Subquery::new_inline(
        InputId::new(1),
        "dim_subquery".to_string(),
        SubqueryBody::LeafMeasure(query_over(QuerySource::new(merge_of(&[])))),
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

    let SubqueryBody::LeafMeasure(reader) = &forwarded.subqueries()[2].body else {
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
        SubqueryBody::LeafMeasure(query_over(QuerySource::new(merge_of(&[])))),
    ))
}

#[test]
fn unread_inline_subqueries_are_dropped_with_their_readers() {
    let reader = |id: usize, reads: usize| {
        Rc::new(Subquery::new(
            InputId::new(id),
            format!("cte_{id}"),
            SubqueryBody::LeafMeasure(query_over(
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
