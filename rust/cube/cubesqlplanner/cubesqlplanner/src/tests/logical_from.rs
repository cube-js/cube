//! Tests for the FROM of a query: its relations, their order and the columns it provides.

use crate::logical_plan::*;
use crate::planner::collectors::collect_join_hints;
use crate::planner::planners::JoinTreeBuilder;
use crate::planner::{InputId, JoinTree, MemberSymbol};
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use std::rc::Rc;

fn context() -> TestContext {
    TestContext::new(MockSchema::from_yaml_file(
        "common/subquery_dimensions.yaml",
    ))
    .unwrap()
}

// Sales joined to Customers.
fn sales_and_customers(context: &TestContext) -> Rc<JoinTree> {
    let mut hints =
        collect_join_hints(&context.create_measure("Sales.totalAmount").unwrap()).unwrap();
    hints.extend(&collect_join_hints(&context.create_dimension("Customers.id").unwrap()).unwrap());
    JoinTreeBuilder::new(context.query_tools().clone())
        .build_for_hints(&hints)
        .unwrap()
        .1
}

fn input(id: usize, dimensions: Vec<Rc<MemberSymbol>>) -> DimensionInput {
    DimensionInput {
        subquery: subquery_ref(id, dimensions),
        keys: vec![],
    }
}

fn subquery_ref(id: usize, dimensions: Vec<Rc<MemberSymbol>>) -> Rc<SubqueryRef> {
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

fn grid_input(id: usize, keys: Vec<Rc<MemberSymbol>>) -> DimensionInput {
    DimensionInput {
        subquery: subquery_ref(id, keys.clone()),
        keys,
    }
}

fn describe(from: &LogicalFrom) -> Vec<String> {
    let relation = |relation: &Relation| match relation {
        Relation::Cube { cube, .. } => cube.cube_id().to_string(),
        Relation::Subquery(input) => input.name().clone(),
        Relation::PreAggregation(_) => "pre-aggregation".to_string(),
        Relation::RawSql { alias, .. } => alias.clone(),
    };
    from.root()
        .into_iter()
        .map(relation)
        .chain(from.joins().iter().map(|join| {
            let on = match join.on() {
                JoinOn::Sql(_) => "sql",
                JoinOn::Keys {
                    null_safe: true, ..
                } => "null-safe keys",
                JoinOn::Keys { .. } => "keys",
            };
            format!("{} on {}", relation(join.relation()), on)
        }))
        .collect()
}

#[test]
fn inputs_join_right_after_their_cube_and_the_dimension_subquery_last() {
    let context = context();
    let spend = context.create_dimension("Customers.totalSpend").unwrap();
    let id = context.create_dimension("Customers.id").unwrap();
    let from = LogicalFrom::over_join(Some(sales_and_customers(&context)))
        .dimension_inputs(vec![input(0, vec![spend])])
        .grid_input(Some(grid_input(1, vec![id])))
        .build()
        .unwrap()
        .0;

    assert_eq!(
        describe(&from),
        vec![
            "Sales",
            "Customers on sql",
            "cte_0 on keys",
            "cte_1 on null-safe keys",
        ]
    );
}

#[test]
fn an_input_no_cube_of_the_join_owns_is_rejected() {
    let context = context();
    let total = context
        .create_dimension("CustomerOrders.customerId")
        .unwrap();
    let result = LogicalFrom::over_join(Some(sales_and_customers(&context)))
        .dimension_inputs(vec![input(0, vec![total])])
        .build();
    assert!(result.is_err());
}

#[test]
fn without_cubes_the_dimension_subquery_is_the_root() {
    let context = context();
    let id = context.create_dimension("Customers.id").unwrap();
    let from = LogicalFrom::over_join(None)
        .grid_input(Some(grid_input(1, vec![id.clone()])))
        .build()
        .unwrap()
        .0;
    assert_eq!(describe(&from), vec!["cte_1"]);

    assert!(LogicalFrom::over_join(None).build().unwrap().0.is_empty());
    assert!(LogicalFrom::over_join(None)
        .dimension_inputs(vec![input(0, vec![id])])
        .build()
        .is_err());
}

#[test]
fn a_join_key_is_one_column_provided_by_the_relation_joined_first() {
    let context = context();
    let spend = context.create_dimension("Customers.totalSpend").unwrap();
    let from = LogicalFrom::over_join(Some(sales_and_customers(&context)))
        .dimension_inputs(vec![input(0, vec![spend.clone()])])
        .grid_input(Some(grid_input(1, vec![spend.clone()])))
        .build()
        .unwrap()
        .0;

    let column = from.schema().column(&spend).unwrap();
    assert_eq!(column.provider, Provider::Input(InputId::new(0)));
}

#[test]
fn a_member_two_relations_provide_is_rejected() {
    let context = context();
    let spend = context.create_dimension("Customers.totalSpend").unwrap();
    let id = context.create_dimension("Customers.id").unwrap();
    // The dimension subquery is not joined by the member, so its copy may differ.
    let result = LogicalFrom::over_join(Some(sales_and_customers(&context)))
        .dimension_inputs(vec![input(0, vec![spend.clone()])])
        .grid_input(Some(DimensionInput {
            subquery: subquery_ref(1, vec![id.clone(), spend]),
            keys: vec![id],
        }))
        .build();
    assert!(result.is_err());
}

#[test]
fn merge_keys_are_provided_by_every_leg() {
    let context = context();
    let id = context.create_dimension("Customers.id").unwrap();
    let spend = context.create_dimension("Customers.totalSpend").unwrap();
    let merge = Merge::builder()
        .schema(
            LogicalSchema::default()
                .set_dimensions(vec![id.clone()])
                .into_rc(),
        )
        .subqueries(vec![
            subquery_ref(0, vec![id.clone(), spend.clone()]),
            subquery_ref(1, vec![id.clone()]),
        ])
        .build();
    let from = LogicalFrom::merge(Rc::new(merge)).unwrap();

    assert_eq!(
        from.schema().column(&id).unwrap().provider,
        Provider::MergeKey(vec![InputId::new(0), InputId::new(1)])
    );
    assert_eq!(
        from.schema().column(&spend).unwrap().provider,
        Provider::Input(InputId::new(0))
    );
}

#[test]
fn forwarding_rebuilds_the_columns() {
    let context = context();
    let id = context.create_dimension("Customers.id").unwrap();
    let spend = context.create_dimension("Customers.totalSpend").unwrap();
    let from = LogicalFrom::over_join(None)
        .grid_input(Some(grid_input(1, vec![id.clone()])))
        .build()
        .unwrap()
        .0;

    let forwarded = from
        .with_forwarded_input(&InputForwards::single(
            InputId::new(1),
            &subquery_ref(2, vec![id.clone(), spend.clone()]),
        ))
        .unwrap();

    assert_eq!(forwarded.referenced_inputs(), vec![InputId::new(2)]);
    assert_eq!(
        forwarded.schema().column(&spend).unwrap().provider,
        Provider::Input(InputId::new(2))
    );
}

#[test]
fn a_new_from_keeps_the_query_join() {
    let context = context();
    let join = sales_and_customers(&context);
    let query = Rc::new(
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
            .from(
                LogicalFrom::over_join(Some(join.clone()))
                    .build()
                    .unwrap()
                    .0,
            )
            .join(Some(join.clone()))
            .build(),
    );

    let replaced = query.reading_rows_of(LogicalFrom::empty()).unwrap();

    assert!(Rc::ptr_eq(replaced.join().as_ref().unwrap(), &join));
}

#[test]
fn a_join_key_of_a_cube_is_read_from_the_cube() {
    let context = context();
    let id = context.create_dimension("Customers.id").unwrap();
    let from = LogicalFrom::over_join(Some(sales_and_customers(&context)))
        .grid_input(Some(grid_input(1, vec![id.clone()])))
        .build()
        .unwrap()
        .0;

    assert!(matches!(
        from.schema().column(&id).unwrap().provider,
        Provider::Cube { .. }
    ));
}

#[test]
fn a_from_joining_one_subquery_twice_is_rejected() {
    let context = context();
    let spend = context.create_dimension("Customers.totalSpend").unwrap();
    let id = context.create_dimension("Customers.id").unwrap();
    let from = LogicalFrom::over_join(Some(sales_and_customers(&context)))
        .dimension_inputs(vec![input(0, vec![spend])])
        .grid_input(Some(grid_input(1, vec![id.clone()])))
        .build()
        .unwrap()
        .0;

    let forwarded = from.with_forwarded_input(&InputForwards::single(
        InputId::new(1),
        &subquery_ref(0, vec![id]),
    ));
    assert!(forwarded.is_err());
}

#[test]
fn forwards_added_in_either_order_reach_the_last_target() {
    let last = subquery_ref(3, vec![]);
    let mut forward_first = InputForwards::default();
    forward_first.add(InputId::new(1), &subquery_ref(2, vec![]));
    forward_first.add(InputId::new(2), &last);
    let mut forward_last = InputForwards::default();
    forward_last.add(InputId::new(2), &last);
    forward_last.add(InputId::new(1), &subquery_ref(2, vec![]));

    for forwards in [forward_first, forward_last] {
        assert_eq!(
            forwards.target(InputId::new(1)).unwrap().id(),
            InputId::new(3)
        );
        assert_eq!(
            forwards.target(InputId::new(2)).unwrap().id(),
            InputId::new(3)
        );
    }
}

#[test]
fn a_ref_already_reading_a_target_takes_the_target_schema() {
    let context = context();
    let id = context.create_dimension("Customers.id").unwrap();
    let widened = subquery_ref(2, vec![id]);
    let forwards = InputForwards::single(InputId::new(1), &widened);

    let collapsed = forwards.collapsed(&subquery_ref(2, vec![]));

    assert!(Rc::ptr_eq(&collapsed, &widened));
}
