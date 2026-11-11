//! Tests for the calc groups a FROM enumerates.

use crate::logical_plan::*;
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::planner::planners::multi_stage::PlanningScope;
use crate::planner::planners::QueryPlanner;
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;
use std::collections::HashMap;
use std::rc::Rc;

fn schema(pre_aggregation_dimensions: &str) -> String {
    format!(
        indoc! {r#"
            cubes:
              - name: orders
                sql: "SELECT * FROM orders"
                dimensions:
                  - name: id
                    type: number
                    sql: id
                    primary_key: true
                  - name: status
                    type: string
                    sql: status
                  - name: currency
                    type: switch
                    values:
                      - USD
                      - EUR
                measures:
                  - name: amount
                    type: sum
                    sql: amount
                pre_aggregations:
                  - name: rollup
                    type: rollup
                    measures:
                      - amount
                    dimensions: [{}]
        "#},
        pre_aggregation_dimensions
    )
}

fn context(pre_aggregation_dimensions: &str) -> TestContext {
    TestContext::new(MockSchema::from_yaml(&schema(pre_aggregation_dimensions)).unwrap()).unwrap()
}

fn plan(context: &TestContext, yaml: &str) -> Rc<RootQuery> {
    let request = context.create_query_properties(yaml).unwrap();
    let mut scope = PlanningScope::new();
    let query = QueryPlanner::new(request, context.query_tools().clone())
        .plan(&mut scope)
        .unwrap();
    Rc::new(
        RootQuery::builder()
            .subqueries(scope.into_members())
            .query(query)
            .build(),
    )
}

fn optimize(context: &TestContext, plan: Rc<RootQuery>) -> Rc<RootQuery> {
    let request = context.create_query_properties(BY_CURRENCY).unwrap();
    PreAggregationOptimizer::new(
        context.query_tools().clone(),
        request.query_join_hints().clone(),
        false,
    )
    .try_optimize(plan, false, None)
    .unwrap()
    .expect("a rollup matches")
}

fn calc_groups(from: &LogicalFrom) -> Vec<(String, Vec<String>)> {
    from.calc_groups()
        .iter()
        .map(|group| (group.dimension.full_name(), group.values.clone()))
        .collect()
}

fn currency(values: &[&str]) -> Vec<(String, Vec<String>)> {
    vec![(
        "orders.currency".to_string(),
        values.iter().map(|v| v.to_string()).collect(),
    )]
}

const BY_CURRENCY: &str = indoc! {r#"
    dimensions:
      - orders.currency
    measures:
      - orders.amount
"#};

const PINNED: &str = indoc! {r#"
    dimensions:
      - orders.currency
    measures:
      - orders.amount
    filters:
      - member: orders.currency
        operator: equals
        values:
          - EUR
"#};

#[test]
fn a_calc_group_the_filters_leave_one_value_of_is_pinned() {
    let context = context("status");
    let root = plan(&context, PINNED);

    assert_eq!(calc_groups(root.query().from()), currency(&["EUR"]));
}

#[test]
fn an_unfiltered_calc_group_is_enumerated_with_all_its_values() {
    let context = context("status");
    let root = plan(&context, BY_CURRENCY);

    assert_eq!(calc_groups(root.query().from()), currency(&["USD", "EUR"]));
}

#[test]
fn filters_applied_on_the_query_behalf_pin_a_calc_group() {
    let context = context("status");
    let root = plan(&context, BY_CURRENCY);
    let pinning = context.create_query_properties(PINNED).unwrap();
    let query = root.query();
    let rebuilt = Rc::new(
        Query::builder()
            .schema(query.schema().clone())
            .filter(Rc::new(LogicalFilter::default()))
            .modifers(query.modifers().clone())
            .from(query.from().with_calc_groups(vec![]).unwrap())
            .filter_params_filters(Some(Rc::new(LogicalFilter {
                dimensions_filters: pinning.dimensions_filters().clone(),
                ..Default::default()
            })))
            .build(),
    )
    .with_resolved_calc_groups()
    .unwrap();

    assert_eq!(calc_groups(rebuilt.from()), currency(&["EUR"]));
}

#[test]
fn a_rollup_storing_a_calc_group_provides_it_as_a_column() {
    let context = context("status, currency");
    let optimized = optimize(&context, plan(&context, PINNED));
    let from = optimized.query().from();

    assert!(from.as_pre_aggregation().is_some());
    assert!(calc_groups(from).is_empty());
    let currency = context.create_dimension("orders.currency").unwrap();
    assert_eq!(
        from.schema().column(&currency).map(|c| c.provider.clone()),
        Some(Provider::PreAggregation)
    );
}

#[test]
fn over_a_rollup_without_a_calc_group_it_is_still_enumerated() {
    let context = context("status");
    let optimized = optimize(&context, plan(&context, BY_CURRENCY));
    let from = optimized.query().from();

    assert!(from.as_pre_aggregation().is_some());
    assert_eq!(calc_groups(from), currency(&["USD", "EUR"]));
}

#[test]
fn a_select_over_cubes_with_an_unresolved_calc_group_fails_to_build() {
    let context = context("status");
    let root = plan(&context, BY_CURRENCY);
    let query = root.query();
    let unresolved = Rc::new(
        Query::builder()
            .schema(query.schema().clone())
            .filter(query.filter().clone())
            .modifers(query.modifers().clone())
            .from(query.from().with_calc_groups(vec![]).unwrap())
            .join(query.join().clone())
            .build(),
    );
    let root = Rc::new(
        RootQuery::builder()
            .subqueries(vec![])
            .query(unresolved)
            .build(),
    );

    let tools = context.query_tools();
    let builder = PhysicalPlanBuilder::new(
        tools.query_tools().clone(),
        tools.plan_sql_templates(false).unwrap(),
        false,
    );
    let error = builder
        .build(root, HashMap::new(), false)
        .err()
        .expect("the select fails to build");
    assert_eq!(
        error.message,
        "Calc group orders.currency is not resolved by the FROM of the query"
    );
}
