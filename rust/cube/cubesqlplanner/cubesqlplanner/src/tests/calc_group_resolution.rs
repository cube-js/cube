//! Tests for the calc groups a FROM enumerates.

use crate::logical_plan::*;
use crate::planner::planners::multi_stage::PlanningScope;
use crate::planner::planners::QueryPlanner;
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;
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
                  - name: currency_proxy
                    type: string
                    sql: "{{CUBE.currency}}"
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

fn optimize(context: &TestContext, plan: Rc<RootQuery>, yaml: &str) -> Rc<RootQuery> {
    let request = context.create_query_properties(yaml).unwrap();
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
fn filters_an_enclosing_select_applies_pin_a_calc_group() {
    let context = context("status");
    let root = plan(&context, BY_CURRENCY);
    let pinning = context.create_query_properties(PINNED).unwrap();
    let query = root.query();
    let rebuilt = Query::builder()
        .schema(query.schema().clone())
        .filter(Rc::new(LogicalFilter::default()))
        .modifers(query.modifers().clone())
        .from(query.from().clone())
        .filter_params_filters(Some(Rc::new(LogicalFilter {
            dimensions_filters: pinning.dimensions_filters().clone(),
            ..Default::default()
        })))
        .build()
        .unwrap();

    assert_eq!(calc_groups(rebuilt.from()), currency(&["EUR"]));
}

#[test]
fn a_select_over_subqueries_enumerates_no_calc_group() {
    let context = context("status");
    let root = plan(&context, BY_CURRENCY);
    let query = root.query();
    let over_subqueries = Query::builder()
        .schema(query.schema().clone())
        .filter(query.filter().clone())
        .modifers(query.modifers().clone())
        .from(LogicalFrom::empty())
        .placement(Some(Rc::new(FilterPlacement::aggregating(
            LogicalFilter::default(),
        ))))
        .build()
        .unwrap();

    assert!(calc_groups(over_subqueries.from()).is_empty());
}

#[test]
fn changing_the_schema_resolves_the_calc_groups_again() {
    let context = context("status");
    let root = plan(
        &context,
        indoc! {r#"
            dimensions:
              - orders.status
            measures:
              - orders.amount
        "#},
    );
    let query = root.query();
    assert!(calc_groups(query.from()).is_empty());

    let by_currency = plan(&context, BY_CURRENCY);
    let widened = query
        .with_schema_and_modifers(
            by_currency.query().schema().clone(),
            query.modifers().clone(),
        )
        .unwrap();

    assert_eq!(calc_groups(widened.from()), currency(&["USD", "EUR"]));
}

#[test]
fn a_rollup_storing_a_calc_group_provides_it_as_a_column() {
    let context = context("status, currency");
    let optimized = optimize(&context, plan(&context, PINNED), PINNED);
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
fn a_calc_group_the_rollup_does_not_store_is_still_enumerated() {
    let context = context("status");
    let optimized = optimize(&context, plan(&context, BY_CURRENCY), BY_CURRENCY);
    let from = optimized.query().from();

    assert!(from.as_pre_aggregation().is_some());
    assert_eq!(calc_groups(from), currency(&["USD", "EUR"]));
}

#[test]
fn replacing_the_rollup_resolves_the_calc_groups_again() {
    let storing_context = context("status, currency");
    let storing = optimize(
        &storing_context,
        plan(&storing_context, BY_CURRENCY),
        BY_CURRENCY,
    );
    assert!(calc_groups(storing.query().from()).is_empty());

    let other_context = self::context("status");
    let other = optimize(
        &other_context,
        plan(&other_context, BY_CURRENCY),
        BY_CURRENCY,
    );
    let other_rollup = other.query().from().inputs();
    let replaced = storing
        .query()
        .clone()
        .with_inputs(vec![storing
            .query()
            .from()
            .clone()
            .with_inputs(other_rollup)
            .unwrap()
            .as_plan_node()])
        .unwrap();

    assert!(replaced.from().as_pre_aggregation().is_some());
    assert_eq!(calc_groups(replaced.from()), currency(&["USD", "EUR"]));
}

const BY_CURRENCY_PROXY: &str = indoc! {r#"
    dimensions:
      - orders.currency_proxy
    measures:
      - orders.amount
"#};

#[test]
fn a_rollup_storing_a_proxy_of_a_calc_group_provides_it_as_a_column() {
    let context = context("currency_proxy");
    for query in [BY_CURRENCY_PROXY, BY_CURRENCY] {
        let optimized = optimize(&context, plan(&context, query), query);
        let from = optimized.query().from();

        assert!(from.as_pre_aggregation().is_some(), "{query}");
        assert!(calc_groups(from).is_empty(), "{query}");
        let currency = context.create_dimension("orders.currency").unwrap();
        assert_eq!(
            from.schema().column(&currency).map(|c| c.provider.clone()),
            Some(Provider::PreAggregation),
            "{query}"
        );
    }
}

#[test]
fn over_a_rollup_storing_a_proxy_of_a_calc_group_its_rows_are_not_repeated() {
    let context = context("currency_proxy");
    for query in [BY_CURRENCY_PROXY, BY_CURRENCY, PINNED] {
        let (sql, usages) = context.build_sql_with_used_pre_aggregations(query).unwrap();

        assert_eq!(usages.len(), 1, "{query}");
        assert!(!sql.contains("CROSS JOIN"), "{sql}");
        assert!(sql.contains("\"orders__currency_proxy\""), "{sql}");
    }
}
