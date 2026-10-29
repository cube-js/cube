//! A member follows its own view's join path, not the paths of other views
//! in the query.

use crate::planner::MemberSymbol;
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;
use std::rc::Rc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/multi_view_join_maps.yaml");
    TestContext::new(schema).unwrap()
}

/// Each group as (root cube, joined cubes, measures).
fn join_groups(ctx: &TestContext, query: &str) -> Vec<(String, Vec<String>, Vec<String>)> {
    let props = ctx.create_query_properties(query).unwrap();
    let measures: Vec<Rc<MemberSymbol>> = props.measures().clone();
    let groups = props
        .compute_join_multi_fact_groups_with_measures(&measures)
        .unwrap();
    groups
        .groups()
        .iter()
        .map(|(tree, measures)| {
            (
                tree.root().cube_id().to_string(),
                tree.joins()
                    .iter()
                    .map(|item| item.cube().cube_id().to_string())
                    .collect(),
                measures.iter().map(|m| m.full_name().clone()).collect(),
            )
        })
        .collect()
}

fn group(root: &str, joins: &[&str], measures: &[&str]) -> (String, Vec<String>, Vec<String>) {
    (
        root.to_string(),
        joins.iter().map(|s| s.to_string()).collect(),
        measures.iter().map(|s| s.to_string()).collect(),
    )
}

#[test]
fn test_dimension_view_measure_next_to_fact_view_measure() {
    let ctx = create_context();
    let groups = join_groups(
        &ctx,
        indoc! {"
            measures:
              - customers_view.count
              - orders_view.total_amount
            dimensions:
              - customers_view.city
        "},
    );
    assert_eq!(
        groups,
        vec![
            group("customers", &[], &["customers_view.count"]),
            group("orders", &["customers"], &["orders_view.total_amount"]),
        ]
    );
}

#[test]
fn test_fact_view_measure_next_to_dimension_view_measure() {
    let ctx = create_context();
    let groups = join_groups(
        &ctx,
        indoc! {"
            measures:
              - returns_view.total_refund
              - customers_view.avg_lifetime_value
            dimensions:
              - customers_view.city
        "},
    );
    assert_eq!(
        groups,
        vec![
            group("returns", &["customers"], &["returns_view.total_refund"]),
            group("customers", &[], &["customers_view.avg_lifetime_value"]),
        ]
    );
}

#[test]
fn test_two_fact_views_by_dimension_of_third_view() {
    let ctx = create_context();
    let groups = join_groups(
        &ctx,
        indoc! {"
            measures:
              - orders_view.total_amount
              - returns_view.total_refund
            dimensions:
              - customers_view.city
        "},
    );
    assert_eq!(
        groups,
        vec![
            group("orders", &["customers"], &["orders_view.total_amount"]),
            group("returns", &["customers"], &["returns_view.total_refund"]),
        ]
    );
}

#[test]
fn test_cube_measure_next_to_fact_view_measure() {
    let ctx = create_context();
    let groups = join_groups(
        &ctx,
        indoc! {"
            measures:
              - customers.count
              - orders_view.total_amount
            dimensions:
              - customers.city
        "},
    );
    assert_eq!(
        groups,
        vec![
            group("customers", &[], &["customers.count"]),
            group("orders", &["customers"], &["orders_view.total_amount"]),
        ]
    );
}

// `orders_view.city` is `customers.city` as reached from `orders`; `returns`
// has no join to `orders`, so the query is rejected naming both.
#[test]
fn test_two_fact_views_by_dimension_of_one_of_them_is_rejected() {
    let ctx = create_context();
    let error = ctx
        .build_sql(indoc! {"
            measures:
              - orders_view.total_amount
              - returns_view.total_refund
            dimensions:
              - orders_view.city
        "})
        .unwrap_err();
    assert_eq!(
        error.message,
        "Can't find join path to join 'orders,customers', 'returns': \
         'orders_view.city' is joined through 'orders', which 'returns_view.total_refund' \
         has no join to"
    );
}

#[test]
fn test_fact_view_dimension_as_filter_of_another_fact_is_rejected() {
    let ctx = create_context();
    let error = ctx
        .build_sql(indoc! {"
            measures:
              - returns_view.total_refund
            filters:
              - member: orders_view.city
                operator: equals
                values:
                  - New York
        "})
        .unwrap_err();
    assert!(
        error.message.ends_with(
            "'orders_view.city' is joined through 'orders', which 'returns_view.total_refund' \
             has no join to"
        ),
        "{}",
        error.message
    );
}

#[test]
fn test_fact_view_root_dimension_next_to_another_fact_is_rejected() {
    let ctx = create_context();
    let error = ctx
        .build_sql(indoc! {"
            measures:
              - returns_view.total_refund
            dimensions:
              - orders_view.status
        "})
        .unwrap_err();
    assert_eq!(
        error.message,
        "Can't find join path to join 'orders', 'returns'"
    );
}
