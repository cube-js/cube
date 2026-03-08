use crate::cube_bridge::join_hints::JoinHintItem;
use crate::planner::sql_evaluator::collectors::{
    collect_join_hints, collect_join_hints_for_measures,
};
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn s(name: &str) -> JoinHintItem {
    JoinHintItem::Single(name.to_string())
}

fn v(names: &[&str]) -> JoinHintItem {
    JoinHintItem::Vector(names.iter().map(|n| n.to_string()).collect())
}

#[test]
fn test_collect_join_hints_single_cube() {
    let ctx = TestContext::new(MockSchema::from_yaml_file("common/simple.yaml")).unwrap();

    let dim = ctx.create_dimension("orders.status").unwrap();
    let hints = collect_join_hints(&dim).unwrap();
    assert_eq!(hints.len(), 1);
    assert_eq!(hints.items(), &[s("orders")]);

    let measure = ctx.create_measure("orders.count").unwrap();
    let hints = collect_join_hints(&measure).unwrap();
    assert_eq!(hints.len(), 1);
    assert_eq!(hints.items(), &[s("orders")]);

    let measure = ctx.create_measure("customers.payments").unwrap();
    let hints = collect_join_hints(&measure).unwrap();
    assert_eq!(hints.len(), 1);
    assert_eq!(hints.items(), &[s("customers")]);
}

#[test]
fn test_collect_join_hints_cross_cube_measure() {
    let ctx = TestContext::new(MockSchema::from_yaml_file("common/simple.yaml")).unwrap();

    let measure = ctx.create_measure("customers.payments_per_order").unwrap();
    let hints = collect_join_hints(&measure).unwrap();
    assert_eq!(hints.len(), 2);
    assert!(hints.items().contains(&s("customers")));
    assert!(hints.items().contains(&s("orders")));
}

#[test]
fn test_collect_join_hints_view_symbols() {
    let ctx = TestContext::new(MockSchema::from_yaml_file("common/diamond_joins.yaml")).unwrap();

    let dim = ctx.create_dimension("a_with_b_and_c.code").unwrap();
    let hints = collect_join_hints(&dim).unwrap();
    assert_eq!(hints.len(), 1);
    assert_eq!(hints.items(), &[v(&["cube_a", "cube_b", "cube_c"])]);

    let dim = ctx.create_dimension("a_with_b_and_c.name").unwrap();
    let hints = collect_join_hints(&dim).unwrap();
    assert_eq!(hints.len(), 1);
    assert_eq!(hints.items(), &[s("cube_a")]);

    // View measure from root join_path
    let measure = ctx.create_measure("a_with_b_and_c.total_value").unwrap();
    let hints = collect_join_hints(&measure).unwrap();
    assert_eq!(hints.len(), 1);
    assert_eq!(hints.items(), &[s("cube_a")]);
}

#[test]
fn test_collect_join_hints_for_measures_multiple() {
    let ctx = TestContext::new(MockSchema::from_yaml_file("common/simple.yaml")).unwrap();

    let m1 = ctx.create_measure("orders.count").unwrap();
    let m2 = ctx.create_measure("customers.count").unwrap();
    let hints = collect_join_hints_for_measures(&vec![m1, m2]).unwrap();

    assert_eq!(hints.len(), 2);
    assert!(hints.items().contains(&s("orders")));
    assert!(hints.items().contains(&s("customers")));

    let m1 = ctx.create_measure("orders.count").unwrap();
    let m2 = ctx.create_measure("orders.max_amount").unwrap();
    let hints = collect_join_hints_for_measures(&vec![m1, m2]).unwrap();
    assert_eq!(hints.len(), 1);
    assert_eq!(hints.items(), &[s("orders")]);
}

// --- many_to_one view tests ---

fn many_to_one_ctx() -> TestContext {
    TestContext::new(MockSchema::from_yaml_file("common/many_to_one_views.yaml")).unwrap()
}

#[test]
fn test_join_hints_many_to_one_view_root_dim() {
    let ctx = many_to_one_ctx();
    let dim = ctx.create_dimension("many_to_one_view.root_dim").unwrap();
    let hints = collect_join_hints(&dim).unwrap();
    assert_eq!(hints.len(), 1);
    assert_eq!(hints.items(), &[s("many_to_one_root")]);
}

#[test]
fn test_join_hints_many_to_one_view_child_dim() {
    let ctx = many_to_one_ctx();
    let dim = ctx.create_dimension("many_to_one_view.child_dim").unwrap();
    let hints = collect_join_hints(&dim).unwrap();
    assert_eq!(hints.len(), 1);
    assert_eq!(
        hints.items(),
        &[v(&["many_to_one_root", "many_to_one_child"])]
    );
}

#[test]
fn test_join_hints_many_to_one_view_root_measure() {
    let ctx = many_to_one_ctx();
    let measure = ctx.create_measure("many_to_one_view.root_val_avg").unwrap();
    let hints = collect_join_hints(&measure).unwrap();
    assert_eq!(hints.len(), 1);
    assert_eq!(hints.items(), &[s("many_to_one_root")]);
}

#[test]
fn test_join_hints_many_to_one_view_child_measure() {
    let ctx = many_to_one_ctx();
    let measure = ctx.create_measure("many_to_one_view.child_val_avg").unwrap();
    let hints = collect_join_hints(&measure).unwrap();
    assert_eq!(hints.len(), 1);
    assert_eq!(
        hints.items(),
        &[v(&["many_to_one_root", "many_to_one_child"])]
    );
}

#[test]
fn test_join_hints_many_to_one_view_combined_measures() {
    let ctx = many_to_one_ctx();
    let m1 = ctx.create_measure("many_to_one_view.root_val_avg").unwrap();
    let m2 = ctx.create_measure("many_to_one_view.child_val_avg").unwrap();
    let hints = collect_join_hints_for_measures(&vec![m1, m2]).unwrap();
    assert_eq!(hints.len(), 2);
    assert!(hints.items().contains(&s("many_to_one_root")));
    assert!(hints.items().contains(&v(&["many_to_one_root", "many_to_one_child"])));
}

#[test]
fn test_many_to_one_view_build_sql() {
    let ctx = many_to_one_ctx();
    let query = indoc! {"
        measures:
          - many_to_one_view.root_val_avg
          - many_to_one_view.child_val_avg
        dimensions:
          - many_to_one_view.root_dim
          - many_to_one_view.child_dim
    "};
    let sql = ctx.build_sql(query).unwrap();
    insta::assert_snapshot!(sql);
}
