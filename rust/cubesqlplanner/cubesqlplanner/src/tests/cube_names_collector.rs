use crate::planner::sql_evaluator::collectors::collect_cube_names;
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;

fn many_to_one_ctx() -> TestContext {
    TestContext::new(MockSchema::from_yaml_file("common/many_to_one_views.yaml")).unwrap()
}

#[test]
fn test_cube_names_single_cube_dimension() {
    let ctx = TestContext::new(MockSchema::from_yaml_file("common/simple.yaml")).unwrap();
    let dim = ctx.create_dimension("orders.status").unwrap();
    let names = collect_cube_names(&dim).unwrap();
    assert_eq!(names, vec!["orders"]);
}

#[test]
fn test_cube_names_single_cube_measure() {
    let ctx = TestContext::new(MockSchema::from_yaml_file("common/simple.yaml")).unwrap();
    let measure = ctx.create_measure("orders.count").unwrap();
    let names = collect_cube_names(&measure).unwrap();
    assert_eq!(names, vec!["orders"]);
}

#[test]
fn test_cube_names_cross_cube_measure() {
    let ctx = TestContext::new(MockSchema::from_yaml_file("common/simple.yaml")).unwrap();
    let measure = ctx.create_measure("customers.payments_per_order").unwrap();
    let mut names = collect_cube_names(&measure).unwrap();
    names.sort();
    assert_eq!(names, vec!["customers", "orders"]);
}

#[test]
fn test_cube_names_many_to_one_view_root_dim() {
    let ctx = many_to_one_ctx();
    let dim = ctx.create_dimension("many_to_one_view.root_dim").unwrap();
    let names = collect_cube_names(&dim).unwrap();
    assert_eq!(names, vec!["many_to_one_root"]);
}

#[test]
fn test_cube_names_many_to_one_view_child_dim() {
    let ctx = many_to_one_ctx();
    let dim = ctx.create_dimension("many_to_one_view.child_dim").unwrap();
    let mut names = collect_cube_names(&dim).unwrap();
    names.sort();
    assert_eq!(names, vec!["many_to_one_child", "many_to_one_root"]);
}

#[test]
fn test_cube_names_many_to_one_view_root_measure() {
    let ctx = many_to_one_ctx();
    let measure = ctx.create_measure("many_to_one_view.root_val_avg").unwrap();
    let names = collect_cube_names(&measure).unwrap();
    assert_eq!(names, vec!["many_to_one_root"]);
}

#[test]
fn test_cube_names_many_to_one_view_child_measure() {
    let ctx = many_to_one_ctx();
    let measure = ctx
        .create_measure("many_to_one_view.child_val_avg")
        .unwrap();
    let mut names = collect_cube_names(&measure).unwrap();
    names.sort();
    assert_eq!(names, vec!["many_to_one_child", "many_to_one_root"]);
}
