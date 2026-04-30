use crate::planner::sql_evaluator::MemberSymbol;
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use std::rc::Rc;

fn simple_ctx() -> TestContext {
    TestContext::new(MockSchema::from_yaml_file("common/simple.yaml")).unwrap()
}

fn visitors_ctx() -> TestContext {
    TestContext::new(MockSchema::from_yaml_file("common/visitors.yaml")).unwrap()
}

fn many_to_one_ctx() -> TestContext {
    TestContext::new(MockSchema::from_yaml_file("common/many_to_one_views.yaml")).unwrap()
}

fn dep_paths(symbol: &Rc<MemberSymbol>) -> Vec<(String, Vec<String>)> {
    let mut result: Vec<_> = symbol
        .get_dependencies()
        .into_iter()
        .map(|d| (d.full_name(), d.path().clone()))
        .collect();
    result.sort_by(|a, b| a.0.cmp(&b.0));
    result
}

// --- Simple dimension/measure path ---

#[test]
fn test_simple_dimension_path() {
    let ctx = simple_ctx();
    let dim = ctx.create_dimension("orders.status").unwrap();
    assert_eq!(dim.path(), &vec!["orders".to_string()]);
    assert_eq!(dim.cube_name(), "orders");
}

#[test]
fn test_simple_measure_path() {
    let ctx = simple_ctx();
    let m = ctx.create_measure("orders.count").unwrap();
    assert_eq!(m.path(), &vec!["orders".to_string()]);
    assert_eq!(m.cube_name(), "orders");
}

// --- View paths ---

#[test]
fn test_view_dimension_has_underlying_path() {
    let ctx = simple_ctx();

    // orders_with_customer.name → join_path: orders.customers
    let dim = ctx.create_dimension("orders_with_customer.name").unwrap();
    assert!(dim.as_dimension().unwrap().is_view());
    assert_eq!(dim.path(), &vec!["orders_with_customer".to_string()]);

    let deps = dep_paths(&dim);
    assert_eq!(deps.len(), 1);
    assert_eq!(deps[0].0, "customers.name");
    assert_eq!(
        deps[0].1,
        vec!["orders".to_string(), "customers".to_string()]
    );
}

#[test]
fn test_many_to_one_view_child_has_underlying_path() {
    let ctx = many_to_one_ctx();

    // many_to_one_view.child_dim → join_path: many_to_one_root.many_to_one_child
    let dim = ctx.create_dimension("many_to_one_view.child_dim").unwrap();
    assert!(dim.as_dimension().unwrap().is_view());
    assert_eq!(dim.path(), &vec!["many_to_one_view".to_string()]);

    let deps = dep_paths(&dim);
    assert_eq!(deps.len(), 1);
    assert_eq!(deps[0].0, "many_to_one_child.child_dim");
    assert_eq!(
        deps[0].1,
        vec![
            "many_to_one_root".to_string(),
            "many_to_one_child".to_string()
        ]
    );
}

// --- Dependencies from SQL templates: same cube ---

#[test]
fn test_dep_path_same_cube_member_ref() {
    // visitors.visitor_id_proxy: sql = "{visitors.visitor_id}"
    let ctx = visitors_ctx();
    let dim = ctx.create_dimension("visitors.visitor_id_proxy").unwrap();
    assert_eq!(dim.path(), &vec!["visitors".to_string()]);

    let deps = dep_paths(&dim);
    assert_eq!(deps.len(), 1);
    assert_eq!(deps[0].0, "visitors.visitor_id");
    assert_eq!(deps[0].1, vec!["visitors".to_string()]);
}

#[test]
fn test_dep_path_short_member_ref() {
    let ctx = visitors_ctx();
    let dim = ctx.create_dimension("visitors.visitor_id_twice").unwrap();
    assert_eq!(dim.path(), &vec!["visitors".to_string()]);

    let deps = dep_paths(&dim);
    assert_eq!(deps.len(), 1);
    assert_eq!(deps[0].0, "visitors.visitor_id");
    assert_eq!(deps[0].1, vec!["visitors".to_string()]);
}

// --- Dependencies from SQL templates: cross-cube ---

#[test]
fn test_dep_path_cross_cube_member_ref() {
    // visitors.minVisitorCheckinDate: sql = "{visitor_checkins.minDate}"
    let ctx = visitors_ctx();
    let dim = ctx
        .create_dimension("visitors.minVisitorCheckinDate")
        .unwrap();
    assert_eq!(dim.path(), &vec!["visitors".to_string()]);

    let deps = dep_paths(&dim);
    assert_eq!(deps.len(), 1);
    assert_eq!(deps[0].0, "visitor_checkins.minDate");
    assert_eq!(deps[0].1, vec!["visitor_checkins".to_string()]);
}

// --- Dependencies from SQL: multiple refs ---

#[test]
fn test_dep_path_multiple_refs_in_sql() {
    let ctx = visitors_ctx();
    let dim = ctx.create_dimension("visitors.source_concat_id").unwrap();
    assert_eq!(dim.path(), &vec!["visitors".to_string()]);

    let deps = dep_paths(&dim);
    assert_eq!(deps.len(), 2);
    assert_eq!(deps[0].0, "visitors.source");
    assert_eq!(deps[0].1, vec!["visitors".to_string()]);
    assert_eq!(deps[1].0, "visitors.visitor_id");
    assert_eq!(deps[1].1, vec!["visitors".to_string()]);
}

// --- Measure dependencies ---

#[test]
fn test_dep_path_measure_self_ref() {
    // visitors.total_revenue_proxy: sql = "{total_revenue}"
    let ctx = visitors_ctx();
    let m = ctx.create_measure("visitors.total_revenue_proxy").unwrap();
    assert_eq!(m.path(), &vec!["visitors".to_string()]);

    let deps = dep_paths(&m);
    assert_eq!(deps.len(), 1);
    assert_eq!(deps[0].0, "visitors.total_revenue");
    assert_eq!(deps[0].1, vec!["visitors".to_string()]);
}

#[test]
fn test_dep_path_measure_cross_cube_ref() {
    // customers.payments_per_order: sql = "{payments} / {orders.count}"
    let ctx = simple_ctx();
    let m = ctx.create_measure("customers.payments_per_order").unwrap();
    assert_eq!(m.path(), &vec!["customers".to_string()]);

    let deps = dep_paths(&m);
    assert_eq!(deps.len(), 2);
    assert_eq!(deps[0].0, "customers.payments");
    assert_eq!(deps[0].1, vec!["customers".to_string()]);
    assert_eq!(deps[1].0, "orders.count");
    assert_eq!(deps[1].1, vec!["orders".to_string()]);
}

#[test]
fn test_dep_path_measure_mixed_refs() {
    let ctx = visitors_ctx();
    let m = ctx
        .create_measure("visitors.total_revenue_per_count")
        .unwrap();
    assert_eq!(m.path(), &vec!["visitors".to_string()]);

    let deps = dep_paths(&m);
    assert_eq!(deps.len(), 2);
    assert_eq!(deps[0].0, "visitors.count");
    assert_eq!(deps[0].1, vec!["visitors".to_string()]);
    assert_eq!(deps[1].0, "visitors.total_revenue");
    assert_eq!(deps[1].1, vec!["visitors".to_string()]);
}

// --- Segment path ---

#[test]
fn test_segment_compiled_path() {
    let ctx = visitors_ctx();
    let seg = ctx.create_symbol("visitors.google").unwrap();
    assert_eq!(seg.path(), &vec!["visitors".to_string()]);
    assert_eq!(seg.cube_name(), "visitors");

    let deps = dep_paths(&seg);
    assert_eq!(deps.len(), 1);
    assert_eq!(deps[0].0, "visitors.source");
    assert_eq!(deps[0].1, vec!["visitors".to_string()]);
}

// --- Time dimension path ---

#[test]
fn test_time_dimension_path() {
    let ctx = simple_ctx();
    let td = ctx.create_dimension("orders.created_at.day").unwrap();
    assert_eq!(td.path(), &vec!["orders".to_string()]);
    assert_eq!(td.cube_name(), "orders");
}
