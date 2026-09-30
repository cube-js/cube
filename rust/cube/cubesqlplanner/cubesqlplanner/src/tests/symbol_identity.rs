use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{CubeId, MemberId};
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use std::collections::HashSet;

#[test]
fn cube_id_renders_and_targets_the_cube_name() {
    let id = CubeId::cube("orders");
    assert_eq!(id.to_string(), "orders");
    assert_eq!(id.target(), "orders");
    assert_eq!(format!("{:?}", id), format!("{:?}", "orders"));
    assert_eq!(id, CubeId::cube("orders".to_string()));
    assert_ne!(id, CubeId::cube("users"));
}

#[test]
fn cube_id_orders_like_its_name() {
    let mut ids = vec![
        CubeId::cube("visitors"),
        CubeId::cube("Orders"),
        CubeId::cube("orders"),
        CubeId::cube("customers"),
    ];
    ids.sort();
    let names = ids.iter().map(|id| id.to_string()).collect::<Vec<_>>();
    let mut expected = vec!["visitors", "Orders", "orders", "customers"];
    expected.sort();
    assert_eq!(names, expected);
}

#[test]
fn cube_id_alias_is_the_alias_of_its_name() {
    let id = CubeId::cube("VisitorCheckins");
    assert_eq!(
        PlanSqlTemplates::alias_name(&id.to_string()),
        PlanSqlTemplates::alias_name("VisitorCheckins")
    );
}

#[test]
fn member_id_full_names() {
    let orders = CubeId::cube("orders");
    let status = MemberId::member(orders.clone(), "status");
    assert_eq!(status.full_name(), "orders.status");
    assert_eq!(status.to_string(), "orders.status");
    assert_eq!(format!("{:?}", status), format!("{:?}", "orders.status"));
    assert_eq!(status.cube(), &orders);
    assert_eq!(status.target_path(), "orders.status");

    let created_at = MemberId::member(orders.clone(), "created_at");
    let month = MemberId::time_dimension(created_at.clone(), Some("month"));
    assert_eq!(month.full_name(), "orders.created_at_month");
    assert_eq!(month.base(), &created_at);
    assert_eq!(month.cube(), &orders);
    assert_eq!(month.target_path(), "orders.created_at");

    let expr = MemberId::expression(orders.clone(), "completed");
    assert_eq!(expr.full_name(), "expr:orders.completed");
    assert_eq!(expr.base(), &expr);
}

#[test]
fn member_id_time_dimension_without_granularity_is_its_day_form() {
    let created_at = MemberId::member(CubeId::cube("orders"), "created_at");
    assert_eq!(
        MemberId::time_dimension(created_at.clone(), None),
        MemberId::time_dimension(created_at.clone(), Some("day"))
    );
    assert_ne!(
        MemberId::time_dimension(created_at.clone(), Some("day")),
        MemberId::time_dimension(created_at, Some("month"))
    );
}

#[test]
fn member_id_equality_follows_the_full_name() {
    let a = MemberId::member(CubeId::cube("orders"), "status");
    let b = MemberId::member(CubeId::cube("orders"), "status".to_string());
    assert_eq!(a, b);
    let set = [a.clone(), b].into_iter().collect::<HashSet<_>>();
    assert_eq!(set.len(), 1);
    assert_ne!(a, MemberId::member(CubeId::cube("users"), "status"));
    assert_ne!(a, MemberId::expression(CubeId::cube("orders"), "status"));
}

#[test]
fn member_id_orders_like_its_full_name() {
    let orders = CubeId::cube("orders");
    let created_at = MemberId::member(orders.clone(), "created_at");
    let mut ids = vec![
        MemberId::member(orders.clone(), "status"),
        MemberId::time_dimension(created_at.clone(), Some("month")),
        MemberId::expression(orders.clone(), "completed"),
        created_at,
        MemberId::member(CubeId::cube("users"), "city"),
    ];
    ids.sort();
    let names = ids.iter().map(|id| id.to_string()).collect::<Vec<_>>();
    let mut expected = names.clone();
    expected.sort();
    assert_eq!(names, expected);
}

fn visitors_ctx() -> TestContext {
    TestContext::new(MockSchema::from_yaml_file("common/visitors.yaml")).unwrap()
}

#[test]
fn symbol_ids_render_as_their_full_names() {
    let ctx = visitors_ctx();

    let dim = ctx.create_dimension("visitors.source").unwrap();
    assert_eq!(
        dim.id(),
        &MemberId::member(CubeId::cube("visitors"), "source")
    );
    assert_eq!(dim.full_name(), "visitors.source");

    let measure = ctx.create_measure("visitors.count").unwrap();
    assert_eq!(measure.full_name(), "visitors.count");

    let td = ctx
        .create_time_dimension("visitors.created_at", Some("month"))
        .unwrap();
    assert_eq!(td.full_name(), "visitors.created_at_month");
    assert_eq!(
        td.id().base(),
        &MemberId::member(CubeId::cube("visitors"), "created_at")
    );

    let view_member = ctx
        .create_dimension("visitors_visitors_checkins.id")
        .unwrap();
    assert_eq!(view_member.full_name(), "visitors_visitors_checkins.id");

    let segment = ctx.create_segment("visitors.google").unwrap();
    assert_eq!(segment.full_name(), "visitors.google");
    assert_eq!(
        segment.member_evaluator().full_name(),
        "expr:visitors.google"
    );
}
