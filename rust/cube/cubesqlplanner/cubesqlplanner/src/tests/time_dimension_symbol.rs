//! Tests for the TimeDimensionSymbol dependency contract.
//!
//! A time dimension is a granularity view of its base dimension, not a
//! consumer of it: the read side looks through the base (emits the
//! base's dependencies, never the base itself), while the transform
//! side receives the base as a replaceable slot.

use crate::planner::MemberSymbol;
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use itertools::Itertools;
use std::cell::RefCell;
use std::rc::Rc;

fn ctx() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    TestContext::new(schema).unwrap()
}

#[test]
fn deps_of_plain_base_are_empty() {
    let ctx = ctx();
    let td = ctx
        .create_time_dimension("visitors.created_at", Some("day"))
        .unwrap();

    assert_eq!(td.full_name(), "visitors.created_at_day");
    assert!(
        td.get_dependencies().is_empty(),
        "the base dimension is not a dependency of its time dimension"
    );

    assert!(
        td.get_cube_refs().is_empty(),
        "a bare-column base carries no cube refs, so neither does its time dimension"
    );
}

#[test]
fn cube_refs_look_through_base() {
    let ctx = ctx();
    let td = ctx
        .create_time_dimension("visitors.visitor_id", Some("day"))
        .unwrap();

    let cube_refs = td.get_cube_refs();
    assert_eq!(cube_refs.len(), 1);
    assert_eq!(cube_refs[0].cube_name(), "visitors");
}

#[test]
fn deps_look_through_base() {
    let ctx = ctx();
    let td = ctx
        .create_time_dimension("visitors.minVisitorCheckinDate", Some("day"))
        .unwrap();

    let dep_names = td
        .get_dependencies()
        .iter()
        .map(|d| d.full_name())
        .collect_vec();
    assert_eq!(
        dep_names,
        vec!["visitor_checkins.minDate".to_string()],
        "deps are the base's dependencies; the base itself is not emitted"
    );
}

#[test]
fn deps_as_time_dimensions_wrap_time_deps() {
    let ctx = ctx();
    let td = ctx
        .create_time_dimension("visitors.minVisitorCheckinDate", Some("day"))
        .unwrap();

    let wrapped = td
        .as_time_dimension()
        .unwrap()
        .get_dependencies_as_time_dimensions();
    assert_eq!(wrapped.len(), 1);
    let dep = wrapped[0].as_time_dimension().unwrap();
    assert_eq!(wrapped[0].full_name(), "visitor_checkins.minDate_day");
    assert_eq!(dep.granularity(), &Some("day".to_string()));
    assert_eq!(dep.base_symbol().full_name(), "visitor_checkins.minDate");
}

#[test]
fn transform_visits_base_node_once() {
    let ctx = ctx();
    let td = ctx
        .create_time_dimension("visitors.minVisitorCheckinDate", Some("day"))
        .unwrap();

    let visited = RefCell::new(Vec::new());
    td.apply_recursive(&|node| {
        visited.borrow_mut().push(node.full_name());
        Ok(node.clone())
    })
    .unwrap();

    let visited = visited.into_inner();
    assert_eq!(
        visited,
        vec![
            "visitors.minVisitorCheckinDate_day".to_string(),
            "visitors.minVisitorCheckinDate".to_string(),
            "visitor_checkins.minDate".to_string(),
        ],
        "the transform sees the base as a node even though deps look through it"
    );
}

#[test]
fn transform_replaces_base_slot_and_keeps_wrapper_identity() {
    let ctx = ctx();
    let td = ctx
        .create_time_dimension("visitors.minVisitorCheckinDate", Some("day"))
        .unwrap();
    let replacement = ctx.create_dimension("visitors.created_at").unwrap();

    let result = td
        .apply_recursive(&|node: &Rc<MemberSymbol>| {
            if node.full_name() == "visitors.minVisitorCheckinDate" {
                Ok(replacement.clone())
            } else {
                Ok(node.clone())
            }
        })
        .unwrap();

    let result_td = result.as_time_dimension().unwrap();
    assert_eq!(result_td.base_symbol().full_name(), "visitors.created_at");
    assert_eq!(
        result.full_name(),
        "visitors.minVisitorCheckinDate_day",
        "replacing the base redirects rendering; the wrapper keeps its own identity"
    );
    assert!(result.get_dependencies().is_empty());
}

#[test]
fn read_and_transform_agree_on_slots() {
    let ctx = ctx();
    let td = ctx
        .create_time_dimension("visitors.minVisitorCheckinDate", Some("day"))
        .unwrap();

    let rebuilt = td.apply_recursive(&|node| Ok(node.clone())).unwrap();

    assert_eq!(rebuilt.full_name(), td.full_name());
    assert_eq!(
        rebuilt
            .get_dependencies()
            .iter()
            .map(|d| d.full_name())
            .collect_vec(),
        td.get_dependencies()
            .iter()
            .map(|d| d.full_name())
            .collect_vec(),
        "an identity transform preserves the dependency list and its order"
    );
}
