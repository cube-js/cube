//! Tests for DimensionSymbol: kind classification and helper methods

use crate::planner::sql_evaluator::symbols::dimension_kinds::DimensionKind;
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;

fn ctx() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/dimension_kind_tests.yaml");
    TestContext::new(schema).unwrap()
}

// ─── Per-dimension property tests ───────────────────────────────────────────

#[test]
fn dimension_regular_string() {
    let ctx = ctx();
    let d = ctx.create_dimension("test_dims.name").unwrap();
    let dim = d.as_dimension().unwrap();

    assert!(matches!(dim.kind(), DimensionKind::Regular(_)));
    assert_eq!(dim.dimension_type(), "string");
    assert!(!dim.is_time());
    assert!(!dim.is_geo());
    assert!(!dim.is_switch());
    assert!(!dim.is_case());
    assert!(!dim.is_calc_group());
    assert!(!dim.is_sub_query());
    assert!(dim.member_sql().is_some());
    assert!(dim.latitude().is_none());
    assert!(dim.longitude().is_none());
    assert!(dim.case().is_none());
    assert!(dim.values().is_empty());
}

#[test]
fn dimension_regular_number() {
    let ctx = ctx();
    let d = ctx.create_dimension("test_dims.amount").unwrap();
    let dim = d.as_dimension().unwrap();

    assert!(matches!(dim.kind(), DimensionKind::Regular(_)));
    assert_eq!(dim.dimension_type(), "number");
    assert!(!dim.is_time());
    assert!(!dim.is_geo());
}

#[test]
fn dimension_regular_time() {
    let ctx = ctx();
    let d = ctx.create_dimension("test_dims.created_at").unwrap();
    let dim = d.as_dimension().unwrap();

    assert!(matches!(dim.kind(), DimensionKind::Regular(_)));
    assert_eq!(dim.dimension_type(), "time");
    assert!(dim.is_time());
    assert!(!dim.is_geo());
    assert!(!dim.is_switch());
    assert!(!dim.is_case());
}

#[test]
fn dimension_regular_boolean() {
    let ctx = ctx();
    let d = ctx.create_dimension("test_dims.is_active").unwrap();
    let dim = d.as_dimension().unwrap();

    assert!(matches!(dim.kind(), DimensionKind::Regular(_)));
    assert_eq!(dim.dimension_type(), "boolean");
    assert!(!dim.is_time());
}

#[test]
fn dimension_geo() {
    let ctx = ctx();
    let d = ctx.create_dimension("test_dims.location").unwrap();
    let dim = d.as_dimension().unwrap();

    assert!(matches!(dim.kind(), DimensionKind::Geo(_)));
    assert_eq!(dim.dimension_type(), "geo");
    assert!(dim.is_geo());
    assert!(!dim.is_time());
    assert!(!dim.is_switch());
    assert!(!dim.is_case());
    assert!(!dim.is_calc_group());
    assert!(dim.latitude().is_some());
    assert!(dim.longitude().is_some());
    assert!(dim.member_sql().is_none());
}

#[test]
fn dimension_switch_with_sql() {
    let ctx = ctx();
    let d = ctx.create_dimension("test_dims.currency").unwrap();
    let dim = d.as_dimension().unwrap();

    assert!(matches!(dim.kind(), DimensionKind::Switch(_)));
    assert_eq!(dim.dimension_type(), "switch");
    assert!(dim.is_switch());
    assert!(!dim.is_calc_group());
    assert!(!dim.is_time());
    assert!(!dim.is_geo());
    assert!(!dim.is_case());
    assert_eq!(dim.values(), &["USD", "EUR", "GBP"]);
    assert!(dim.member_sql().is_some());
}

#[test]
fn dimension_calc_group() {
    let ctx = ctx();
    let d = ctx.create_dimension("test_dims.calc_group").unwrap();
    let dim = d.as_dimension().unwrap();

    assert!(matches!(dim.kind(), DimensionKind::Switch(_)));
    assert_eq!(dim.dimension_type(), "switch");
    assert!(dim.is_switch());
    assert!(dim.is_calc_group());
    assert_eq!(dim.values(), &["option_a", "option_b"]);
    assert!(dim.member_sql().is_none());
    assert!(!dim.owned_by_cube());
}

#[test]
fn dimension_case() {
    let ctx = ctx();
    let d = ctx.create_dimension("test_dims.status_label").unwrap();
    let dim = d.as_dimension().unwrap();

    assert!(matches!(dim.kind(), DimensionKind::Case(_)));
    assert_eq!(dim.dimension_type(), "string");
    assert!(dim.is_case());
    assert!(!dim.is_time());
    assert!(!dim.is_geo());
    assert!(!dim.is_switch());
    assert!(!dim.is_calc_group());
    assert!(dim.case().is_some());
}

#[test]
fn dimension_sub_query() {
    let ctx = ctx();
    let d = ctx.create_dimension("test_dims.sub_query_dim").unwrap();
    let dim = d.as_dimension().unwrap();

    assert!(matches!(dim.kind(), DimensionKind::Regular(_)));
    assert_eq!(dim.dimension_type(), "time");
    assert!(dim.is_time());
    assert!(dim.is_sub_query());
}
