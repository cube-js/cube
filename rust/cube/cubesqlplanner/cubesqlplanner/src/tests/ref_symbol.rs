//! `MemberSymbol::Ref`: a view member standing for the member it re-exports.

use crate::planner::symbols::transforms;
use crate::planner::{MeasureRenderModifier, RefSymbol};
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::schemas::TestCompiler;

fn compiler(yaml: &str) -> TestCompiler {
    TestCompiler::new(MockSchema::from_yaml_file(yaml).create_evaluator())
}

#[test]
fn test_ref_requires_a_direct_reference() {
    let mut test_compiler = compiler("common/integration_view_members.yaml");
    let calculated = test_compiler
        .compiler
        .add_dimension_evaluator("orders_view.status_label".to_string())
        .unwrap();
    let dimension = calculated.as_dimension().unwrap();

    let error = RefSymbol::try_new(
        dimension.compiled_path().clone(),
        dimension.member_sql().unwrap(),
        None,
    )
    .err()
    .expect("a calculated member is not a reference");
    assert!(error.message.contains("is not a direct reference"));
}

// An in-cube proxy is a reference too, but not a `Ref`: peeling stops at it.
#[test]
fn test_peel_refs_stops_at_an_in_cube_proxy() {
    let mut test_compiler = compiler("common/visitors.yaml");
    let proxy = test_compiler
        .compiler
        .add_dimension_evaluator("visitors.visitor_id_proxy".to_string())
        .unwrap();

    assert!(proxy.is_reference());
    assert_eq!(proxy.peel_refs().full_name(), "visitors.visitor_id_proxy");
    assert_eq!(
        proxy.clone().resolve_reference_chain().full_name(),
        "visitors.visitor_id"
    );

    let view_member = test_compiler
        .compiler
        .add_dimension_evaluator("visitors_visitors_checkins.visitor_id".to_string())
        .unwrap();
    assert_eq!(
        view_member.peel_refs().full_name(),
        "visitor_checkins.visitor_id"
    );
}

// A transform reaches the target through the reference and leaves the
// reference itself as it was.
#[test]
fn test_transform_rewrites_the_target_of_a_ref() {
    let mut test_compiler = compiler("common/visitors.yaml");
    let view_count = test_compiler
        .compiler
        .add_measure_evaluator("visitors_visitors_checkins.count".to_string())
        .unwrap();

    let stamped =
        transforms::measures_render_modifier(&view_count, &MeasureRenderModifier::RawValue)
            .unwrap();

    let reference = stamped.as_ref_symbol().unwrap();
    assert_eq!(stamped.full_name(), "visitors_visitors_checkins.count");
    let target = reference.target_member().unwrap().as_measure().unwrap();
    assert!(matches!(
        target.render_modifier(),
        Some(MeasureRenderModifier::RawValue)
    ));
}
