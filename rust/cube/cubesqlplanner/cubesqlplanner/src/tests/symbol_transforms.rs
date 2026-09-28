//! Tests for schema-level symbol transforms.

use crate::logical_plan::transforms::{
    mark_tz_converted_at_source_in_schema, measures_render_modifier_in_schema,
};
use crate::logical_plan::LogicalSchema;
use crate::planner::symbols::transforms;
use crate::planner::{MeasureRenderModifier, MemberSymbol};
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

fn ctx() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/symbol_transforms.yaml");
    TestContext::new(schema).unwrap()
}

/// Mark of the time dimension `name` anywhere in the tree, `None`
/// when the tree has no such time dimension.
fn tz_mark_of(symbol: &Rc<MemberSymbol>, name: &str) -> Option<bool> {
    let found = RefCell::new(None);
    symbol
        .apply_recursive(&|node| {
            if let MemberSymbol::TimeDimension(td) = node.as_ref() {
                if node.full_name() == name {
                    *found.borrow_mut() = Some(td.tz_converted_at_source());
                }
            }
            Ok(node.clone())
        })
        .unwrap();
    found.into_inner()
}

// Deriving another form of the same member keeps the mark: it says
// where the value comes from, which re-reading it at a different
// granularity does not change.
#[test]
fn tz_mark_survives_granularity_change() {
    let ctx = ctx();
    let td = ctx
        .create_time_dimension("events.created_at", Some("day"))
        .unwrap();
    let names = HashSet::from(["events.created_at_day".to_string()]);
    let marked = transforms::mark_tz_converted_at_source(&td, &names).unwrap();
    let marked = marked.as_time_dimension().unwrap();
    assert!(marked.tz_converted_at_source());

    let regranulated = marked
        .change_granularity(ctx.query_tools().clone(), Some("month".to_string()))
        .unwrap();
    assert!(
        regranulated.tz_converted_at_source(),
        "the mark must survive a granularity change"
    );
}

/// Whether the measure `name` anywhere in the tree carries a render
/// modifier, `None` when the tree has no such measure.
fn has_render_modifier(symbol: &Rc<MemberSymbol>, name: &str) -> Option<bool> {
    let found = RefCell::new(None);
    symbol
        .apply_recursive(&|node| {
            if let MemberSymbol::Measure(m) = node.as_ref() {
                if node.full_name() == name {
                    *found.borrow_mut() = Some(m.render_modifier().is_some());
                }
            }
            Ok(node.clone())
        })
        .unwrap();
    found.into_inner()
}

// A render form belongs to the measure as rendered in the select, so a
// measure reached through a dimension's expression tree takes the same
// form as the schema's own measure entries.
#[test]
fn render_modifier_reaches_measures_embedded_in_dimensions() {
    let ctx = ctx();
    let label = ctx.create_dimension("events.count_label").unwrap();
    let count = ctx.create_measure("events.count").unwrap();

    assert_eq!(
        has_render_modifier(&label, "events.count"),
        Some(false),
        "the dimension embeds an unstamped measure"
    );

    let schema = LogicalSchema::default()
        .set_dimensions(vec![label])
        .set_measures(vec![count])
        .into_rc();
    let stamped =
        measures_render_modifier_in_schema(&schema, &MeasureRenderModifier::UngroupedFinal)
            .unwrap();

    assert_eq!(
        has_render_modifier(&stamped.measures[0], "events.count"),
        Some(true)
    );
    assert_eq!(
        has_render_modifier(&stamped.dimensions[0], "events.count"),
        Some(true),
        "the embedded occurrence must carry the form too"
    );
}

// The mark is a property of the member in the select, not of its
// schema position: an occurrence embedded in another member's
// expression tree (a granularity reference) must be marked the same
// way as the schema's own time-dimension entry.
#[test]
fn tz_mark_reaches_time_dimensions_embedded_in_other_members() {
    let ctx = ctx();
    let td = ctx
        .create_time_dimension("events.created_at", Some("day"))
        .unwrap();
    let proxy = ctx.create_dimension("events.created_day").unwrap();

    assert_eq!(
        tz_mark_of(&proxy, "events.created_at_day"),
        Some(false),
        "the granularity reference embeds an unmarked time dimension"
    );

    let schema = LogicalSchema::default()
        .set_time_dimensions(vec![td])
        .set_dimensions(vec![proxy])
        .into_rc();
    let marked = mark_tz_converted_at_source_in_schema(&schema).unwrap();

    assert_eq!(
        tz_mark_of(&marked.time_dimensions[0], "events.created_at_day"),
        Some(true)
    );
    assert_eq!(
        tz_mark_of(&marked.dimensions[0], "events.created_at_day"),
        Some(true),
        "the embedded occurrence must carry the mark too"
    );
}
