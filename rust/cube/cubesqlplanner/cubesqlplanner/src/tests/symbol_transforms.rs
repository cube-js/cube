//! Tests for schema-level symbol transforms.

use crate::logical_plan::transforms::mark_tz_converted_at_source_in_schema;
use crate::logical_plan::LogicalSchema;
use crate::planner::MemberSymbol;
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use std::cell::RefCell;
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
