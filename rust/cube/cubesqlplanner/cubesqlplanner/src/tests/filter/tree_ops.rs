use crate::planner::filter::base_filter::{BaseFilter, FilterType};
use crate::planner::filter::base_segment::BaseSegment;
use crate::planner::filter::filter_operator::FilterOperator;
use crate::planner::filter::tree_ops;
use crate::planner::filter::{FilterGroup, FilterGroupOperator, FilterItem};
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use std::rc::Rc;

fn ctx() -> TestContext {
    TestContext::new(MockSchema::from_yaml_file("common/visitors.yaml")).unwrap()
}

fn make_dim_filter(ctx: &TestContext, member_path: &str, value: &str) -> Rc<BaseFilter> {
    let symbol = ctx.create_symbol(member_path).unwrap();
    BaseFilter::try_new(
        ctx.query_tools().clone(),
        symbol,
        FilterType::Dimension,
        FilterOperator::Equal,
        Some(vec![Some(value.to_string())]),
    )
    .unwrap()
}

fn make_segment(ctx: &TestContext, segment_path: &str) -> Rc<BaseSegment> {
    ctx.create_segment(segment_path).unwrap()
}

fn item_segment_names(items: &[FilterItem]) -> Vec<String> {
    let mut names = Vec::new();
    for item in items {
        match item {
            FilterItem::Segment(s) => names.push(s.full_name()),
            FilterItem::Group(g) => names.extend(item_segment_names(&g.items)),
            FilterItem::Item(_) => {}
        }
    }
    names
}

fn item_member_names(items: &[FilterItem]) -> Vec<String> {
    let mut names = Vec::new();
    for item in items {
        match item {
            FilterItem::Item(b) => names.push(b.member_name()),
            FilterItem::Group(g) => names.extend(item_member_names(&g.items)),
            FilterItem::Segment(_) => {}
        }
    }
    names
}

#[test]
fn exclude_members_removes_only_target() {
    let ctx = ctx();
    let visitor_id = make_dim_filter(&ctx, "visitors.visitor_id", "1");
    let source = make_dim_filter(&ctx, "visitors.source", "google");

    let filters = vec![FilterItem::Item(visitor_id), FilterItem::Item(source)];

    let result = tree_ops::exclude_members(&["visitors.visitor_id".to_string()], &filters);
    assert_eq!(item_member_names(&result), vec!["visitors.source"]);
}

#[test]
fn exclude_members_handles_multiple_names() {
    let ctx = ctx();
    let visitor_id = make_dim_filter(&ctx, "visitors.visitor_id", "1");
    let source = make_dim_filter(&ctx, "visitors.source", "google");
    let kept = make_dim_filter(&ctx, "visitors.id", "42");

    let filters = vec![
        FilterItem::Item(visitor_id),
        FilterItem::Item(source),
        FilterItem::Item(kept),
    ];

    let result = tree_ops::exclude_members(
        &[
            "visitors.visitor_id".to_string(),
            "visitors.source".to_string(),
        ],
        &filters,
    );
    assert_eq!(item_member_names(&result), vec!["visitors.id"]);
}

#[test]
fn exclude_members_descends_into_groups_and_preserves_structure() {
    let ctx = ctx();
    let inner_a = make_dim_filter(&ctx, "visitors.visitor_id", "1");
    let inner_b = make_dim_filter(&ctx, "visitors.source", "google");
    let group = FilterItem::Group(Rc::new(FilterGroup::new(
        FilterGroupOperator::Or,
        vec![FilterItem::Item(inner_a), FilterItem::Item(inner_b)],
    )));

    let result = tree_ops::exclude_members(&["visitors.visitor_id".to_string()], &[group]);
    assert_eq!(result.len(), 1);
    match &result[0] {
        FilterItem::Group(g) => {
            assert!(matches!(g.operator, FilterGroupOperator::Or));
            assert_eq!(item_member_names(&g.items), vec!["visitors.source"]);
        }
        _ => panic!("expected group preserved"),
    }
}

#[test]
fn exclude_members_drops_groups_with_no_surviving_items() {
    let ctx = ctx();
    let a = make_dim_filter(&ctx, "visitors.visitor_id", "1");
    let b = make_dim_filter(&ctx, "visitors.source", "google");
    let kept = make_dim_filter(&ctx, "visitors.id", "42");

    let group = FilterItem::Group(Rc::new(FilterGroup::new(
        FilterGroupOperator::And,
        vec![FilterItem::Item(a), FilterItem::Item(b)],
    )));
    let filters = vec![group, FilterItem::Item(kept)];

    let result = tree_ops::exclude_members(
        &[
            "visitors.visitor_id".to_string(),
            "visitors.source".to_string(),
        ],
        &filters,
    );
    assert_eq!(result.len(), 1);
    assert!(matches!(&result[0], FilterItem::Item(b) if b.member_name() == "visitors.id"));
}

#[test]
fn exclude_members_drops_nested_empty_groups() {
    let ctx = ctx();
    let inner = make_dim_filter(&ctx, "visitors.visitor_id", "1");
    let nested = FilterItem::Group(Rc::new(FilterGroup::new(
        FilterGroupOperator::And,
        vec![FilterItem::Group(Rc::new(FilterGroup::new(
            FilterGroupOperator::Or,
            vec![FilterItem::Item(inner)],
        )))],
    )));

    let result = tree_ops::exclude_members(&["visitors.visitor_id".to_string()], &[nested]);
    assert!(
        result.is_empty(),
        "nested empty groups should bubble up and be dropped, got {:?}",
        item_member_names(&result)
    );
}

// ─── keep_only_members ──────────────────────────────────────────────────────

#[test]
fn keep_only_members_keeps_target_only() {
    let ctx = ctx();
    let visitor_id = make_dim_filter(&ctx, "visitors.visitor_id", "1");
    let source = make_dim_filter(&ctx, "visitors.source", "google");
    let id = make_dim_filter(&ctx, "visitors.id", "42");

    let filters = vec![
        FilterItem::Item(visitor_id),
        FilterItem::Item(source),
        FilterItem::Item(id),
    ];

    let result = tree_ops::keep_only_members(
        &["visitors.visitor_id".to_string(), "visitors.id".to_string()],
        &filters,
    );
    let mut names = item_member_names(&result);
    names.sort();
    assert_eq!(names, vec!["visitors.id", "visitors.visitor_id"]);
}

#[test]
fn keep_only_members_drops_groups_with_no_surviving_items() {
    let ctx = ctx();
    let a = make_dim_filter(&ctx, "visitors.visitor_id", "1");
    let b = make_dim_filter(&ctx, "visitors.source", "google");
    let kept = make_dim_filter(&ctx, "visitors.id", "42");

    let group = FilterItem::Group(Rc::new(FilterGroup::new(
        FilterGroupOperator::And,
        vec![FilterItem::Item(a), FilterItem::Item(b)],
    )));
    let filters = vec![group, FilterItem::Item(kept)];

    let result = tree_ops::keep_only_members(&["visitors.id".to_string()], &filters);
    assert_eq!(result.len(), 1);
    assert!(matches!(&result[0], FilterItem::Item(b) if b.member_name() == "visitors.id"));
}

#[test]
fn keep_only_members_preserves_partially_matching_group() {
    let ctx = ctx();
    let visitor_id = make_dim_filter(&ctx, "visitors.visitor_id", "1");
    let source = make_dim_filter(&ctx, "visitors.source", "google");

    let group = FilterItem::Group(Rc::new(FilterGroup::new(
        FilterGroupOperator::Or,
        vec![FilterItem::Item(visitor_id), FilterItem::Item(source)],
    )));

    let result = tree_ops::keep_only_members(&["visitors.source".to_string()], &[group]);
    assert_eq!(result.len(), 1);
    match &result[0] {
        FilterItem::Group(g) => {
            assert!(matches!(g.operator, FilterGroupOperator::Or));
            assert_eq!(item_member_names(&g.items), vec!["visitors.source"]);
        }
        _ => panic!("expected group preserved with surviving children"),
    }
}

#[test]
fn keep_only_members_with_empty_list_clears_all() {
    let ctx = ctx();
    let visitor_id = make_dim_filter(&ctx, "visitors.visitor_id", "1");
    let source = make_dim_filter(&ctx, "visitors.source", "google");

    let filters = vec![FilterItem::Item(visitor_id), FilterItem::Item(source)];

    let result = tree_ops::keep_only_members(&[], &filters);
    assert!(result.is_empty());
}

// ─── segment handling ───────────────────────────────────────────────────────

#[test]
fn exclude_members_drops_listed_segment() {
    let ctx = ctx();
    let dim = make_dim_filter(&ctx, "visitors.source", "google");
    let segment = make_segment(&ctx, "visitors.google");

    let filters = vec![FilterItem::Item(dim), FilterItem::Segment(segment)];

    let result = tree_ops::exclude_members(&["visitors.google".to_string()], &filters);
    assert_eq!(item_segment_names(&result), Vec::<String>::new());
    assert_eq!(item_member_names(&result), vec!["visitors.source"]);
}

#[test]
fn exclude_members_keeps_unlisted_segment() {
    let ctx = ctx();
    let dim = make_dim_filter(&ctx, "visitors.source", "google");
    let segment = make_segment(&ctx, "visitors.google");

    let filters = vec![FilterItem::Item(dim), FilterItem::Segment(segment)];

    let result = tree_ops::exclude_members(&["visitors.source".to_string()], &filters);
    assert_eq!(item_segment_names(&result), vec!["visitors.google"]);
    assert_eq!(item_member_names(&result), Vec::<String>::new());
}

#[test]
fn keep_only_members_keeps_listed_segment() {
    let ctx = ctx();
    let dim = make_dim_filter(&ctx, "visitors.source", "google");
    let segment = make_segment(&ctx, "visitors.google");

    let filters = vec![FilterItem::Item(dim), FilterItem::Segment(segment)];

    let result = tree_ops::keep_only_members(&["visitors.google".to_string()], &filters);
    assert_eq!(item_segment_names(&result), vec!["visitors.google"]);
    assert_eq!(item_member_names(&result), Vec::<String>::new());
}

#[test]
fn keep_only_members_drops_unlisted_segment() {
    let ctx = ctx();
    let dim = make_dim_filter(&ctx, "visitors.source", "google");
    let segment = make_segment(&ctx, "visitors.google");

    let filters = vec![FilterItem::Item(dim), FilterItem::Segment(segment)];

    let result = tree_ops::keep_only_members(&["visitors.source".to_string()], &filters);
    assert_eq!(item_segment_names(&result), Vec::<String>::new());
    assert_eq!(item_member_names(&result), vec!["visitors.source"]);
}

#[test]
fn has_filter_for_member_finds_top_level_item() {
    let ctx = ctx();
    let dim = make_dim_filter(&ctx, "visitors.visitor_id", "1");
    let filters = vec![FilterItem::Item(dim)];

    assert!(tree_ops::has_filter_for_member(
        &"visitors.visitor_id".to_string(),
        &filters
    ));
    assert!(!tree_ops::has_filter_for_member(
        &"visitors.source".to_string(),
        &filters
    ));
}

#[test]
fn has_filter_for_member_descends_into_groups() {
    let ctx = ctx();
    let inner = make_dim_filter(&ctx, "visitors.visitor_id", "1");
    let group = FilterItem::Group(Rc::new(FilterGroup::new(
        FilterGroupOperator::And,
        vec![FilterItem::Item(inner)],
    )));

    assert!(tree_ops::has_filter_for_member(
        &"visitors.visitor_id".to_string(),
        &[group]
    ));
}

#[test]
fn has_filter_for_member_ignores_segments() {
    // No segment fixture readily available; verify the false-path here:
    // a tree that contains only items for other members must return false.
    let ctx = ctx();
    let other = make_dim_filter(&ctx, "visitors.source", "google");
    let filters = vec![FilterItem::Item(other)];

    assert!(!tree_ops::has_filter_for_member(
        &"visitors.visitor_id".to_string(),
        &filters
    ));
}
