//! Tests for which filters a query reading subqueries applies itself.

use crate::logical_plan::transforms;
use crate::logical_plan::*;
use crate::planner::filter::FilterItem;
use crate::planner::planners::multi_stage::PlanningScope;
use crate::planner::planners::QueryPlanner;
use crate::planner::{MeasureRenderModifier, MemberSymbol, QueryAnalysis};
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;
use std::rc::Rc;

fn context() -> TestContext {
    TestContext::new(MockSchema::from_yaml_file(
        "common/integration_multi_stage_dimension_filter.yaml",
    ))
    .unwrap()
}

fn placement(yaml: &str, merges_legs: bool, selected: &[&str]) -> FilterPlacement {
    let context = context();
    let analysis = QueryAnalysis::try_new(context.create_query_properties(yaml).unwrap()).unwrap();
    let selected = selected
        .iter()
        .map(|path| context.create_dimension(path).unwrap())
        .collect::<Vec<Rc<MemberSymbol>>>();
    analysis.filter_placement(merges_legs, &selected).unwrap()
}

fn members(items: &[FilterItem]) -> Vec<String> {
    items
        .iter()
        .flat_map(|item| item.all_member_evaluators())
        .map(|member| member.full_name())
        .collect()
}

const QUERY: &str = indoc! {r#"
    dimensions:
      - activity.tier_ms_dim
    measures:
      - activity.theo_win
    timeDimensions:
      - dimension: activity.played_at
        dateRange:
          - "2024-01-01"
          - "2024-12-31"
    filters:
      - member: activity.player
        operator: equals
        values:
          - p2
      - and:
          - member: activity.tier_ms_dim
            operator: equals
            values:
              - low
          - member: activity.player
            operator: set
      - member: activity.theo_win
        operator: gt
        values:
          - "0"
"#};

#[test]
fn over_merged_legs_only_the_measure_filters_are_applied_to_rows() {
    let placement = placement(QUERY, true, &["activity.tier_ms_dim"]);

    assert!(!placement.aggregates());
    assert!(placement.unrenderable_reason().is_none());
    assert!(placement.own().dimensions_filters.is_empty());
    assert!(placement.own().time_dimensions_filters.is_empty());
    assert!(placement.own().segments.is_empty());
    assert_eq!(
        members(&placement.own().measures_filter),
        vec!["activity.theo_win"]
    );
}

#[test]
fn over_the_dimension_subquery_the_part_on_its_dimensions_is_applied_too() {
    let placement = placement(QUERY, false, &["activity.tier_ms_dim"]);

    assert!(placement.aggregates());
    assert!(placement.unrenderable_reason().is_none());
    // Only the multi-stage part of the AND group is left to the select.
    assert_eq!(
        members(&placement.own().dimensions_filters),
        vec!["activity.tier_ms_dim"]
    );
    assert!(placement.own().time_dimensions_filters.is_empty());
    assert_eq!(
        members(&placement.own().measures_filter),
        vec!["activity.theo_win"]
    );
}

#[test]
fn without_multi_stage_filters_only_the_measure_filters_are_applied() {
    let placement = placement(
        indoc! {r#"
            dimensions:
              - activity.tier_ms_dim
            filters:
              - member: activity.player
                operator: equals
                values:
                  - p2
        "#},
        false,
        &["activity.tier_ms_dim"],
    );

    assert!(placement.unrenderable_reason().is_none());
    assert!(placement.own().dimensions_filters.is_empty());
    assert!(placement.own().measures_filter.is_empty());
}

#[test]
fn a_filter_on_an_unselected_multi_stage_dimension_is_recorded_as_unrenderable() {
    let placement = placement(QUERY, false, &[]);

    assert_eq!(
        placement.unrenderable_reason().map(|reason| reason.as_str()),
        Some(
            "Filtering on multi-stage dimension activity.tier_ms_dim requires it to be selected as a dimension"
        )
    );
}

#[test]
fn an_or_group_mixing_multi_stage_and_other_members_is_recorded_as_unrenderable() {
    let placement = placement(
        indoc! {r#"
            dimensions:
              - activity.tier_ms_dim
            filters:
              - or:
                  - member: activity.tier_ms_dim
                    operator: equals
                    values:
                      - low
                  - member: activity.player
                    operator: equals
                    values:
                      - p2
        "#},
        false,
        &["activity.tier_ms_dim"],
    );

    assert!(placement.own().measures_filter.is_empty());
    let reason = placement.unrenderable_reason().unwrap();
    assert!(
        reason.contains("can't mix multi-stage dimensions with other members"),
        "{reason}"
    );
}

fn query_with_placement() -> Rc<Query> {
    let properties = context().create_query_properties(QUERY).unwrap();
    let filter = LogicalFilter {
        dimensions_filters: properties.dimensions_filters().clone(),
        ..Default::default()
    };
    Rc::new(
        Query::builder()
            .schema(LogicalSchema::default().into_rc())
            .filter(Rc::new(filter))
            .modifers(Rc::new(LogicalQueryModifiers {
                offset: None,
                limit: None,
                ungrouped: false,
                distinct: false,
                order_by: vec![],
            }))
            .from(LogicalFrom::empty())
            .placement(Some(Rc::new(FilterPlacement::unrenderable(
                "unrenderable".to_string(),
            ))))
            .build(),
    )
}

#[test]
fn the_select_applies_its_own_part_of_the_filter() {
    let query = query_with_placement();

    assert!(!query.applies_whole_filter());
    assert!(!Rc::ptr_eq(query.applied_filter(), query.filter()));
    assert!(query.applied_filter().dimensions_filters.is_empty());
}

#[test]
fn reading_rows_applies_all_filters_again() {
    let reading = query_with_placement().reading_rows_of(LogicalFrom::empty());

    assert!(reading.applies_whole_filter());
    assert!(Rc::ptr_eq(reading.applied_filter(), reading.filter()));
    assert!(reading.aggregates());
    assert!(reading.unrenderable_filter().is_none());
}

const UNRENDERABLE: &str = indoc! {r#"
    dimensions:
      - activity.tier_ms_dim
    filters:
      - member: activity.tier_label_ms_dim
        operator: equals
        values:
          - low
"#};

#[test]
fn an_unrenderable_filter_plans_and_fails_only_when_the_select_is_built() {
    let context = context();
    let request = context.create_query_properties(UNRENDERABLE).unwrap();
    let query = QueryPlanner::new(request, context.query_tools().clone())
        .plan(&mut PlanningScope::new())
        .unwrap();
    let reason = "Filtering on multi-stage dimension activity.tier_label_ms_dim requires it to be selected as a dimension";

    assert_eq!(
        query.unrenderable_filter().map(|r| r.as_str()),
        Some(reason)
    );
    let error = context.build_sql(UNRENDERABLE).unwrap_err();
    assert_eq!(error.message, reason);
}

#[test]
fn mapping_the_filters_maps_the_part_the_select_applies() {
    let context = context();
    let request = context
        .create_query_properties(indoc! {r#"
            dimensions:
              - activity.tier_ms_dim
            measures:
              - activity.theo_win
            filters:
              - member: activity.theo_win
                operator: gt
                values:
                  - "0"
        "#})
        .unwrap();
    let query = QueryPlanner::new(request, context.query_tools().clone())
        .plan(&mut PlanningScope::new())
        .unwrap();
    assert!(!query.applies_whole_filter());

    let stamped =
        transforms::measures_render_modifier_in_query(&query, &MeasureRenderModifier::RawValue)
            .unwrap();

    let measures_filter = &stamped.applied_filter().measures_filter;
    assert_eq!(measures_filter.len(), 1);
    let measure = measures_filter[0].all_member_evaluators()[0].clone();
    assert!(measure.as_measure().unwrap().render_modifier().is_some());
}
