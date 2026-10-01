use crate::planner::filter::FilterItem;
use crate::planner::MemberId;
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::{member_id, TestContext};
use indoc::indoc;

fn ctx() -> TestContext {
    TestContext::new(MockSchema::from_yaml_file("common/simple.yaml")).unwrap()
}

fn filter_member_ids(filters: &[FilterItem]) -> Vec<MemberId> {
    filters
        .iter()
        .filter_map(|item| match item {
            FilterItem::Item(filter) => Some(filter.member_id()),
            _ => None,
        })
        .collect()
}

#[test]
fn filter_on_a_measure_through_a_join_path_is_a_measure_filter() {
    let props = ctx()
        .create_query_properties(indoc! {"
            measures:
              - orders.count
            filters:
              - member: orders.customers.max_age
                operator: gt
                values:
                  - \"30\"
        "})
        .unwrap();

    assert_eq!(
        filter_member_ids(props.measures_filters()),
        vec![member_id("customers.max_age")]
    );
    assert!(props.dimensions_filters().is_empty());
}

#[test]
fn filter_on_a_dimension_through_a_join_path_is_a_dimension_filter() {
    let props = ctx()
        .create_query_properties(indoc! {"
            measures:
              - orders.count
            filters:
              - member: orders.customers.name
                operator: equals
                values:
                  - Alice
        "})
        .unwrap();

    assert_eq!(
        filter_member_ids(props.dimensions_filters()),
        vec![member_id("customers.name")]
    );
    assert!(props.measures_filters().is_empty());
}
