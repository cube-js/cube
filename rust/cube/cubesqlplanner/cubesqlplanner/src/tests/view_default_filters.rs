use crate::planner::filter::FilterItem;
use crate::test_fixtures::cube_bridge::{
    MockDimensionDefinition, MockMeasureDefinition, MockSchemaBuilder, MockViewFilterDefinition,
};
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn build_schema_with_default_filter(filter: MockViewFilterDefinition) -> TestContext {
    let schema = MockSchemaBuilder::new()
        .add_cube("orders")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .add_dimension(
            "currency",
            MockDimensionDefinition::builder()
                .dimension_type("string".to_string())
                .sql("currency".to_string())
                .build(),
        )
        .add_dimension(
            "country",
            MockDimensionDefinition::builder()
                .dimension_type("string".to_string())
                .sql("country".to_string())
                .build(),
        )
        .add_measure(
            "count",
            MockMeasureDefinition::builder()
                .measure_type("count".to_string())
                .sql("COUNT(*)".to_string())
                .build(),
        )
        .finish_cube()
        .add_view("orders_view")
        .include_cube(
            "orders",
            vec![
                "id".to_string(),
                "currency".to_string(),
                "country".to_string(),
                "count".to_string(),
            ],
        )
        .add_default_filter(filter)
        .finish_view()
        .build();

    TestContext::new(schema).unwrap()
}

fn extract_member_paths(filters: &[FilterItem]) -> Vec<String> {
    filters
        .iter()
        .flat_map(|f| f.all_member_evaluators())
        .map(|m| m.full_name())
        .collect()
}

#[test]
fn test_default_filter_applies_when_view_is_active() {
    let ctx = build_schema_with_default_filter(
        MockViewFilterDefinition::builder()
            .operator("equals".to_string())
            .member_reference("orders_view.currency".to_string())
            .values_references(Some(vec![Some("USD".to_string())]))
            .build(),
    );

    let query = indoc! {"
        measures:
          - orders_view.count
    "};

    let props = ctx.create_query_properties(query).unwrap();
    let mentioned = extract_member_paths(props.dimensions_filters());
    assert_eq!(mentioned, vec!["orders_view.currency".to_string()]);
}

#[test]
fn test_default_filter_keeps_applying_when_unless_member_is_only_in_dimensions() {
    // `unless` is intentionally filter-only: pulling the member into the
    // projection does not release the guard, because doing so would make
    // the row set silently depend on which columns the user selects.
    let ctx = build_schema_with_default_filter(
        MockViewFilterDefinition::builder()
            .operator("equals".to_string())
            .member_reference("orders_view.currency".to_string())
            .values_references(Some(vec![Some("USD".to_string())]))
            .unless_references(Some(vec!["orders_view.currency".to_string()]))
            .build(),
    );

    let query = indoc! {"
        measures:
          - orders_view.count
        dimensions:
          - orders_view.currency
    "};

    let props = ctx.create_query_properties(query).unwrap();
    let mentioned = extract_member_paths(props.dimensions_filters());
    assert_eq!(mentioned, vec!["orders_view.currency".to_string()]);
}

#[test]
fn test_default_filter_released_when_explicit_filter_on_unless_member() {
    let ctx = build_schema_with_default_filter(
        MockViewFilterDefinition::builder()
            .operator("equals".to_string())
            .member_reference("orders_view.currency".to_string())
            .values_references(Some(vec![Some("USD".to_string())]))
            .unless_references(Some(vec!["orders_view.currency".to_string()]))
            .build(),
    );

    let query = indoc! {"
        measures:
          - orders_view.count
        filters:
          - member: orders_view.currency
            operator: equals
            values:
              - EUR
    "};

    let props = ctx.create_query_properties(query).unwrap();
    let mentioned = extract_member_paths(props.dimensions_filters());
    assert_eq!(mentioned, vec!["orders_view.currency".to_string()]);
    assert_eq!(
        props.dimensions_filters().len(),
        1,
        "default filter must be released when an explicit filter on the same member is present, \
         got {} filter(s)",
        props.dimensions_filters().len(),
    );
}

#[test]
fn test_default_filter_applies_when_unless_member_is_not_mentioned() {
    let ctx = build_schema_with_default_filter(
        MockViewFilterDefinition::builder()
            .operator("equals".to_string())
            .member_reference("orders_view.currency".to_string())
            .values_references(Some(vec![Some("USD".to_string())]))
            .unless_references(Some(vec!["orders_view.currency".to_string()]))
            .build(),
    );

    let query = indoc! {"
        measures:
          - orders_view.count
        dimensions:
          - orders_view.country
    "};

    let props = ctx.create_query_properties(query).unwrap();
    let mentioned = extract_member_paths(props.dimensions_filters());
    assert_eq!(mentioned, vec!["orders_view.currency".to_string()]);
}

#[test]
fn test_default_filter_applies_even_when_member_is_in_dimensions_without_unless() {
    let ctx = build_schema_with_default_filter(
        MockViewFilterDefinition::builder()
            .operator("equals".to_string())
            .member_reference("orders_view.currency".to_string())
            .values_references(Some(vec![Some("USD".to_string())]))
            .build(),
    );

    let query = indoc! {"
        measures:
          - orders_view.count
        dimensions:
          - orders_view.currency
    "};

    let props = ctx.create_query_properties(query).unwrap();
    let mentioned = extract_member_paths(props.dimensions_filters());
    assert_eq!(mentioned, vec!["orders_view.currency".to_string()]);
}
