use crate::planner::filter::base_filter::{BaseFilter, FilterType};
use crate::planner::filter::filter_operator::FilterOperator;
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;

fn create_ctx() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    TestContext::new(schema).unwrap()
}

fn assert_raw(result: &(String, Vec<String>), expected_sql: &str) {
    assert_eq!(result.0, expected_sql, "SQL mismatch");
    assert!(
        result.1.is_empty(),
        "Expected no allocated params for raw values, got: {:?}",
        result.1
    );
}

#[test]
fn test_in_date_range_use_raw_values() {
    let ctx = create_ctx();
    let symbol = ctx.create_symbol("visitors.created_at").unwrap();

    let filter = BaseFilter::try_new(
        ctx.query_tools().clone(),
        symbol,
        FilterType::Dimension,
        FilterOperator::InDateRange,
        Some(vec![
            Some("2024-01-01".to_string()),
            Some("2024-12-31".to_string()),
        ]),
    )
    .unwrap();

    let raw_filter = filter
        .change_operator(
            FilterOperator::InDateRange,
            vec![
                Some("(SELECT min(df) FROM cte)".to_string()),
                Some("(SELECT max(dt) FROM cte)".to_string()),
            ],
            true,
        )
        .unwrap();

    let result = ctx.build_base_filter_sql(&raw_filter).unwrap();
    assert_raw(
        &result,
        r#""visitors".created_at >= (SELECT min(df) FROM cte) AND "visitors".created_at <= (SELECT max(dt) FROM cte)"#,
    );
}

#[test]
fn test_not_in_date_range_use_raw_values() {
    let ctx = create_ctx();
    let symbol = ctx.create_symbol("visitors.created_at").unwrap();

    let filter = BaseFilter::try_new(
        ctx.query_tools().clone(),
        symbol,
        FilterType::Dimension,
        FilterOperator::NotInDateRange,
        Some(vec![
            Some("2024-01-01".to_string()),
            Some("2024-12-31".to_string()),
        ]),
    )
    .unwrap();

    let raw_filter = filter
        .change_operator(
            FilterOperator::NotInDateRange,
            vec![
                Some("(SELECT min(df) FROM cte)".to_string()),
                Some("(SELECT max(dt) FROM cte)".to_string()),
            ],
            true,
        )
        .unwrap();

    let result = ctx.build_base_filter_sql(&raw_filter).unwrap();
    assert_raw(
        &result,
        r#""visitors".created_at < (SELECT min(df) FROM cte) OR "visitors".created_at > (SELECT max(dt) FROM cte)"#,
    );
}

#[test]
fn test_before_or_on_date_use_raw_values() {
    let ctx = create_ctx();
    let symbol = ctx.create_symbol("visitors.created_at").unwrap();

    let filter = BaseFilter::try_new(
        ctx.query_tools().clone(),
        symbol,
        FilterType::Dimension,
        FilterOperator::InDateRange,
        Some(vec![
            Some("2024-01-01".to_string()),
            Some("2024-12-31".to_string()),
        ]),
    )
    .unwrap();

    let raw_filter = filter
        .change_operator(
            FilterOperator::BeforeOrOnDate,
            vec![Some("(SELECT max(dt) FROM cte)".to_string())],
            true,
        )
        .unwrap();

    let result = ctx.build_base_filter_sql(&raw_filter).unwrap();
    assert_raw(
        &result,
        r#""visitors".created_at <= (SELECT max(dt) FROM cte)"#,
    );
}

#[test]
fn test_after_or_on_date_use_raw_values() {
    let ctx = create_ctx();
    let symbol = ctx.create_symbol("visitors.created_at").unwrap();

    let filter = BaseFilter::try_new(
        ctx.query_tools().clone(),
        symbol,
        FilterType::Dimension,
        FilterOperator::InDateRange,
        Some(vec![
            Some("2024-01-01".to_string()),
            Some("2024-12-31".to_string()),
        ]),
    )
    .unwrap();

    let raw_filter = filter
        .change_operator(
            FilterOperator::AfterOrOnDate,
            vec![Some("(SELECT min(df) FROM cte)".to_string())],
            true,
        )
        .unwrap();

    let result = ctx.build_base_filter_sql(&raw_filter).unwrap();
    assert_raw(
        &result,
        r#""visitors".created_at >= (SELECT min(df) FROM cte)"#,
    );
}
