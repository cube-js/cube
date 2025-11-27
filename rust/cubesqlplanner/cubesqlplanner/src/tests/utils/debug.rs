use crate::plan::filter::{FilterGroup, FilterGroupOperator};
use crate::plan::FilterItem;
use crate::planner::filter::{BaseFilter, FilterOperator};
use crate::planner::sql_evaluator::DebugSql;
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;

#[test]
fn test_dimension_basic() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let ctx = TestContext::new(schema).unwrap();
    let symbol = ctx.create_symbol("visitors.id").unwrap();
    let sql = symbol.debug_sql(false);
    assert_eq!(sql, "id");
}

#[test]
fn test_measure_basic() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let ctx = TestContext::new(schema).unwrap();
    let symbol = ctx.create_symbol("visitors.count").unwrap();
    let sql = symbol.debug_sql(false);
    assert_eq!(sql, "COUNT(*)");
}

#[test]
fn test_time_dimension_basic() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let ctx = TestContext::new(schema).unwrap();
    let symbol = ctx.create_symbol("visitors.created_at").unwrap();
    let sql = symbol.debug_sql(false);
    assert_eq!(sql, "created_at");
}

#[test]
fn test_geo_dimension() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let ctx = TestContext::new(schema).unwrap();
    let symbol = ctx.create_symbol("visitors.location").unwrap();
    let sql = symbol.debug_sql(false);
    assert_eq!(sql, "GEO(latitude, longitude)");
}

#[test]
fn test_proxy_dimension_collapsed() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let ctx = TestContext::new(schema).unwrap();
    let symbol = ctx.create_symbol("visitors.minVisitorCheckinDate").unwrap();
    let sql = symbol.debug_sql(false);
    assert_eq!(sql, "{visitor_checkins.minDate}");
}

#[test]
fn test_proxy_dimension_expanded() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let ctx = TestContext::new(schema).unwrap();
    let symbol = ctx.create_symbol("visitors.minVisitorCheckinDate").unwrap();
    let sql = symbol.debug_sql(true);
    assert_eq!(sql, "MIN(created_at)");
}

#[test]
fn test_visitor_id_proxy_collapsed() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let ctx = TestContext::new(schema).unwrap();
    let symbol = ctx.create_symbol("visitors.visitor_id_proxy").unwrap();
    let sql = symbol.debug_sql(false);
    assert_eq!(sql, "{visitors.visitor_id}");
}

#[test]
fn test_visitor_id_proxy_expanded() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let ctx = TestContext::new(schema).unwrap();
    let symbol = ctx.create_symbol("visitors.visitor_id_proxy").unwrap();
    let sql = symbol.debug_sql(true);
    assert_eq!(sql, "visitors.visitor_id");
}

#[test]
fn test_visitor_id_twice_collapsed() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let ctx = TestContext::new(schema).unwrap();
    let symbol = ctx.create_symbol("visitors.visitor_id_twice").unwrap();
    let sql = symbol.debug_sql(false);
    assert_eq!(sql, "{visitors.visitor_id} * 2");
}

#[test]
fn test_visitor_id_twice_expanded() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let ctx = TestContext::new(schema).unwrap();
    let symbol = ctx.create_symbol("visitors.visitor_id_twice").unwrap();
    let sql = symbol.debug_sql(true);
    assert_eq!(sql, "visitors.visitor_id * 2");
}

#[test]
fn test_total_revenue_per_count_collapsed() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let ctx = TestContext::new(schema).unwrap();
    let symbol = ctx
        .create_symbol("visitors.total_revenue_per_count")
        .unwrap();
    let sql = symbol.debug_sql(false);
    assert_eq!(sql, "{visitors.count} / {visitors.total_revenue}");
}

#[test]
fn test_total_revenue_per_count_expanded() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let ctx = TestContext::new(schema).unwrap();
    let symbol = ctx
        .create_symbol("visitors.total_revenue_per_count")
        .unwrap();
    let sql = symbol.debug_sql(true);
    assert_eq!(sql, "COUNT(*) / revenue");
}

#[test]
fn test_time_dimension() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let ctx = TestContext::new(schema).unwrap();
    let symbol = ctx.create_dimension("visitors.created_at.day").unwrap();
    let sql = symbol.debug_sql(true);
    assert_eq!(sql, "(created_at).day");
    let sql = symbol.debug_sql(false);
    assert_eq!(sql, "({visitors.created_at}).day");
}

#[test]
fn test_filter_simple_collapsed() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let ctx = TestContext::new(schema).unwrap();
    let symbol = ctx.create_symbol("visitors.source").unwrap();

    let filter = BaseFilter::try_new(
        ctx.query_tools().clone(),
        symbol,
        crate::planner::filter::base_filter::FilterType::Dimension,
        FilterOperator::Equal,
        Some(vec![Some("google".to_string())]),
    )
    .unwrap();

    let sql = filter.debug_sql(false);
    assert_eq!(sql, "{visitors.source} equals: ['google']");
}

#[test]
fn test_filter_simple_expanded() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let ctx = TestContext::new(schema).unwrap();
    let symbol = ctx.create_symbol("visitors.source").unwrap();

    let filter = BaseFilter::try_new(
        ctx.query_tools().clone(),
        symbol,
        crate::planner::filter::base_filter::FilterType::Dimension,
        FilterOperator::Equal,
        Some(vec![Some("google".to_string())]),
    )
    .unwrap();

    let sql = filter.debug_sql(true);
    assert_eq!(sql, "source equals: ['google']");
}

#[test]
fn test_filter_group_and_collapsed() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let ctx = TestContext::new(schema).unwrap();
    let source_symbol = ctx.create_symbol("visitors.source").unwrap();
    let id_symbol = ctx.create_symbol("visitors.id").unwrap();

    let filter1 = BaseFilter::try_new(
        ctx.query_tools().clone(),
        source_symbol,
        crate::planner::filter::base_filter::FilterType::Dimension,
        FilterOperator::Equal,
        Some(vec![Some("google".to_string())]),
    )
    .unwrap();

    let filter2 = BaseFilter::try_new(
        ctx.query_tools().clone(),
        id_symbol,
        crate::planner::filter::base_filter::FilterType::Dimension,
        FilterOperator::Gt,
        Some(vec![Some("100".to_string())]),
    )
    .unwrap();

    let group = FilterGroup::new(
        FilterGroupOperator::And,
        vec![FilterItem::Item(filter1), FilterItem::Item(filter2)],
    );

    let sql = group.debug_sql(false);
    let expected =
        "AND: [\n  {visitors.source} equals: ['google'],\n  {visitors.id} gt: ['100']\n]";
    assert_eq!(sql, expected);
}
