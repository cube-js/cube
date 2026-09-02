use crate::cube_bridge::base_query_options::FilterValue;
use crate::planner::filter::{
    BaseFilter, FilterGroup, FilterGroupOperator, FilterItem, FilterOperator,
};
use crate::planner::DebugSql;
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use cubenativeutils::CubeError;
use std::rc::Rc;

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
        ctx.query_tools().query_tools().clone(),
        symbol,
        crate::planner::filter::base_filter::FilterType::Dimension,
        FilterOperator::Equal,
        Some(vec![FilterValue::Str("google".to_string())]),
        None,
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
        ctx.query_tools().query_tools().clone(),
        symbol,
        crate::planner::filter::base_filter::FilterType::Dimension,
        FilterOperator::Equal,
        Some(vec![FilterValue::Str("google".to_string())]),
        None,
    )
    .unwrap();

    let sql = filter.debug_sql(true);
    assert_eq!(sql, "source equals: ['google']");
}

#[test]
fn test_filter_bool_and_num_values_unquoted() -> Result<(), CubeError> {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let ctx = TestContext::new(schema)?;
    let symbol = ctx.create_symbol("visitors.id")?;

    // Booleans and numbers render bare; only strings are quoted.
    let filter = BaseFilter::try_new(
        ctx.query_tools().query_tools().clone(),
        symbol,
        crate::planner::filter::base_filter::FilterType::Dimension,
        FilterOperator::In,
        Some(vec![
            FilterValue::Num(42.0),
            FilterValue::Bool(true),
            FilterValue::Str("google".to_string()),
        ]),
        None,
    )?;

    let sql = filter.debug_sql(false);
    assert_eq!(sql, "{visitors.id} in: [42, true, 'google']");

    Ok(())
}

#[test]
fn test_filter_null_value_renders_null() -> Result<(), CubeError> {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let ctx = TestContext::new(schema)?;
    let symbol = ctx.create_symbol("visitors.source")?;

    let filter = BaseFilter::try_new(
        ctx.query_tools().query_tools().clone(),
        symbol,
        crate::planner::filter::base_filter::FilterType::Dimension,
        FilterOperator::Equal,
        Some(vec![FilterValue::Null]),
        None,
    )?;

    let sql = filter.debug_sql(false);
    assert_eq!(sql, "{visitors.source} equals: [NULL]");

    Ok(())
}

#[test]
fn test_filter_group_and_collapsed() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let ctx = TestContext::new(schema).unwrap();
    let source_symbol = ctx.create_symbol("visitors.source").unwrap();
    let id_symbol = ctx.create_symbol("visitors.id").unwrap();

    let filter1 = BaseFilter::try_new(
        ctx.query_tools().query_tools().clone(),
        source_symbol,
        crate::planner::filter::base_filter::FilterType::Dimension,
        FilterOperator::Equal,
        Some(vec![FilterValue::Str("google".to_string())]),
        None,
    )
    .unwrap();

    let filter2 = BaseFilter::try_new(
        ctx.query_tools().query_tools().clone(),
        id_symbol,
        crate::planner::filter::base_filter::FilterType::Dimension,
        FilterOperator::Gt,
        Some(vec![FilterValue::Str("100".to_string())]),
        None,
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

#[test]
fn test_find_subtree_for_members_excludes_segments_from_and_group() {
    let schema = MockSchema::from_yaml_file("common/integration_basic.yaml");
    let ctx = TestContext::new(schema).unwrap();
    let city_symbol = ctx.create_symbol("customers.city").unwrap();
    let amount_symbol = ctx.create_symbol("orders.amount").unwrap();

    let city_filter = BaseFilter::try_new(
        ctx.query_tools().query_tools().clone(),
        city_symbol.clone(),
        crate::planner::filter::base_filter::FilterType::Dimension,
        FilterOperator::Equal,
        Some(vec![FilterValue::Str("New York".to_string())]),
        None,
    )
    .unwrap();

    let amount_filter = BaseFilter::try_new(
        ctx.query_tools().query_tools().clone(),
        amount_symbol.clone(),
        crate::planner::filter::base_filter::FilterType::Dimension,
        FilterOperator::Gte,
        Some(vec![FilterValue::Str("100".to_string())]),
        None,
    )
    .unwrap();

    let completed_segment = ctx.create_segment("orders.completed_orders").unwrap();

    let filter_tree = FilterItem::Group(Rc::new(FilterGroup::new(
        FilterGroupOperator::And,
        vec![
            FilterItem::Item(city_filter),
            FilterItem::Item(amount_filter),
            FilterItem::Segment(completed_segment),
        ],
    )));

    let city_member = city_symbol.clone().resolve_reference_chain().full_name();
    let amount_member = amount_symbol.clone().resolve_reference_chain().full_name();
    let targets = vec![&city_member, &amount_member];

    let subtree = filter_tree
        .find_subtree_for_members(&targets)
        .expect("matching filters should still be extracted");

    assert_eq!(
        subtree.debug_sql(false),
        "AND: [\n  {customers.city} equals: ['New York'],\n  {orders.amount} gte: ['100']\n]"
    );
}

#[test]
fn test_find_subtree_for_members_keeps_or_groups_all_or_nothing() {
    let schema = MockSchema::from_yaml_file("common/integration_basic.yaml");
    let ctx = TestContext::new(schema).unwrap();
    let city_symbol = ctx.create_symbol("customers.city").unwrap();

    let city_filter = BaseFilter::try_new(
        ctx.query_tools().query_tools().clone(),
        city_symbol.clone(),
        crate::planner::filter::base_filter::FilterType::Dimension,
        FilterOperator::Equal,
        Some(vec![FilterValue::Str("New York".to_string())]),
        None,
    )
    .unwrap();

    let completed_segment = ctx.create_segment("orders.completed_orders").unwrap();

    let filter_tree = FilterItem::Group(Rc::new(FilterGroup::new(
        FilterGroupOperator::Or,
        vec![
            FilterItem::Item(city_filter),
            FilterItem::Segment(completed_segment),
        ],
    )));

    let city_member = city_symbol.clone().resolve_reference_chain().full_name();
    let targets = vec![&city_member];

    assert!(
        filter_tree.find_subtree_for_members(&targets).is_none(),
        "partial matches should not be extracted from OR groups"
    );
}

#[test]
fn test_find_subtree_for_members_rejects_or_groups_with_partially_matching_children() {
    let schema = MockSchema::from_yaml_file("common/integration_basic.yaml");
    let ctx = TestContext::new(schema).unwrap();
    let city_symbol = ctx.create_symbol("customers.city").unwrap();
    let amount_symbol = ctx.create_symbol("orders.amount").unwrap();

    let city_filter = BaseFilter::try_new(
        ctx.query_tools().query_tools().clone(),
        city_symbol.clone(),
        crate::planner::filter::base_filter::FilterType::Dimension,
        FilterOperator::Equal,
        Some(vec![FilterValue::Str("New York".to_string())]),
        None,
    )
    .unwrap();

    let amount_filter = BaseFilter::try_new(
        ctx.query_tools().query_tools().clone(),
        amount_symbol.clone(),
        crate::planner::filter::base_filter::FilterType::Dimension,
        FilterOperator::Gte,
        Some(vec![FilterValue::Str("100".to_string())]),
        None,
    )
    .unwrap();

    let completed_segment = ctx.create_segment("orders.completed_orders").unwrap();

    let partially_matching_branch = FilterItem::Group(Rc::new(FilterGroup::new(
        FilterGroupOperator::And,
        vec![
            FilterItem::Item(city_filter),
            FilterItem::Segment(completed_segment),
        ],
    )));

    let filter_tree = FilterItem::Group(Rc::new(FilterGroup::new(
        FilterGroupOperator::Or,
        vec![partially_matching_branch, FilterItem::Item(amount_filter)],
    )));

    let city_member = city_symbol.clone().resolve_reference_chain().full_name();
    let amount_member = amount_symbol.clone().resolve_reference_chain().full_name();
    let targets = vec![&city_member, &amount_member];

    assert!(
        filter_tree.find_subtree_for_members(&targets).is_none(),
        "OR groups should only match when every branch matches without pruning"
    );
}

#[test]
fn test_find_subtree_for_members_segment_only_and_group_returns_none() {
    let schema = MockSchema::from_yaml_file("common/integration_basic.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let completed_segment = ctx.create_segment("orders.completed_orders").unwrap();

    let filter_tree = FilterItem::Group(Rc::new(FilterGroup::new(
        FilterGroupOperator::And,
        vec![FilterItem::Segment(completed_segment)],
    )));

    let dummy_member = "orders.amount".to_string();
    let targets = vec![&dummy_member];

    assert!(
        filter_tree.find_subtree_for_members(&targets).is_none(),
        "AND group containing only segments should return None"
    );
}

#[test]
fn test_find_subtree_for_members_all_matching_and_group_preserves_group() {
    let schema = MockSchema::from_yaml_file("common/integration_basic.yaml");
    let ctx = TestContext::new(schema).unwrap();
    let city_symbol = ctx.create_symbol("customers.city").unwrap();
    let amount_symbol = ctx.create_symbol("orders.amount").unwrap();

    let city_filter = BaseFilter::try_new(
        ctx.query_tools().query_tools().clone(),
        city_symbol.clone(),
        crate::planner::filter::base_filter::FilterType::Dimension,
        FilterOperator::Equal,
        Some(vec![FilterValue::Str("New York".to_string())]),
        None,
    )
    .unwrap();

    let amount_filter = BaseFilter::try_new(
        ctx.query_tools().query_tools().clone(),
        amount_symbol.clone(),
        crate::planner::filter::base_filter::FilterType::Dimension,
        FilterOperator::Gte,
        Some(vec![FilterValue::Str("100".to_string())]),
        None,
    )
    .unwrap();

    let filter_tree = FilterItem::Group(Rc::new(FilterGroup::new(
        FilterGroupOperator::And,
        vec![
            FilterItem::Item(city_filter),
            FilterItem::Item(amount_filter),
        ],
    )));

    let city_member = city_symbol.clone().resolve_reference_chain().full_name();
    let amount_member = amount_symbol.clone().resolve_reference_chain().full_name();
    let targets = vec![&city_member, &amount_member];

    let subtree = filter_tree
        .find_subtree_for_members(&targets)
        .expect("fully matching AND group should be returned");

    let expected_sql =
        "AND: [\n  {customers.city} equals: ['New York'],\n  {orders.amount} gte: ['100']\n]";
    assert_eq!(subtree.debug_sql(false), expected_sql);

    assert!(
        subtree == filter_tree,
        "fully matching group should be structurally identical to the original"
    );
}
