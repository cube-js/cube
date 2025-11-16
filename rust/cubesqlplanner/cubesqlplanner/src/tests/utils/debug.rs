use crate::planner::sql_evaluator::DebugSql;
use crate::test_fixtures::schemas::create_visitors_schema;
use crate::test_fixtures::test_utils::TestContext;

#[test]
fn test_dimension_basic() {
    let ctx = TestContext::new(create_visitors_schema()).unwrap();
    let symbol = ctx.create_symbol("visitors.id").unwrap();
    let sql = symbol.debug_sql(false);
    assert_eq!(sql, "id");
}

#[test]
fn test_measure_basic() {
    let ctx = TestContext::new(create_visitors_schema()).unwrap();
    let symbol = ctx.create_symbol("visitors.count").unwrap();
    let sql = symbol.debug_sql(false);
    assert_eq!(sql, "COUNT(*)");
}

#[test]
fn test_time_dimension_basic() {
    let ctx = TestContext::new(create_visitors_schema()).unwrap();
    let symbol = ctx.create_symbol("visitors.created_at").unwrap();
    let sql = symbol.debug_sql(false);
    assert_eq!(sql, "created_at");
}

#[test]
fn test_geo_dimension() {
    let ctx = TestContext::new(create_visitors_schema()).unwrap();
    let symbol = ctx.create_symbol("visitors.location").unwrap();
    let sql = symbol.debug_sql(false);
    assert_eq!(sql, "GEO(latitude, longitude)");
}

#[test]
fn test_proxy_dimension_collapsed() {
    let ctx = TestContext::new(create_visitors_schema()).unwrap();
    let symbol = ctx.create_symbol("visitors.minVisitorCheckinDate").unwrap();
    let sql = symbol.debug_sql(false);
    assert_eq!(sql, "{visitor_checkins.minDate}");
}

#[test]
fn test_proxy_dimension_expanded() {
    let ctx = TestContext::new(create_visitors_schema()).unwrap();
    let symbol = ctx.create_symbol("visitors.minVisitorCheckinDate").unwrap();
    let sql = symbol.debug_sql(true);
    assert_eq!(sql, "MIN(created_at)");
}

#[test]
fn test_visitor_id_proxy_collapsed() {
    let ctx = TestContext::new(create_visitors_schema()).unwrap();
    let symbol = ctx.create_symbol("visitors.visitor_id_proxy").unwrap();
    let sql = symbol.debug_sql(false);
    assert_eq!(sql, "{visitors.visitor_id}");
}

#[test]
fn test_visitor_id_proxy_expanded() {
    let ctx = TestContext::new(create_visitors_schema()).unwrap();
    let symbol = ctx.create_symbol("visitors.visitor_id_proxy").unwrap();
    let sql = symbol.debug_sql(true);
    assert_eq!(sql, "visitors.visitor_id");
}

#[test]
fn test_visitor_id_twice_collapsed() {
    let ctx = TestContext::new(create_visitors_schema()).unwrap();
    let symbol = ctx.create_symbol("visitors.visitor_id_twice").unwrap();
    let sql = symbol.debug_sql(false);
    assert_eq!(sql, "{visitors.visitor_id} * 2");
}

#[test]
fn test_visitor_id_twice_expanded() {
    let ctx = TestContext::new(create_visitors_schema()).unwrap();
    let symbol = ctx.create_symbol("visitors.visitor_id_twice").unwrap();
    let sql = symbol.debug_sql(true);
    assert_eq!(sql, "visitors.visitor_id * 2");
}

#[test]
fn test_total_revenue_per_count_collapsed() {
    let ctx = TestContext::new(create_visitors_schema()).unwrap();
    let symbol = ctx
        .create_symbol("visitors.total_revenue_per_count")
        .unwrap();
    let sql = symbol.debug_sql(false);
    assert_eq!(sql, "{visitors.count} / {visitors.total_revenue}");
}

#[test]
fn test_total_revenue_per_count_expanded() {
    let ctx = TestContext::new(create_visitors_schema()).unwrap();
    let symbol = ctx
        .create_symbol("visitors.total_revenue_per_count")
        .unwrap();
    let sql = symbol.debug_sql(true);
    assert_eq!(sql, "COUNT(*) / revenue");
}

#[test]
fn test_time_dimension() {
    let ctx = TestContext::new(create_visitors_schema()).unwrap();
    let symbol = ctx.create_symbol("visitors.created_at.day").unwrap();
    let sql = symbol.debug_sql(true);
    assert_eq!(sql, "(created_at).day");
    let sql = symbol.debug_sql(false);
    assert_eq!(sql, "({visitors.created_at}).day");
}
