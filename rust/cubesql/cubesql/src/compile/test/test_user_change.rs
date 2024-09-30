//! Tests that check user change via __user virtual column

use cubeclient::models::{V1LoadRequestQuery, V1LoadRequestQueryFilterItem};
use pretty_assertions::assert_eq;

use crate::compile::{
    convert_sql_to_cube_query,
    test::{
        convert_select_to_query_plan, get_test_session, get_test_tenant_ctx, init_testing_logger,
        utils::LogicalPlanTestUtils, TestContext,
    },
    DatabaseProtocol, Rewriter,
};

#[tokio::test]
async fn test_change_user_via_filter() {
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce WHERE __user = 'gopher'".to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await;

    let cube_scan = query_plan.as_logical_plan().find_cube_scan();

    assert_eq!(cube_scan.options.change_user, Some("gopher".to_string()));

    assert_eq!(
        cube_scan.request,
        V1LoadRequestQuery {
            measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
            segments: Some(vec![]),
            dimensions: Some(vec![]),
            time_dimensions: None,
            order: Some(vec![]),
            limit: None,
            offset: None,
            filters: None,
            ungrouped: None,
        }
    )
}

#[tokio::test]
async fn test_change_user_via_in_filter() {
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce WHERE __user IN ('gopher')"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await;

    let cube_scan = query_plan.as_logical_plan().find_cube_scan();

    assert_eq!(cube_scan.options.change_user, Some("gopher".to_string()));

    assert_eq!(
        cube_scan.request,
        V1LoadRequestQuery {
            measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
            segments: Some(vec![]),
            dimensions: Some(vec![]),
            time_dimensions: None,
            order: Some(vec![]),
            limit: None,
            offset: None,
            filters: None,
            ungrouped: None,
        }
    )
}

#[tokio::test]
async fn test_change_user_via_in_filter_thoughtspot() {
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        r#"SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce "ta_1" WHERE (LOWER("ta_1"."__user") IN ('gopher')) = TRUE"#.to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let expected_request = V1LoadRequestQuery {
        measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
        segments: Some(vec![]),
        dimensions: Some(vec![]),
        time_dimensions: None,
        order: Some(vec![]),
        limit: None,
        offset: None,
        filters: None,
        ungrouped: None,
    };

    let cube_scan = query_plan.as_logical_plan().find_cube_scan();
    assert_eq!(cube_scan.options.change_user, Some("gopher".to_string()));
    assert_eq!(cube_scan.request, expected_request);

    let query_plan = convert_select_to_query_plan(
        r#"SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce "ta_1" WHERE ((LOWER("ta_1"."__user") IN ('gopher') = TRUE) = TRUE)"#.to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let cube_scan = query_plan.as_logical_plan().find_cube_scan();
    assert_eq!(cube_scan.options.change_user, Some("gopher".to_string()));
    assert_eq!(cube_scan.request, expected_request);
}

#[tokio::test]
async fn test_change_user_via_filter_and() {
    let query_plan = convert_select_to_query_plan(
        "SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce WHERE __user = 'gopher' AND customer_gender = 'male'".to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let cube_scan = query_plan.as_logical_plan().find_cube_scan();

    assert_eq!(cube_scan.options.change_user, Some("gopher".to_string()));

    assert_eq!(
        cube_scan.request,
        V1LoadRequestQuery {
            measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
            segments: Some(vec![]),
            dimensions: Some(vec![]),
            time_dimensions: None,
            order: Some(vec![]),
            limit: None,
            offset: None,
            filters: Some(vec![V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some("equals".to_string()),
                values: Some(vec!["male".to_string()]),
                or: None,
                and: None,
            }]),
            ungrouped: None,
        }
    )
}

#[tokio::test]
async fn test_change_user_via_filter_or() {
    // OR is not allowed for __user
    let meta = get_test_tenant_ctx();
    let query =
        convert_sql_to_cube_query(
            &"SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce WHERE __user = 'gopher' OR customer_gender = 'male'".to_string(),
            meta.clone(),
            get_test_session(DatabaseProtocol::PostgreSQL, meta).await
        ).await;

    // TODO: We need to propagate error to result, to assert message
    query.unwrap_err();
}

#[tokio::test]
async fn test_user_with_join() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let logical_plan = convert_select_to_query_plan(
        "SELECT aliased.count as c, aliased.user_1 as u1, aliased.user_2 as u2 FROM (SELECT \"KibanaSampleDataEcommerce\".count as count, \"KibanaSampleDataEcommerce\".__user as user_1, Logs.__user as user_2 FROM \"KibanaSampleDataEcommerce\" CROSS JOIN Logs WHERE __user = 'foo') aliased".to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await
        .as_logical_plan();

    let cube_scan = logical_plan.find_cube_scan();
    assert_eq!(
        cube_scan.request,
        V1LoadRequestQuery {
            measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
            dimensions: Some(vec![]),
            segments: Some(vec![]),
            time_dimensions: None,
            order: Some(vec![]),
            limit: None,
            offset: None,
            filters: None,
            ungrouped: Some(true),
        }
    );

    assert_eq!(cube_scan.options.change_user, Some("foo".to_string()))
}

/// This should test that query with CubeScanWrapper uses proper change_user for both SQL generation and execution calls
#[tokio::test]
async fn test_user_change_sql_generation() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let context = TestContext::new(DatabaseProtocol::PostgreSQL).await;

    context
        .execute_query(
            // language=PostgreSQL
            r#"
SELECT
    COALESCE(customer_gender, 'N/A'),
    AVG(avgPrice)
FROM
    KibanaSampleDataEcommerce
WHERE
    __user = 'gopher'
GROUP BY 1
;
        "#
            .to_string(),
        )
        .await
        .expect_err("Test transport does not support load with SQL");

    let load_calls = context.load_calls().await;
    assert_eq!(load_calls.len(), 1);
    let sql_query = load_calls[0].sql_query.as_ref().unwrap();
    // This should be placed from load meta to query by TestConnectionTransport::sql
    // It would mean that SQL generation used changed user
    assert!(sql_query.sql.contains(r#""changeUser":"gopher""#));
    assert_eq!(load_calls[0].meta.change_user(), Some("gopher".to_string()));
}
