//! Tests that check database name propagation through LoadRequestMeta

use pretty_assertions::assert_eq;

use crate::compile::{
    test::{init_testing_logger, TestContext},
    DatabaseProtocol, Rewriter,
};
use crate::transport::LoadRequestMeta;

#[tokio::test]
async fn test_database_propagates_through_load_request_meta() {
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
    LOWER(customer_gender) = 'test'
GROUP BY 1
;
        "#
            .to_string(),
        )
        .await
        .expect_err("Test transport does not support load with SQL");

    let load_calls = context.load_calls().await;
    assert_eq!(load_calls.len(), 1);
    assert_eq!(load_calls[0].meta.database(), Some("cubedb".to_string()));
}

#[test]
fn test_load_request_meta_database_serialization() {
    let mut meta = LoadRequestMeta::new(
        "postgres".to_string(),
        "sql".to_string(),
        Some("test-app".to_string()),
    );

    let json = serde_json::to_value(&meta).unwrap();
    assert!(json.get("database").is_none());

    meta.set_database(Some("mydb".to_string()));
    let json = serde_json::to_value(&meta).unwrap();
    assert_eq!(json["database"], "mydb");
    assert_eq!(meta.database(), Some("mydb".to_string()));
}

#[test]
fn test_load_request_meta_no_database_by_default() {
    let meta = LoadRequestMeta::new(
        "postgres".to_string(),
        "sql".to_string(),
        None,
    );

    assert_eq!(meta.database(), None);

    let json = serde_json::to_value(&meta).unwrap();
    assert!(json.get("database").is_none());
}

/// Verifies that database name from session state propagates into the SQL query
/// meta passed to TestConnectionTransport::sql. Follows the same pattern as
/// test_user_change::test_user_change_sql_generation.
#[tokio::test]
async fn test_database_in_sql_query_meta() {
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
    LOWER(customer_gender) = 'test'
GROUP BY 1
;
        "#
            .to_string(),
        )
        .await
        .expect_err("Test transport does not support load with SQL");

    let load_calls = context.load_calls().await;
    assert_eq!(load_calls.len(), 1);

    // Database should appear in the serialized SQL query (set by TestConnectionTransport::sql)
    let sql_query = load_calls[0].sql_query.as_ref().unwrap();
    assert!(sql_query.sql.contains(r#""database": "cubedb""#));

    // And directly on the meta object
    assert_eq!(load_calls[0].meta.database(), Some("cubedb".to_string()));
}
