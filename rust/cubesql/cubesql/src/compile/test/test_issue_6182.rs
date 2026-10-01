//! https://github.com/cube-js/cube/issues/6182
//! https://github.com/cube-js/cube/issues/9136
//! The SQL API types every `number` dimension as Float64, so 64-bit integer keys
//! (ClickHouse `UInt64`, BigQuery `INT64`) lose their last digits: the REST API returns
//! `8307928136669839578`, the SQL API returns `8307928136669839000`. Reproduced on a
//! running Cube v1.7.48 against ClickHouse 24.8 and Postgres 16.

use cubeclient::models::V1LoadRequestQuery;
use serde_json::json;

use crate::compile::{
    rewrite::rewriter::Rewriter,
    test::{init_testing_logger, TestContext},
    DatabaseProtocol,
};

async fn select_number_dimension(value: &str) -> String {
    let context = TestContext::new(DatabaseProtocol::PostgreSQL).await;

    context
        .add_cube_load_mock(
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            },
            crate::compile::tests::simple_load_response(
                vec!["KibanaSampleDataEcommerce.taxful_total_price"],
                vec![vec![json!(value)]],
            ),
        )
        .await;

    context
        .execute_query(
            "SELECT taxful_total_price FROM KibanaSampleDataEcommerce GROUP BY 1".to_string(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn test_issue_6182_uint64_number_dimension_keeps_all_digits() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let result = select_number_dimension("8307928136669839578").await;
    assert!(result.contains("8307928136669839578"), "{}", result);
}

#[tokio::test]
async fn test_issue_9136_negative_int64_number_dimension_keeps_all_digits() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let result = select_number_dimension("-9222709539351053981").await;
    assert!(result.contains("-9222709539351053981"), "{}", result);
}
