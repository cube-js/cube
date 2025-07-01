use cubeclient::models::V1LoadRequestQuery;
use pretty_assertions::assert_eq;

use crate::compile::{
    test::{convert_select_to_query_plan, init_testing_logger, utils::LogicalPlanTestUtils},
    DatabaseProtocol,
};

/// LIMIT n OFFSET m should be pushed to CubeScan
#[tokio::test]
async fn cubescan_limit_offset() {
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
        SELECT
            customer_gender
        FROM
            KibanaSampleDataEcommerce
        GROUP BY
            1
        LIMIT 2
        OFFSET 3
        "#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await;

    let logical_plan = query_plan.as_logical_plan();
    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec![]),
            dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
            segments: Some(vec![]),
            order: Some(vec![]),
            limit: Some(2),
            offset: Some(3),
            ..Default::default()
        }
    );
}

/// LIMIT over LIMIT should be pushed to single CubeScan
#[tokio::test]
async fn cubescan_limit_limit() {
    init_testing_logger();

    let variants = vec![
        // language=PostgreSQL
        r#"
        SELECT
            customer_gender
        FROM (
            SELECT
                customer_gender
            FROM
                KibanaSampleDataEcommerce
            GROUP BY
                1
            LIMIT 3
        ) scan
        LIMIT 2
        "#,
        // language=PostgreSQL
        r#"
        SELECT
            customer_gender
        FROM (
            SELECT
                customer_gender
            FROM
                KibanaSampleDataEcommerce
            GROUP BY
                1
            LIMIT 2
        ) scan
        LIMIT 3
        "#,
    ];

    for variant in variants {
        let query_plan =
            convert_select_to_query_plan(variant.to_string(), DatabaseProtocol::PostgreSQL).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                limit: Some(2),
                ..Default::default()
            }
        );
    }
}

/// OFFSET over OFFSET should be pushed to single CubeScan
#[tokio::test]
async fn cubescan_offset_offset() {
    init_testing_logger();

    let variants = vec![
        // language=PostgreSQL
        r#"
        SELECT
            customer_gender
        FROM (
            SELECT
                customer_gender
            FROM
                KibanaSampleDataEcommerce
            GROUP BY
                1
            OFFSET 3
        ) scan
        OFFSET 2
        "#,
        // language=PostgreSQL
        r#"
        SELECT
            customer_gender
        FROM (
            SELECT
                customer_gender
            FROM
                KibanaSampleDataEcommerce
            GROUP BY
                1
            OFFSET 2
        ) scan
        OFFSET 3
        "#,
    ];

    for variant in variants {
        let query_plan =
            convert_select_to_query_plan(variant.to_string(), DatabaseProtocol::PostgreSQL).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                offset: Some(5),
                ..Default::default()
            }
        );
    }
}

/// LIMIT OFFSET over LIMIT OFFSET should be pushed to single CubeScan with a proper values
#[tokio::test]
async fn cubescan_limit_offset_limit_offset() {
    init_testing_logger();

    let variants = vec![
        (
            // language=PostgreSQL
            r#"
        SELECT
            customer_gender
        FROM (
            SELECT
                customer_gender
            FROM
                KibanaSampleDataEcommerce
            GROUP BY
                1
            LIMIT 3
            OFFSET 3
        ) scan
        LIMIT 2
        OFFSET 2
        "#,
            1,
        ),
        (
            // language=PostgreSQL
            r#"
        SELECT
            customer_gender
        FROM (
            SELECT
                customer_gender
            FROM
                KibanaSampleDataEcommerce
            GROUP BY
                1
            LIMIT 10
            OFFSET 3
        ) scan
        LIMIT 2
        OFFSET 2
        "#,
            2,
        ),
    ];

    for (variant, limit) in variants {
        let query_plan =
            convert_select_to_query_plan(variant.to_string(), DatabaseProtocol::PostgreSQL).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                limit: Some(limit),
                offset: Some(5),
                ..Default::default()
            }
        );
    }
}
