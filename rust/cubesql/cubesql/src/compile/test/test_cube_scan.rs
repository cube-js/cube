use cubeclient::models::{
    V1LoadRequestQuery, V1LoadRequestQueryFilterItem, V1LoadRequestQueryTimeDimension,
};
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

/// LIMIT 0 should be pushed to CubeScan as limit=0, and not replaced with a default limit
#[tokio::test]
async fn cubescan_limit_zero() {
    init_testing_logger();

    let variants = vec![
        // language=PostgreSQL
        r#"
        SELECT
            customer_gender
        FROM
            KibanaSampleDataEcommerce
        GROUP BY
            1
        LIMIT 0
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
            LIMIT 3
        ) scan
        LIMIT 0
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
            LIMIT 0
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
                limit: Some(0),
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

/// GROUP BY () is an empty grouping set, equivalent to aggregation without GROUP BY
#[tokio::test]
async fn cubescan_empty_group_by_tuple() {
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
        SELECT
            MEASURE(count),
            MEASURE(maxPrice)
        FROM
            KibanaSampleDataEcommerce
        WHERE customer_gender = 'female'
        GROUP BY ()
        LIMIT 100
        "#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await;

    let logical_plan = query_plan.as_logical_plan();
    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec![
                "KibanaSampleDataEcommerce.count".to_string(),
                "KibanaSampleDataEcommerce.maxPrice".to_string(),
            ]),
            dimensions: Some(vec![]),
            segments: Some(vec![]),
            order: Some(vec![]),
            limit: Some(100),
            filters: Some(vec![V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some("equals".to_string()),
                values: Some(vec!["female".to_string()]),
                or: None,
                and: None,
            }]),
            ..Default::default()
        }
    );
}

/// Projection over an ordered CubeScan with a limit or offset should keep its members
#[tokio::test]
async fn cubescan_projection_keeps_order_members() {
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
        SELECT notes
        FROM (
            SELECT notes
            FROM KibanaSampleDataEcommerce
            GROUP BY notes, customer_gender
            ORDER BY customer_gender
            LIMIT 5
        ) t
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
            dimensions: Some(vec![
                "KibanaSampleDataEcommerce.notes".to_string(),
                "KibanaSampleDataEcommerce.customer_gender".to_string(),
            ]),
            segments: Some(vec![]),
            order: Some(vec![vec![
                "KibanaSampleDataEcommerce.customer_gender".to_string(),
                "asc".to_string(),
            ]]),
            limit: Some(5),
            ..Default::default()
        }
    );

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
        SELECT notes
        FROM (
            SELECT notes, SUM(sumPrice)
            FROM KibanaSampleDataEcommerce
            GROUP BY notes
            ORDER BY SUM(sumPrice) DESC
            LIMIT 5
        ) t
        "#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await;

    let logical_plan = query_plan.as_logical_plan();
    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec!["KibanaSampleDataEcommerce.sumPrice".to_string()]),
            dimensions: Some(vec!["KibanaSampleDataEcommerce.notes".to_string()]),
            segments: Some(vec![]),
            order: Some(vec![vec![
                "KibanaSampleDataEcommerce.sumPrice".to_string(),
                "desc".to_string(),
            ]]),
            limit: Some(5),
            ..Default::default()
        }
    );

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
        SELECT notes
        FROM (
            SELECT notes
            FROM KibanaSampleDataEcommerce
            GROUP BY notes, customer_gender
            ORDER BY customer_gender
            OFFSET 5
        ) t
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
            dimensions: Some(vec![
                "KibanaSampleDataEcommerce.notes".to_string(),
                "KibanaSampleDataEcommerce.customer_gender".to_string(),
            ]),
            segments: Some(vec![]),
            order: Some(vec![vec![
                "KibanaSampleDataEcommerce.customer_gender".to_string(),
                "asc".to_string(),
            ]]),
            offset: Some(5),
            ..Default::default()
        }
    );

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
        SELECT m
        FROM (
            SELECT
                DATE_TRUNC('day', order_date) d,
                DATE_TRUNC('month', order_date) m
            FROM KibanaSampleDataEcommerce
            GROUP BY 1, 2
            ORDER BY 1
            LIMIT 5
        ) t
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
            dimensions: Some(vec![]),
            segments: Some(vec![]),
            time_dimensions: Some(vec![
                V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: None,
                },
                V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: None,
                },
            ]),
            order: Some(vec![vec![
                "KibanaSampleDataEcommerce.order_date".to_string(),
                "asc".to_string(),
            ]]),
            limit: Some(5),
            ..Default::default()
        }
    );
}
