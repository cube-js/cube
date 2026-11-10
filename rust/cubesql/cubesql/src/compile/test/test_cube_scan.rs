use cubeclient::models::{
    V1LoadRequestQuery, V1LoadRequestQueryFilterItem, V1LoadRequestQueryTimeDimension,
};
use pretty_assertions::assert_eq;

use crate::compile::{
    test::{convert_select_to_query_plan, init_testing_logger, utils::LogicalPlanTestUtils},
    DatabaseProtocol, Rewriter,
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

    // A grouped scan can drop a measure it doesn't order by
    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
        SELECT notes
        FROM (
            SELECT notes, SUM(sumPrice)
            FROM KibanaSampleDataEcommerce
            GROUP BY notes
            ORDER BY notes
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
            dimensions: Some(vec!["KibanaSampleDataEcommerce.notes".to_string()]),
            segments: Some(vec![]),
            order: Some(vec![vec![
                "KibanaSampleDataEcommerce.notes".to_string(),
                "asc".to_string(),
            ]]),
            limit: Some(5),
            ..Default::default()
        }
    );

    // An ungrouped scan only needs to keep the members it orders by
    for (sql, dimensions, order_member) in [
        (
            "SELECT customer_gender FROM (SELECT customer_gender, notes FROM KibanaSampleDataEcommerce ORDER BY customer_gender LIMIT 5) t",
            vec!["KibanaSampleDataEcommerce.customer_gender"],
            "KibanaSampleDataEcommerce.customer_gender",
        ),
        (
            "SELECT customer_gender FROM (SELECT customer_gender, notes FROM KibanaSampleDataEcommerce ORDER BY notes LIMIT 5) t",
            vec![
                "KibanaSampleDataEcommerce.customer_gender",
                "KibanaSampleDataEcommerce.notes",
            ],
            "KibanaSampleDataEcommerce.notes",
        ),
    ] {
        let query_plan =
            convert_select_to_query_plan(sql.to_string(), DatabaseProtocol::PostgreSQL).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(dimensions.into_iter().map(|d| d.to_string()).collect()),
                segments: Some(vec![]),
                order: Some(vec![vec![order_member.to_string(), "asc".to_string()]]),
                limit: Some(5),
                ungrouped: Some(true),
                ..Default::default()
            }
        );
    }
}

/// Projection over a grouped CubeScan should keep its grouping dimensions
#[tokio::test]
async fn cubescan_projection_keeps_grouping_dimensions() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let expected_dimensions = Some(vec![
        "KibanaSampleDataEcommerce.notes".to_string(),
        "KibanaSampleDataEcommerce.customer_gender".to_string(),
    ]);

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
        SELECT notes, s
        FROM (
            SELECT notes, customer_gender, SUM(sumPrice) s
            FROM KibanaSampleDataEcommerce
            GROUP BY 1, 2
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
            dimensions: expected_dimensions.clone(),
            segments: Some(vec![]),
            order: Some(vec![]),
            ..Default::default()
        }
    );

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
        SELECT customer_gender
        FROM (
            SELECT customer_gender
            FROM KibanaSampleDataEcommerce
            GROUP BY customer_gender, notes
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
                "KibanaSampleDataEcommerce.customer_gender".to_string(),
                "KibanaSampleDataEcommerce.notes".to_string(),
            ]),
            segments: Some(vec![]),
            order: Some(vec![]),
            limit: Some(5),
            ..Default::default()
        }
    );

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
        SELECT notes
        FROM KibanaSampleDataEcommerce
        GROUP BY notes, customer_gender
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
            dimensions: expected_dimensions,
            segments: Some(vec![]),
            order: Some(vec![]),
            ..Default::default()
        }
    );
}

/// Projection that drops order members of an unlimited scan or all members of a joined table
/// should still be pushed down, so the aggregate and join rules above it apply
#[tokio::test]
async fn cubescan_projection_pushed_under_aggregate_and_join() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
        SELECT customer_gender, MEASURE(count)
        FROM (
            SELECT customer_gender, count
            FROM KibanaSampleDataEcommerce
            ORDER BY notes
        ) t
        GROUP BY 1
        "#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await;

    let logical_plan = query_plan.as_logical_plan();
    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
            dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
            segments: Some(vec![]),
            order: Some(vec![vec![
                "KibanaSampleDataEcommerce.notes".to_string(),
                "asc".to_string(),
            ]]),
            ..Default::default()
        }
    );

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
        SELECT a.customer_gender, l.content
        FROM (
            SELECT customer_gender, __cubeJoinField
            FROM KibanaSampleDataEcommerce
            GROUP BY 1, 2
        ) a
        CROSS JOIN Logs l
        WHERE a.__cubeJoinField = l.__cubeJoinField
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
                "KibanaSampleDataEcommerce.customer_gender".to_string(),
                "Logs.content".to_string(),
            ]),
            segments: Some(vec![]),
            order: Some(vec![]),
            join_hints: Some(vec![vec![
                "KibanaSampleDataEcommerce".to_string(),
                "Logs".to_string(),
            ]]),
            ..Default::default()
        }
    );

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
        SELECT "source"."customer_gender", "Q2"."c"
        FROM (
            SELECT customer_gender
            FROM KibanaSampleDataEcommerce
            ORDER BY order_date
        ) "source"
        LEFT JOIN (
            SELECT customer_gender, COUNT(*) c
            FROM KibanaSampleDataEcommerce
            GROUP BY 1
        ) "Q2" ON "source"."customer_gender" = "Q2"."customer_gender"
        LIMIT 2000
        "#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await;

    let request = query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .request;
    assert_eq!(request.subquery_joins.unwrap().len(), 1);
}
