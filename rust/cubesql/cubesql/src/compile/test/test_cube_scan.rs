use cubeclient::models::{
    V1LoadRequestQuery, V1LoadRequestQueryFilterItem, V1LoadRequestQueryTimeDimension,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use pretty_assertions::assert_eq;
use serde_json::json;
use std::sync::Arc;

use crate::config::ConfigObjImpl;
use crate::{
    compile::{
        test::{
            convert_select_to_query_plan, convert_sql_to_cube_query, get_test_session,
            get_test_tenant_ctx_with_meta, init_testing_logger, utils::LogicalPlanTestUtils,
            TestContext,
        },
        DatabaseProtocol, Rewriter,
    },
    transport::{CubeMeta, CubeMetaDimension, CubeMetaMeasure},
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

/// Cube ignores orders on members a request does not select, so the scan selects them when it loads
#[tokio::test]
async fn cubescan_loads_order_members() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let context = TestContext::new(DatabaseProtocol::PostgreSQL).await;
    context
        .add_cube_load_mock(
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
                limit: Some(50000),
                ungrouped: Some(true),
                ..Default::default()
            },
            crate::compile::tests::simple_load_response(
                vec![
                    "KibanaSampleDataEcommerce.notes",
                    "KibanaSampleDataEcommerce.customer_gender",
                ],
                vec![
                    vec![json!("b"), json!("a")],
                    vec![json!("female"), json!("male")],
                ],
            ),
        )
        .await;
    context
        .add_cube_load_mock(
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.sumPrice".to_string()]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.sumPrice".to_string(),
                    "asc".to_string(),
                ]]),
                limit: Some(50000),
                ..Default::default()
            },
            crate::compile::tests::simple_load_response(
                vec![
                    "KibanaSampleDataEcommerce.customer_gender",
                    "KibanaSampleDataEcommerce.sumPrice",
                ],
                vec![
                    vec![json!("male"), json!("female")],
                    vec![json!(1), json!(2)],
                ],
            ),
        )
        .await;

    insta::assert_snapshot!(context
        .execute_query("SELECT notes FROM KibanaSampleDataEcommerce ORDER BY customer_gender")
        .await
        .unwrap());
    insta::assert_snapshot!(
        context
            .execute_query(
                "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1 ORDER BY SUM(sumPrice)"
            )
            .await
            .unwrap()
    );

    // An order member from another data source is not loaded, as Cube would reject the load
    let context = TestContext::with_config_and_meta(
        DatabaseProtocol::PostgreSQL,
        Arc::new(ConfigObjImpl::default()),
        crate::compile::test::get_test_tenant_ctx_with_multi_data_source_view(),
    )
    .await;
    context
        .add_cube_load_mock(
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["MultiSourceView.customer_gender".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![vec![
                    "MultiSourceView.content".to_string(),
                    "desc".to_string(),
                ]]),
                limit: Some(50000),
                ungrouped: Some(true),
                ..Default::default()
            },
            crate::compile::tests::simple_load_response(
                vec!["MultiSourceView.customer_gender"],
                vec![vec![json!("male")]],
            ),
        )
        .await;
    insta::assert_snapshot!(context
        .execute_query("SELECT customer_gender FROM MultiSourceView ORDER BY content DESC")
        .await
        .unwrap());
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

/// BI tools read the columns of a table with `SELECT *`, which expands to one column per
/// member: on a wide view the rewrite must not grow with the member count, or it hits the
/// node limit
#[tokio::test]
async fn select_star_from_wide_view() {
    init_testing_logger();

    let members = 1000;
    let meta = get_test_tenant_ctx_with_meta(vec![CubeMeta {
        name: "wide".to_string(),
        dimensions: (0..members)
            .map(|i| CubeMetaDimension {
                name: format!("wide.dim{i}"),
                r#type: "string".to_string(),
                ..CubeMetaDimension::default()
            })
            .collect(),
        measures: (0..members)
            .map(|i| CubeMetaMeasure {
                name: format!("wide.measure{i}"),
                r#type: "number".to_string(),
                agg_type: Some("sum".to_string()),
                ..CubeMetaMeasure::default()
            })
            .collect(),
        ..CubeMeta::default()
    }]);

    for (sql, limit) in [
        ("SELECT * FROM wide WHERE 1 = 0", Some(0)),
        ("SELECT * FROM wide LIMIT 0", Some(0)),
        ("SELECT * FROM wide", None),
    ] {
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
        let query_plan = convert_sql_to_cube_query(&sql.to_string(), meta.clone(), session)
            .await
            .unwrap_or_else(|e| panic!("{}: {}", sql, e));
        let request = query_plan.as_logical_plan().find_cube_scan().request;
        assert_eq!(request.measures.unwrap().len(), members, "{sql}");
        assert_eq!(request.dimensions.unwrap().len(), members, "{sql}");
        assert_eq!(request.limit, limit, "{sql}");
    }
}

/// A `SELECT *` filtered in SQL is still one wrapped query, with no post-processing above it
#[tokio::test]
async fn select_star_with_sql_filter_is_wrapped() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT * FROM KibanaSampleDataEcommerce WHERE LOWER(customer_gender) = 'female' LIMIT 10"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await;

    let request = query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .request;
    assert_eq!(request.limit, Some(10));
}

/// A `time` measure is typed `Timestamp`, like a `time` dimension
#[tokio::test]
async fn time_measure_is_timestamp() {
    init_testing_logger();

    let meta = get_test_tenant_ctx_with_meta(vec![CubeMeta {
        name: "Orders".to_string(),
        dimensions: vec![
            CubeMetaDimension {
                name: "Orders.status".to_string(),
                r#type: "string".to_string(),
                ..CubeMetaDimension::default()
            },
            CubeMetaDimension {
                name: "Orders.created_at".to_string(),
                r#type: "time".to_string(),
                ..CubeMetaDimension::default()
            },
        ],
        measures: vec![CubeMetaMeasure {
            name: "Orders.last_shipped_at".to_string(),
            r#type: "time".to_string(),
            agg_type: Some("time".to_string()),
            ..CubeMetaMeasure::default()
        }],
        ..CubeMeta::default()
    }]);

    let timestamp = DataType::Timestamp(TimeUnit::Nanosecond, None);
    for sql in [
        "SELECT status, created_at, MEASURE(last_shipped_at) FROM Orders GROUP BY 1, 2",
        "SELECT status, created_at, last_shipped_at FROM Orders",
    ] {
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
        let query_plan = convert_sql_to_cube_query(&sql.to_string(), meta.clone(), session)
            .await
            .unwrap_or_else(|e| panic!("{}: {}", sql, e));
        let logical_plan = query_plan.as_logical_plan();
        let schema = logical_plan.schema();
        assert_eq!(schema.field(1).data_type(), &timestamp, "{sql}");
        assert_eq!(schema.field(2).data_type(), &timestamp, "{sql}");
        assert_eq!(
            logical_plan.find_cube_scan().request.measures,
            Some(vec!["Orders.last_shipped_at".to_string()]),
            "{sql}"
        );
    }

    let sql = "SELECT status, MEASURE(last_shipped_at) FROM Orders GROUP BY 1 HAVING MEASURE(last_shipped_at) > '2026-01-01'";
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let query_plan = convert_sql_to_cube_query(&sql.to_string(), meta.clone(), session)
        .await
        .unwrap();
    assert_eq!(
        query_plan
            .as_logical_plan()
            .find_cube_scan()
            .request
            .filters,
        Some(vec![V1LoadRequestQueryFilterItem {
            member: Some("Orders.last_shipped_at".to_string()),
            operator: Some("gt".to_string()),
            values: Some(vec!["2026-01-01T00:00:00.000Z".to_string()]),
            or: None,
            and: None,
        }])
    );

    // `<>` is compared as a timestamp in the SQL pushed down to Cube
    if Rewriter::sql_push_down_enabled() {
        let sql = "SELECT status, MEASURE(last_shipped_at) FROM Orders GROUP BY 1 HAVING MEASURE(last_shipped_at) <> '2026-01-01'";
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
        let query_plan = convert_sql_to_cube_query(&sql.to_string(), meta.clone(), session)
            .await
            .unwrap();
        let wrapped_sql = query_plan
            .as_logical_plan()
            .find_cube_scan_wrapped_sql()
            .wrapped_sql
            .sql;
        assert!(
            wrapped_sql.contains("!= timestamptz '2026-01-01T00:00:00.000Z'"),
            "{wrapped_sql}"
        );
    }

    // Values load as timestamps
    let context = TestContext::with_config_and_meta(
        DatabaseProtocol::PostgreSQL,
        Arc::new(ConfigObjImpl::default()),
        meta,
    )
    .await;
    context
        .add_cube_load_mock(
            V1LoadRequestQuery {
                measures: Some(vec!["Orders.last_shipped_at".to_string()]),
                dimensions: Some(vec!["Orders.status".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            },
            crate::compile::tests::simple_load_response(
                vec!["Orders.status", "Orders.last_shipped_at"],
                vec![
                    vec![json!("shipped"), json!("returned"), json!("pending")],
                    vec![
                        json!("2026-01-07T00:00:00.000"),
                        json!("2026-01-01T00:00:00.000"),
                        json!(null),
                    ],
                ],
            ),
        )
        .await;
    assert_eq!(
        context
            .execute_query("SELECT status, MEASURE(last_shipped_at) AS l FROM Orders GROUP BY 1")
            .await
            .unwrap(),
        "+----------+-------------------------+\n\
         | status   | l                       |\n\
         +----------+-------------------------+\n\
         | shipped  | 2026-01-07T00:00:00.000 |\n\
         | returned | 2026-01-01T00:00:00.000 |\n\
         | pending  | NULL                    |\n\
         +----------+-------------------------+"
    );
}
