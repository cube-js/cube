use std::sync::Arc;

use cubeclient::models::{
    V1CubeMetaType, V1LoadRequestQuery, V1LoadRequestQueryFilterItem,
    V1LoadRequestQueryTimeDimension,
};
use pretty_assertions::assert_eq;

use crate::{
    compile::{
        rewrite::rewriter::Rewriter,
        test::{
            convert_sql_to_cube_query, get_test_session_with_config, get_test_tenant_ctx_with_meta,
            init_testing_logger, utils::LogicalPlanTestUtils,
        },
        CompilationError, DatabaseProtocol, QueryPlan,
    },
    config::{ConfigObj, ConfigObjImpl},
    transport::{CubeMeta, CubeMetaDimension, CubeMetaMeasure},
};

/// Two views that both expose the same underlying `customers.customer_city`
/// dimension (via `aliasMember`). `orders_view` carries an `orders` measure
/// while `customers_view` carries a `customers` measure, so a query that
/// touches both is a multi-fact query joined on the shared key.
fn views_meta() -> Vec<CubeMeta> {
    let dimension = |name: &str, alias: &str| CubeMetaDimension {
        name: name.to_string(),
        r#type: "string".to_string(),
        alias_member: Some(alias.to_string()),
        ..CubeMetaDimension::default()
    };
    let time_dimension = |name: &str, alias: &str| CubeMetaDimension {
        name: name.to_string(),
        r#type: "time".to_string(),
        alias_member: Some(alias.to_string()),
        ..CubeMetaDimension::default()
    };
    let measure = |name: &str, alias: &str, agg: &str| CubeMetaMeasure {
        name: name.to_string(),
        title: None,
        short_title: None,
        description: None,
        r#type: "number".to_string(),
        agg_type: Some(agg.to_string()),
        meta: None,
        alias_member: Some(alias.to_string()),
        format: None,
        format_description: None,
        currency: None,
    };

    vec![
        CubeMeta {
            name: "customers_view".to_string(),
            description: None,
            title: None,
            r#type: V1CubeMetaType::View,
            dimensions: vec![
                dimension("customers_view.customer_city", "customers.customer_city"),
                // A second dimension that is NOT a join key, used to test that a
                // query grouping by it (instead of the join key) is not merged.
                dimension("customers_view.status", "customers.status"),
                time_dimension("customers_view.created_at", "customers.created_at"),
            ],
            measures: vec![measure(
                "customers_view.avg_age",
                "customers.avg_age",
                "avg",
            )],
            segments: vec![],
            joins: None,
            folders: None,
            nested_folders: None,
            hierarchies: None,
            meta: None,
        },
        CubeMeta {
            name: "orders_view".to_string(),
            description: None,
            title: None,
            r#type: V1CubeMetaType::View,
            dimensions: vec![
                dimension("orders_view.customer_city", "customers.customer_city"),
                dimension("orders_view.status", "orders.status"),
                time_dimension("orders_view.created_at", "customers.created_at"),
            ],
            measures: vec![measure("orders_view.revenue", "orders.revenue", "sum")],
            segments: vec![],
            joins: None,
            folders: None,
            nested_folders: None,
            hierarchies: None,
            meta: None,
        },
        CubeMeta {
            name: "returns_view".to_string(),
            description: None,
            title: None,
            r#type: V1CubeMetaType::View,
            dimensions: vec![dimension(
                "returns_view.customer_city",
                "customers.customer_city",
            )],
            measures: vec![measure("returns_view.refunds", "returns.refunds", "sum")],
            segments: vec![],
            joins: None,
            folders: None,
            nested_folders: None,
            hierarchies: None,
            meta: None,
        },
        CubeMeta {
            name: "payments_view".to_string(),
            description: None,
            title: None,
            r#type: V1CubeMetaType::View,
            dimensions: vec![dimension(
                "payments_view.customer_city",
                "customers.customer_city",
            )],
            measures: vec![measure("payments_view.paid", "payments.paid", "sum")],
            segments: vec![],
            joins: None,
            folders: None,
            nested_folders: None,
            hierarchies: None,
            meta: None,
        },
    ]
}

fn set_filter(member: &str) -> V1LoadRequestQueryFilterItem {
    V1LoadRequestQueryFilterItem {
        member: Some(member.to_string()),
        operator: Some("set".to_string()),
        values: None,
        or: None,
        and: None,
    }
}

/// Plans `sql` against the two views, with the Tesseract SQL planner enabled or
/// disabled. The shared-member view-join merge only fires when Tesseract is
/// enabled.
async fn plan_view_join(sql: &str, tesseract: bool) -> Result<QueryPlan, CompilationError> {
    let meta = get_test_tenant_ctx_with_meta(views_meta());
    let mut config = ConfigObjImpl::default();
    config.tesseract_sql_planner = tesseract;
    let config: Arc<dyn ConfigObj> = Arc::new(config);
    let session =
        get_test_session_with_config(DatabaseProtocol::PostgreSQL, config, meta.clone()).await;
    convert_sql_to_cube_query(&sql.to_string(), meta, session).await
}

const GROUPED_LEFT_JOIN: &str = r#"
    SELECT c.customer_city, measure(o.revenue), measure(c.avg_age)
    FROM customers_view c
    LEFT JOIN orders_view o ON o.customer_city = c.customer_city
    GROUP BY 1
"#;

/// The motivating query: a grouped (multi-fact) `LEFT JOIN` selecting a
/// dimension and measures from each view, joined on the shared `customer_city`
/// which is also the GROUP BY key. The two view scans are merged into a single
/// grouped CubeScan, and the left join key gets a `set` filter to recover
/// LEFT-join semantics on top of the FULL OUTER multi-fact stitch.
#[tokio::test]
async fn test_group_by_left_join_two_views_on_shared_member() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let logical_plan = plan_view_join(GROUPED_LEFT_JOIN, true)
        .await
        .unwrap()
        .as_logical_plan();

    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec![
                "orders_view.revenue".to_string(),
                "customers_view.avg_age".to_string(),
            ]),
            dimensions: Some(vec!["customers_view.customer_city".to_string()]),
            segments: Some(vec![]),
            order: Some(vec![]),
            filters: Some(vec![set_filter("customers_view.customer_city")]),
            join_hints: Some(vec![vec![
                "customers_view".to_string(),
                "orders_view".to_string(),
            ]]),
            ..Default::default()
        }
    )
}

/// Same shape but `INNER JOIN`: both sides must be present, so the merged scan
/// carries a `set` filter on the join key of each side.
#[tokio::test]
async fn test_group_by_inner_join_two_views_on_shared_member() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let logical_plan = plan_view_join(
        r#"
            SELECT c.customer_city, measure(o.revenue), measure(c.avg_age)
            FROM customers_view c
            INNER JOIN orders_view o ON o.customer_city = c.customer_city
            GROUP BY 1
            "#,
        true,
    )
    .await
    .unwrap()
    .as_logical_plan();

    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec![
                "orders_view.revenue".to_string(),
                "customers_view.avg_age".to_string(),
            ]),
            dimensions: Some(vec!["customers_view.customer_city".to_string()]),
            segments: Some(vec![]),
            order: Some(vec![]),
            filters: Some(vec![
                set_filter("orders_view.customer_city"),
                set_filter("customers_view.customer_city"),
            ]),
            join_hints: Some(vec![vec![
                "customers_view".to_string(),
                "orders_view".to_string(),
            ]]),
            ..Default::default()
        }
    )
}

/// The merge relies on the Tesseract SQL planner; with it disabled the join is
/// not merged and the query is rejected like any other unsupported cube join.
#[tokio::test]
async fn test_grouped_view_join_not_merged_without_tesseract() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let error = plan_view_join(GROUPED_LEFT_JOIN, false).await.unwrap_err();
    assert!(matches!(error, CompilationError::Rewrite(..)));
}

/// Ungrouped query (`SELECT *`): the shared-member merge only applies to
/// grouped queries, so an ungrouped join is not merged and is rejected even
/// when Tesseract is enabled.
#[tokio::test]
async fn test_ungrouped_join_two_views_on_shared_member_is_not_merged() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let error = plan_view_join(
        r#"
            SELECT *
            FROM customers_view
            LEFT JOIN orders_view
                ON (orders_view.customer_city = customers_view.customer_city)
            "#,
        true,
    )
    .await
    .unwrap_err();
    assert!(matches!(error, CompilationError::Rewrite(..)));
}

/// The join is over a dimension (`customer_city`) that is not in the GROUP BY
/// (the query groups by `status` instead). The merge requires the join key to
/// be the group-by key, so this is not merged and is rejected.
#[tokio::test]
async fn test_group_by_join_dimension_not_in_group_by_is_not_merged() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let error = plan_view_join(
        r#"
            SELECT c.status, measure(o.revenue), measure(c.avg_age)
            FROM customers_view c
            LEFT JOIN orders_view o ON o.customer_city = c.customer_city
            GROUP BY 1
            "#,
        true,
    )
    .await
    .unwrap_err();
    assert!(matches!(error, CompilationError::Rewrite(..)));
}

/// The merge only fires when the join key is fully within dimensions. Joining
/// the two views on a measure (`o.revenue = c.avg_age`) is not a shared-member
/// dimension join, so the scans are not merged and the query is rejected.
#[tokio::test]
async fn test_join_two_views_on_measure_is_not_merged() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let error = plan_view_join(
        r#"
            SELECT c.customer_city, measure(o.revenue)
            FROM customers_view c
            LEFT JOIN orders_view o ON (o.revenue = c.avg_age)
            GROUP BY 1
            "#,
        true,
    )
    .await
    .unwrap_err();
    assert!(matches!(error, CompilationError::Rewrite(..)));
}

/// `RIGHT JOIN`: the right side must be present, so the merged scan carries a
/// `set` filter on the right join key.
#[tokio::test]
async fn test_group_by_right_join_two_views_on_shared_member() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let logical_plan = plan_view_join(
        r#"
            SELECT c.customer_city, measure(o.revenue), measure(c.avg_age)
            FROM customers_view c
            RIGHT JOIN orders_view o ON o.customer_city = c.customer_city
            GROUP BY 1
            "#,
        true,
    )
    .await
    .unwrap()
    .as_logical_plan();

    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec![
                "orders_view.revenue".to_string(),
                "customers_view.avg_age".to_string(),
            ]),
            dimensions: Some(vec!["customers_view.customer_city".to_string()]),
            segments: Some(vec![]),
            order: Some(vec![]),
            filters: Some(vec![set_filter("orders_view.customer_city")]),
            join_hints: Some(vec![vec![
                "customers_view".to_string(),
                "orders_view".to_string(),
            ]]),
            ..Default::default()
        }
    )
}

/// `FULL JOIN`: every key from either side is kept (default multi-fact
/// behavior), so no presence `set` filter is added.
#[tokio::test]
async fn test_group_by_full_join_two_views_on_shared_member() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let logical_plan = plan_view_join(
        r#"
            SELECT c.customer_city, measure(o.revenue), measure(c.avg_age)
            FROM customers_view c
            FULL JOIN orders_view o ON o.customer_city = c.customer_city
            GROUP BY 1
            "#,
        true,
    )
    .await
    .unwrap()
    .as_logical_plan();

    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec![
                "orders_view.revenue".to_string(),
                "customers_view.avg_age".to_string(),
            ]),
            dimensions: Some(vec!["customers_view.customer_city".to_string()]),
            segments: Some(vec![]),
            order: Some(vec![]),
            // FULL JOIN adds no presence filter.
            filters: None,
            join_hints: Some(vec![vec![
                "customers_view".to_string(),
                "orders_view".to_string(),
            ]]),
            ..Default::default()
        }
    )
}

/// Joining three views on the shared key (FULL JOIN, so no presence filters)
/// merges into a single multi-fact CubeScan with all three measures.
#[tokio::test]
async fn test_group_by_full_join_three_views_on_shared_member() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let logical_plan = plan_view_join(
        r#"
            SELECT c.customer_city, measure(o.revenue), measure(r.refunds)
            FROM customers_view c
            FULL JOIN orders_view o ON o.customer_city = c.customer_city
            FULL JOIN returns_view r ON r.customer_city = c.customer_city
            GROUP BY 1
            "#,
        true,
    )
    .await
    .unwrap()
    .as_logical_plan();

    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec![
                "orders_view.revenue".to_string(),
                "returns_view.refunds".to_string(),
            ]),
            dimensions: Some(vec!["customers_view.customer_city".to_string()]),
            segments: Some(vec![]),
            order: Some(vec![]),
            join_hints: Some(vec![
                vec!["customers_view".to_string(), "orders_view".to_string()],
                vec!["customers_view".to_string(), "returns_view".to_string()],
            ]),
            ..Default::default()
        }
    )
}

/// Joining four views on the shared key (FULL JOIN) merges into a single
/// multi-fact CubeScan with all four measures.
#[tokio::test]
async fn test_group_by_full_join_four_views_on_shared_member() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let logical_plan = plan_view_join(
        r#"
            SELECT c.customer_city, measure(o.revenue), measure(r.refunds), measure(p.paid)
            FROM customers_view c
            FULL JOIN orders_view o ON o.customer_city = c.customer_city
            FULL JOIN returns_view r ON r.customer_city = c.customer_city
            FULL JOIN payments_view p ON p.customer_city = c.customer_city
            GROUP BY 1
            "#,
        true,
    )
    .await
    .unwrap()
    .as_logical_plan();

    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec![
                "orders_view.revenue".to_string(),
                "returns_view.refunds".to_string(),
                "payments_view.paid".to_string(),
            ]),
            dimensions: Some(vec!["customers_view.customer_city".to_string()]),
            segments: Some(vec![]),
            order: Some(vec![]),
            join_hints: Some(vec![
                vec!["customers_view".to_string(), "orders_view".to_string()],
                vec!["customers_view".to_string(), "returns_view".to_string()],
                vec!["customers_view".to_string(), "payments_view".to_string()],
            ]),
            ..Default::default()
        }
    )
}

/// A WHERE filter on top of the join is pushed through the wrapper into the
/// merged scan and shows up as a Cube query filter alongside the join-semantics
/// `set` filter.
#[tokio::test]
async fn test_group_by_left_join_with_where_filter() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let logical_plan = plan_view_join(
        r#"
            SELECT c.customer_city, measure(o.revenue)
            FROM customers_view c
            LEFT JOIN orders_view o ON o.customer_city = c.customer_city
            WHERE c.status = 'active'
            GROUP BY 1
            "#,
        true,
    )
    .await
    .unwrap()
    .as_logical_plan();

    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec!["orders_view.revenue".to_string()]),
            dimensions: Some(vec!["customers_view.customer_city".to_string()]),
            segments: Some(vec![]),
            order: Some(vec![]),
            filters: Some(vec![
                set_filter("customers_view.customer_city"),
                V1LoadRequestQueryFilterItem {
                    member: Some("customers_view.status".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["active".to_string()]),
                    or: None,
                    and: None,
                },
            ]),
            join_hints: Some(vec![vec![
                "customers_view".to_string(),
                "orders_view".to_string(),
            ]]),
            ..Default::default()
        }
    )
}

/// A filter placed in the ON clause (in addition to the shared-key equality).
#[tokio::test]
async fn test_group_by_left_join_with_on_filter() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let logical_plan = plan_view_join(
        r#"
            SELECT c.customer_city, measure(o.revenue)
            FROM customers_view c
            LEFT JOIN orders_view o
                ON o.customer_city = c.customer_city AND o.status = 'completed'
            GROUP BY 1
            "#,
        true,
    )
    .await
    .unwrap()
    .as_logical_plan();

    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec!["orders_view.revenue".to_string()]),
            dimensions: Some(vec!["customers_view.customer_city".to_string()]),
            segments: Some(vec![]),
            order: Some(vec![]),
            filters: Some(vec![
                set_filter("customers_view.customer_city"),
                V1LoadRequestQueryFilterItem {
                    member: Some("orders_view.status".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["completed".to_string()]),
                    or: None,
                    and: None,
                },
            ]),
            join_hints: Some(vec![vec![
                "customers_view".to_string(),
                "orders_view".to_string(),
            ]]),
            ..Default::default()
        }
    )
}

/// A 3-way LEFT join pins the per-pass presence-filter accumulation through
/// `shared-member-join-extend-wrapper`: each LEFT join contributes a `set`
/// filter on its own left-side join key.
#[tokio::test]
async fn test_group_by_left_join_three_views_presence_filters() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let logical_plan = plan_view_join(
        r#"
            SELECT c.customer_city, measure(o.revenue), measure(r.refunds)
            FROM customers_view c
            LEFT JOIN orders_view o ON o.customer_city = c.customer_city
            LEFT JOIN returns_view r ON r.customer_city = o.customer_city
            GROUP BY 1
            "#,
        true,
    )
    .await
    .unwrap()
    .as_logical_plan();

    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec![
                "orders_view.revenue".to_string(),
                "returns_view.refunds".to_string(),
            ]),
            dimensions: Some(vec!["customers_view.customer_city".to_string()]),
            segments: Some(vec![]),
            order: Some(vec![]),
            filters: Some(vec![
                set_filter("orders_view.customer_city"),
                set_filter("customers_view.customer_city"),
            ]),
            join_hints: Some(vec![
                vec!["customers_view".to_string(), "orders_view".to_string()],
                vec!["orders_view".to_string(), "returns_view".to_string()],
            ]),
            ..Default::default()
        }
    )
}

/// Joining two views on a raw shared time column and grouping by
/// `DATE_TRUNC('day', ...)` merges into a single multi-fact CubeScan with the
/// grouped column emitted as a `timeDimensions` entry (granularity `day`).
#[tokio::test]
async fn test_left_join_raw_time_group_by_date_trunc() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let logical_plan = plan_view_join(
        r#"
            SELECT DATE_TRUNC('day', c.created_at), measure(o.revenue)
            FROM customers_view c
            LEFT JOIN orders_view o ON o.created_at = c.created_at
            GROUP BY 1
            "#,
        true,
    )
    .await
    .unwrap()
    .as_logical_plan();

    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec!["orders_view.revenue".to_string()]),
            dimensions: Some(vec![]),
            segments: Some(vec![]),
            time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                dimension: "customers_view.created_at".to_string(),
                granularity: Some("day".to_string()),
                date_range: None,
            }]),
            order: Some(vec![]),
            filters: Some(vec![set_filter("customers_view.created_at")]),
            join_hints: Some(vec![vec![
                "customers_view".to_string(),
                "orders_view".to_string(),
            ]]),
            ..Default::default()
        }
    )
}

/// Joining two views directly on `DATE_TRUNC('day', ...)` (which the SQL planner
/// lowers to `Filter(<eq>, CrossJoin(...))`, i.e. an INNER join) merges into a
/// single multi-fact CubeScan. Both truncated keys are marked present (INNER).
#[tokio::test]
async fn test_inner_join_on_date_trunc_group_by_date_trunc() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let logical_plan = plan_view_join(
        r#"
            SELECT DATE_TRUNC('day', c.created_at), measure(o.revenue)
            FROM customers_view c
            LEFT JOIN orders_view o
                ON DATE_TRUNC('day', o.created_at) = DATE_TRUNC('day', c.created_at)
            GROUP BY 1
            "#,
        true,
    )
    .await
    .unwrap()
    .as_logical_plan();

    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec!["orders_view.revenue".to_string()]),
            dimensions: Some(vec![]),
            segments: Some(vec![]),
            time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                dimension: "customers_view.created_at".to_string(),
                granularity: Some("day".to_string()),
                date_range: None,
            }]),
            order: Some(vec![]),
            filters: Some(vec![
                set_filter("orders_view.created_at"),
                set_filter("customers_view.created_at"),
            ]),
            join_hints: Some(vec![vec![
                "customers_view".to_string(),
                "orders_view".to_string(),
            ]]),
            ..Default::default()
        }
    )
}
