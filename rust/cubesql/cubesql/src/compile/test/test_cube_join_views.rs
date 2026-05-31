use cubeclient::models::{V1CubeMetaType, V1LoadRequestQuery, V1LoadRequestQueryFilterItem};
use pretty_assertions::assert_eq;

use crate::{
    compile::{
        rewrite::rewriter::Rewriter,
        test::{
            convert_select_to_query_plan_with_meta, convert_sql_to_cube_query, get_test_session,
            get_test_tenant_ctx_with_meta, init_testing_logger, utils::LogicalPlanTestUtils,
        },
        CompilationError, DatabaseProtocol,
    },
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
            dimensions: vec![dimension(
                "customers_view.customer_city",
                "customers.customer_city",
            )],
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
            dimensions: vec![dimension(
                "orders_view.customer_city",
                "customers.customer_city",
            )],
            measures: vec![measure("orders_view.revenue", "orders.revenue", "sum")],
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

/// A join between two views on a dimension that resolves to the same
/// underlying cube member (`customers.customer_city`) should be merged into a
/// single CubeScan over the combined members, exactly like a regular
/// cube-to-cube join. As a `LEFT JOIN`, the left ("must be present") side is
/// guarded with a `set` filter on its join key so the downstream FULL OUTER
/// multi-fact stitch keeps left-join semantics.
#[tokio::test]
async fn test_join_two_views_on_shared_member() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let logical_plan = convert_select_to_query_plan_with_meta(
        r#"
            SELECT *
            FROM customers_view
            LEFT JOIN orders_view
                ON (orders_view.customer_city = customers_view.customer_city)
            "#
        .to_string(),
        views_meta(),
    )
    .await
    .as_logical_plan();

    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec![
                "customers_view.avg_age".to_string(),
                "orders_view.revenue".to_string(),
            ]),
            dimensions: Some(vec![
                "customers_view.customer_city".to_string(),
                "orders_view.customer_city".to_string(),
            ]),
            segments: Some(vec![]),
            order: Some(vec![]),
            filters: Some(vec![set_filter("customers_view.customer_city")]),
            ungrouped: Some(true),
            join_hints: Some(vec![vec![
                "customers_view".to_string(),
                "orders_view".to_string(),
            ]]),
            ..Default::default()
        }
    )
}

/// The motivating query: a grouped (multi-fact) `LEFT JOIN` selecting a
/// dimension and measures from each view, joined on the shared `customer_city`.
/// The two view scans are merged into a single grouped CubeScan, and the left
/// join key gets a `set` filter to recover LEFT-join semantics on top of the
/// FULL OUTER multi-fact stitch.
#[tokio::test]
async fn test_group_by_left_join_two_views_on_shared_member() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let logical_plan = convert_select_to_query_plan_with_meta(
        r#"
            SELECT c.customer_city, measure(o.revenue), measure(c.avg_age)
            FROM customers_view c
            LEFT JOIN orders_view o ON o.customer_city = c.customer_city
            GROUP BY 1
            "#
        .to_string(),
        views_meta(),
    )
    .await
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

    let logical_plan = convert_select_to_query_plan_with_meta(
        r#"
            SELECT c.customer_city, measure(o.revenue), measure(c.avg_age)
            FROM customers_view c
            INNER JOIN orders_view o ON o.customer_city = c.customer_city
            GROUP BY 1
            "#
        .to_string(),
        views_meta(),
    )
    .await
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

/// The merge only fires when the join key is fully within dimensions. Joining
/// the two views on a measure (`o.revenue = c.avg_age`) is not a shared-member
/// dimension join, so the scans are not merged and the query is rejected the
/// same way any other unsupported cube join is.
#[tokio::test]
async fn test_join_two_views_on_measure_is_not_merged() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let meta = get_test_tenant_ctx_with_meta(views_meta());
    let query = convert_sql_to_cube_query(
        &r#"
            SELECT *
            FROM customers_view c
            LEFT JOIN orders_view o ON (o.revenue = c.avg_age)
            "#
        .to_string(),
        meta.clone(),
        get_test_session(DatabaseProtocol::PostgreSQL, meta).await,
    )
    .await;

    let error = query.unwrap_err();
    assert!(matches!(error, CompilationError::Rewrite(..)));
}
