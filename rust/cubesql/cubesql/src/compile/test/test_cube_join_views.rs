use cubeclient::models::V1LoadRequestQuery;
use pretty_assertions::assert_eq;

use crate::{
    compile::{
        rewrite::rewriter::Rewriter,
        test::{
            convert_select_to_query_plan_with_meta, init_testing_logger,
            utils::LogicalPlanTestUtils,
        },
    },
    transport::{CubeMeta, CubeMetaDimension, CubeMetaMeasure},
};
use cubeclient::models::V1CubeMetaType;

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

/// A join between two views on a dimension that resolves to the same
/// underlying cube member (`customers.customer_city`) should be merged into a
/// single CubeScan over the combined members, exactly like a regular
/// cube-to-cube join.
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
            ungrouped: Some(true),
            join_hints: Some(vec![vec![
                "customers_view".to_string(),
                "orders_view".to_string(),
            ]]),
            ..Default::default()
        }
    )
}

/// The motivating query: a grouped (multi-fact) query selecting a dimension
/// and measures from each view, joined on the shared `customer_city`. The two
/// view scans are merged into a single grouped CubeScan over the combined
/// members.
#[tokio::test]
async fn test_group_by_join_two_views_on_shared_member() {
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
            join_hints: Some(vec![vec![
                "customers_view".to_string(),
                "orders_view".to_string(),
            ]]),
            ..Default::default()
        }
    )
}
