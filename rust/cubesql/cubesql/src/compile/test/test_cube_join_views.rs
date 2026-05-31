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

/// Two views that both expose the same underlying `Customers.city`
/// dimension (via `aliasMember`). `OrdersView` carries an `Orders`
/// measure while `CustomersView` carries a `Customers` measure, so a
/// query touching both is a multi-fact query joined on the shared key.
fn views_meta() -> Vec<CubeMeta> {
    let dimension = |name: &str, alias: &str| CubeMetaDimension {
        name: name.to_string(),
        r#type: "string".to_string(),
        alias_member: Some(alias.to_string()),
        ..CubeMetaDimension::default()
    };
    let measure = |name: &str, alias: &str| CubeMetaMeasure {
        name: name.to_string(),
        title: None,
        short_title: None,
        description: None,
        r#type: "number".to_string(),
        agg_type: Some("sum".to_string()),
        meta: None,
        alias_member: Some(alias.to_string()),
        format: None,
        format_description: None,
        currency: None,
    };

    vec![
        CubeMeta {
            name: "OrdersView".to_string(),
            description: None,
            title: None,
            r#type: V1CubeMetaType::View,
            dimensions: vec![dimension("OrdersView.city", "Customers.city")],
            measures: vec![measure("OrdersView.revenue", "Orders.revenue")],
            segments: vec![],
            joins: None,
            folders: None,
            nested_folders: None,
            hierarchies: None,
            meta: None,
        },
        CubeMeta {
            name: "CustomersView".to_string(),
            description: None,
            title: None,
            r#type: V1CubeMetaType::View,
            dimensions: vec![dimension("CustomersView.city", "Customers.city")],
            measures: vec![measure("CustomersView.amount", "Customers.amount")],
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
/// underlying cube member (`Customers.city`) should be merged into a
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
            FROM OrdersView
            LEFT JOIN CustomersView ON (OrdersView.city = CustomersView.city)
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
                "OrdersView.revenue".to_string(),
                "CustomersView.amount".to_string(),
            ]),
            dimensions: Some(vec![
                "OrdersView.city".to_string(),
                "CustomersView.city".to_string(),
            ]),
            segments: Some(vec![]),
            order: Some(vec![]),
            ungrouped: Some(true),
            join_hints: Some(vec![vec![
                "OrdersView".to_string(),
                "CustomersView".to_string(),
            ]]),
            ..Default::default()
        }
    )
}
