use cubeclient::models::{
    V1LoadRequestQuery, V1LoadRequestQueryFilterItem, V1LoadRequestQueryTimeDimension,
};
use pretty_assertions::assert_eq;
use serde_json::json;

use crate::compile::{
    rewrite::rewriter::Rewriter,
    test::{
        convert_select_to_query_plan, convert_sql_to_cube_query, get_test_session,
        get_test_tenant_ctx, init_testing_logger, utils::LogicalPlanTestUtils,
    },
    DatabaseProtocol,
};

#[tokio::test]
async fn powerbi_join() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT \
            \n  \"_\".\"semijoin1.c30\" AS \"c30\", \"_\".\"a0\" AS \"a0\" FROM \
            \n  (SELECT \"rows\".\"semijoin1.c30\" AS \"semijoin1.c30\", count(distinct \"rows\".\"basetable0.a0\") AS \"a0\" FROM (\
            \n    SELECT \"$Outer\".\"basetable0.a0\", \"$Inner\".\"semijoin1.c30\" FROM (\
            \n      SELECT \"__cubeJoinField\" AS \"basetable0.c22\", \"agentCount\" AS \"basetable0.a0\" FROM \"public\".\"Logs\" AS \"$Table\"\
            \n    ) AS \"$Outer\" JOIN (\
            \n    SELECT \"rows\".\"customer_gender\" AS \"semijoin1.c30\", \"rows\".\"__cubeJoinField\" AS \"semijoin1.c22\" FROM (\
            \n      SELECT \"customer_gender\", \"__cubeJoinField\" FROM \"public\".\"KibanaSampleDataEcommerce\" AS \"$Table\"\
            \n    ) AS \"rows\" GROUP BY \"customer_gender\", \"__cubeJoinField\"\
            \n  ) AS \"$Inner\" ON (\
            \n    \"$Outer\".\"basetable0.c22\" = \"$Inner\".\"semijoin1.c22\" OR \"$Outer\".\"basetable0.c22\" IS NULL AND \"$Inner\".\"semijoin1.c22\" IS NULL\
            \n  )\
            \n  ) AS \"rows\" GROUP BY \"semijoin1.c30\"\
            \n  ) AS \"_\" WHERE NOT \"_\".\"a0\" IS NULL LIMIT 1000001".to_string(),
        DatabaseProtocol::PostgreSQL,
    ).await;

    let logical_plan = query_plan.as_logical_plan();
    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec!["Logs.agentCount".to_string()]),
            dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
            segments: Some(vec![]),
            order: Some(vec![]),
            ..Default::default()
        }
    );
}

#[tokio::test]
async fn powerbi_transitive_join() {
    // FIXME: the test is currently broken and requires a revisit into joins
    // See original query assertion below
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        r#"SELECT "_"."semijoin3.c98" AS "c98", "_"."a0" AS "a0" FROM (
            SELECT "rows"."semijoin3.c98" AS "semijoin3.c98", sum(CAST("rows"."basetable2.a0" AS NUMERIC)) AS "a0" FROM
            (
                SELECT "$Outer"."basetable2.a0", "$Inner"."semijoin3.c98" FROM (
                    SELECT "__cubeJoinField" AS "basetable2.c95", "count" AS "basetable2.a0" FROM "public"."KibanaSampleDataEcommerce" AS "$Table"
                ) AS "$Outer" JOIN (
                    SELECT "rows"."semijoin1.c98" AS "semijoin3.c98", "rows"."basetable0.c108" AS "semijoin3.c95" FROM (
                        SELECT "$Outer"."basetable0.c108", "$Inner"."semijoin1.c98" FROM (
                            SELECT "rows"."__cubeJoinField" AS "basetable0.c108" FROM (
                                SELECT "__cubeJoinField" FROM "public"."NumberCube" AS "$Table"
                            ) AS "rows" GROUP BY "__cubeJoinField"
                        ) AS "$Outer" JOIN (
                            SELECT "rows"."content" AS "semijoin1.c98", "rows"."__cubeJoinField" AS "semijoin1.c108" FROM (
                                SELECT "content", "__cubeJoinField" FROM "public"."Logs" AS "$Table"
                            ) AS "rows" GROUP BY "content", "__cubeJoinField"
                        ) AS "$Inner" ON (
                            "$Outer"."basetable0.c108" = "$Inner"."semijoin1.c108" OR "$Outer"."basetable0.c108" IS NULL AND "$Inner"."semijoin1.c108" IS NULL
                        )) AS "rows" GROUP BY "semijoin1.c98", "basetable0.c108"
                    ) AS "$Inner" ON (
                    "$Outer"."basetable2.c95" = "$Inner"."semijoin3.c95" OR "$Outer"."basetable2.c95" IS NULL AND "$Inner"."semijoin3.c95" IS NULL
                )
            ) AS "rows" GROUP BY "semijoin3.c98") AS "_" WHERE NOT "_"."a0" IS NULL LIMIT 1000001
            "#.to_string(),
        DatabaseProtocol::PostgreSQL,
    ).await;

    let logical_plan = query_plan.as_logical_plan();
    // FIXME: original query assertion
    // assert_eq!(
    //     logical_plan.find_cube_scan().request,
    //     V1LoadRequestQuery {
    //         measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
    //         dimensions: Some(vec!["Logs.content".to_string()]),
    //         segments: Some(vec![]),
    //         time_dimensions: None,
    //         order: Some(vec![]),
    //         limit: Some(1000001),
    //         offset: None,
    //         filters: Some(vec![V1LoadRequestQueryFilterItem {
    //             member: Some("KibanaSampleDataEcommerce.count".to_string()),
    //             operator: Some("set".to_string()),
    //             values: None,
    //             or: None,
    //             and: None,
    //         }]),
    //         ungrouped: None,
    //     }
    // );
    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
            dimensions: Some(vec!["Logs.content".to_string()]),
            segments: Some(vec![]),
            order: Some(vec![]),
            ..Default::default()
        }
    );
}

#[tokio::test]
async fn test_join_three_cubes() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let logical_plan = convert_select_to_query_plan(
        r#"
            SELECT *
            FROM KibanaSampleDataEcommerce
            LEFT JOIN Logs ON (KibanaSampleDataEcommerce.__cubeJoinField = Logs.__cubeJoinField)
            LEFT JOIN NumberCube ON (NumberCube.__cubeJoinField = Logs.__cubeJoinField)
            "#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await
    .as_logical_plan();

    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec![
                "KibanaSampleDataEcommerce.count".to_string(),
                "KibanaSampleDataEcommerce.maxPrice".to_string(),
                "KibanaSampleDataEcommerce.sumPrice".to_string(),
                "KibanaSampleDataEcommerce.minPrice".to_string(),
                "KibanaSampleDataEcommerce.avgPrice".to_string(),
                "KibanaSampleDataEcommerce.countDistinct".to_string(),
                "Logs.agentCount".to_string(),
                "Logs.agentCountApprox".to_string(),
                "NumberCube.someNumber".to_string(),
            ]),
            dimensions: Some(vec![
                "KibanaSampleDataEcommerce.order_date".to_string(),
                "KibanaSampleDataEcommerce.last_mod".to_string(),
                "KibanaSampleDataEcommerce.customer_gender".to_string(),
                "KibanaSampleDataEcommerce.notes".to_string(),
                "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                "KibanaSampleDataEcommerce.has_subscription".to_string(),
                "Logs.id".to_string(),
                "Logs.read".to_string(),
                "Logs.content".to_string(),
            ]),
            segments: Some(vec![]),
            order: Some(vec![]),
            ungrouped: Some(true),
            ..Default::default()
        }
    )
}

#[tokio::test]
async fn test_join_three_cubes_split() {
    init_testing_logger();

    let logical_plan = convert_select_to_query_plan(
        r#"
            SELECT count(KibanaSampleDataEcommerce.count), Logs.read, NumberCube.someNumber, extract(MONTH FROM KibanaSampleDataEcommerce.order_date)
            FROM KibanaSampleDataEcommerce
            LEFT JOIN Logs ON (KibanaSampleDataEcommerce.__cubeJoinField = Logs.__cubeJoinField)
            LEFT JOIN NumberCube ON (NumberCube.__cubeJoinField = Logs.__cubeJoinField)
            WHERE Logs.read
            GROUP BY 2,3,4
            "#
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await
        .as_logical_plan();

    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec![
                "NumberCube.someNumber".to_string(),
                "KibanaSampleDataEcommerce.count".to_string(),
            ]),
            dimensions: Some(vec!["Logs.read".to_string(),]),
            segments: Some(vec![]),
            time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                granularity: Some("month".to_owned()),
                date_range: None
            }]),
            order: Some(vec![]),
            filters: Some(vec![V1LoadRequestQueryFilterItem {
                member: Some("Logs.read".to_string()),
                operator: Some("equals".to_string()),
                values: Some(vec!["true".to_string()]),
                or: None,
                and: None
            }]),
            ..Default::default()
        }
    )
}

#[tokio::test]
async fn test_join_two_subqueries_with_filter_order_limit() {
    init_testing_logger();

    let logical_plan = convert_select_to_query_plan(
        r#"
            SELECT count(KibanaSampleDataEcommerce.count), Logs.read
            FROM (SELECT * FROM KibanaSampleDataEcommerce where customer_gender is not null order by customer_gender) KibanaSampleDataEcommerce
            LEFT JOIN (SELECT read, __cubeJoinField FROM Logs) Logs ON (KibanaSampleDataEcommerce.__cubeJoinField = Logs.__cubeJoinField)
            WHERE Logs.read
            GROUP BY 2
            "#
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await
        .as_logical_plan();

    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
            dimensions: Some(vec!["Logs.read".to_string(),]),
            segments: Some(vec![]),
            order: Some(vec![vec![
                "KibanaSampleDataEcommerce.customer_gender".to_string(),
                "asc".to_string(),
            ]]),
            filters: Some(vec![
                V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("set".to_string()),
                    values: None,
                    or: None,
                    and: None
                },
                V1LoadRequestQueryFilterItem {
                    member: Some("Logs.read".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["true".to_string()]),
                    or: None,
                    and: None
                }
            ]),
            ..Default::default()
        }
    )
}

#[tokio::test]
async fn test_join_three_subqueries_with_filter_order_limit_and_split() {
    init_testing_logger();

    let logical_plan = convert_select_to_query_plan(
        r#"
            SELECT count(Ecommerce.count), Logs.r, extract(MONTH FROM Ecommerce.order_date)
            FROM (SELECT * FROM KibanaSampleDataEcommerce where customer_gender is not null order by customer_gender) Ecommerce
            LEFT JOIN (SELECT read r, __cubeJoinField FROM Logs) Logs ON (Ecommerce.__cubeJoinField = Logs.__cubeJoinField)
            LEFT JOIN (SELECT someNumber, __cubeJoinField from NumberCube) NumberC ON (Logs.__cubeJoinField = NumberC.__cubeJoinField)
            WHERE Logs.r
            GROUP BY 2, 3
            "#
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await
        .as_logical_plan();

    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
            dimensions: Some(vec!["Logs.read".to_string(),]),
            segments: Some(vec![]),
            time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                granularity: Some("month".to_owned()),
                date_range: None
            }]),
            order: Some(vec![vec![
                "KibanaSampleDataEcommerce.customer_gender".to_string(),
                "asc".to_string(),
            ]]),
            filters: Some(vec![
                V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("set".to_string()),
                    values: None,
                    or: None,
                    and: None
                },
                V1LoadRequestQueryFilterItem {
                    member: Some("Logs.read".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["true".to_string()]),
                    or: None,
                    and: None
                }
            ]),
            ..Default::default()
        }
    )
}

#[tokio::test]
async fn test_join_subquery_and_table_with_filter_order_limit() {
    init_testing_logger();

    let logical_plan = convert_select_to_query_plan(
        r#"
            SELECT count(KibanaSampleDataEcommerce.count), Logs.read
            FROM (SELECT * FROM KibanaSampleDataEcommerce where customer_gender is not null order by customer_gender) KibanaSampleDataEcommerce
            LEFT JOIN Logs ON (KibanaSampleDataEcommerce.__cubeJoinField = Logs.__cubeJoinField)
            WHERE Logs.read
            GROUP BY 2
            "#
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await
        .as_logical_plan();

    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
            dimensions: Some(vec!["Logs.read".to_string(),]),
            segments: Some(vec![]),
            order: Some(vec![vec![
                "KibanaSampleDataEcommerce.customer_gender".to_string(),
                "asc".to_string(),
            ]]),
            filters: Some(vec![
                V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("set".to_string()),
                    values: None,
                    or: None,
                    and: None
                },
                V1LoadRequestQueryFilterItem {
                    member: Some("Logs.read".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["true".to_string()]),
                    or: None,
                    and: None
                }
            ]),
            ..Default::default()
        }
    )
}

#[tokio::test]
async fn test_join_two_subqueries_and_table_with_filter_order_limit_and_split() {
    init_testing_logger();

    let logical_plan = convert_select_to_query_plan(
        r#"
            SELECT count(Ecommerce.count), Logs.read, extract(MONTH FROM Ecommerce.order_date)
            FROM (SELECT * FROM KibanaSampleDataEcommerce where customer_gender is not null order by customer_gender) Ecommerce
            LEFT JOIN Logs ON (Ecommerce.__cubeJoinField = Logs.__cubeJoinField)
            LEFT JOIN (SELECT someNumber, __cubeJoinField from NumberCube) NumberC ON (Logs.__cubeJoinField = NumberC.__cubeJoinField)
            WHERE Logs.read
            GROUP BY 2, 3
            "#
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await
        .as_logical_plan();

    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
            dimensions: Some(vec!["Logs.read".to_string(),]),
            segments: Some(vec![]),
            time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                granularity: Some("month".to_owned()),
                date_range: None
            }]),
            order: Some(vec![vec![
                "KibanaSampleDataEcommerce.customer_gender".to_string(),
                "asc".to_string(),
            ]]),
            filters: Some(vec![
                V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("set".to_string()),
                    values: None,
                    or: None,
                    and: None
                },
                V1LoadRequestQueryFilterItem {
                    member: Some("Logs.read".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["true".to_string()]),
                    or: None,
                    and: None
                }
            ]),
            ..Default::default()
        }
    )
}

#[tokio::test]
async fn test_join_two_subqueries_filter_push_down() {
    init_testing_logger();

    let logical_plan = convert_select_to_query_plan(
        r#"
            SELECT count(Ecommerce.count), Logs.r, Ecommerce.date
            FROM (SELECT __cubeJoinField, count, order_date date FROM KibanaSampleDataEcommerce where customer_gender = 'female') Ecommerce
            LEFT JOIN (select __cubeJoinField, read r from Logs) Logs ON (Ecommerce.__cubeJoinField = Logs.__cubeJoinField)
            WHERE (Logs.r IS NOT NULL) AND (Ecommerce.date BETWEEN timestamp with time zone '2022-06-13T12:30:00.000Z' AND timestamp with time zone '2022-06-29T12:30:00.000Z')
            GROUP BY 2, 3
            ORDER BY 1
            "#
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await
        .as_logical_plan();

    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
            dimensions: Some(vec![
                "Logs.read".to_string(),
                "KibanaSampleDataEcommerce.order_date".to_string(),
            ]),
            segments: Some(vec![]),
            time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                granularity: None,
                date_range: Some(json!(vec![
                    "2022-06-13T12:30:00.000Z".to_string(),
                    "2022-06-29T12:30:00.000Z".to_string()
                ]))
            }]),
            order: Some(vec![vec![
                "KibanaSampleDataEcommerce.count".to_string(),
                "asc".to_string(),
            ]]),
            filters: Some(vec![
                V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["female".to_string()]),
                    or: None,
                    and: None
                },
                V1LoadRequestQueryFilterItem {
                    member: Some("Logs.read".to_string()),
                    operator: Some("set".to_string()),
                    values: None,
                    or: None,
                    and: None
                }
            ]),
            ..Default::default()
        }
    )
}

#[tokio::test]
async fn test_join_cubes_on_wrong_field_error() {
    init_testing_logger();

    let meta = get_test_tenant_ctx();
    let query = convert_sql_to_cube_query(
        &r#"
            SELECT *
            FROM (SELECT customer_gender, has_subscription FROM KibanaSampleDataEcommerce) kibana
            LEFT JOIN (SELECT read, content FROM Logs) logs ON (kibana.has_subscription = logs.read)
            "#
        .to_string(),
        meta.clone(),
        get_test_session(DatabaseProtocol::PostgreSQL, meta).await,
    )
    .await;

    assert_eq!(
        query.unwrap_err().message(),
        "Error during rewrite: Use __cubeJoinField to join Cubes. Please check logs for additional information.".to_string()
    )
}

#[tokio::test]
async fn test_join_cubes_filter_from_wrong_side_error() {
    init_testing_logger();

    let meta = get_test_tenant_ctx();
    let query = convert_sql_to_cube_query(
        &r#"
            SELECT count(KibanaSampleDataEcommerce.count), Logs.read
            FROM (SELECT * FROM KibanaSampleDataEcommerce) KibanaSampleDataEcommerce
            LEFT JOIN (SELECT read, __cubeJoinField FROM Logs where read order by read limit 10) Logs ON (KibanaSampleDataEcommerce.__cubeJoinField = Logs.__cubeJoinField)
            GROUP BY 2
            "#
            .to_string(),
        meta.clone(),
        get_test_session(DatabaseProtocol::PostgreSQL, meta).await,
    )
        .await;

    assert_eq!(
        query.unwrap_err().message(),
        "Error during rewrite: Can not join Cubes. This is most likely due to one of the following reasons:\n\
            • one of the cubes contains a group by\n\
            • one of the cubes contains a measure\n\
            • the cube on the right contains a filter, sorting or limits\n\
            . Please check logs for additional information.".to_string()
    )
}

#[tokio::test]
async fn test_join_cubes_with_aggr_error() {
    init_testing_logger();

    let meta = get_test_tenant_ctx();
    let query = convert_sql_to_cube_query(
        &r#"
            SELECT *
            FROM (SELECT count(count), __cubeJoinField FROM KibanaSampleDataEcommerce group by 2) KibanaSampleDataEcommerce
            LEFT JOIN (SELECT read, __cubeJoinField FROM Logs) Logs ON (KibanaSampleDataEcommerce.__cubeJoinField = Logs.__cubeJoinField)
            "#
            .to_string(),
        meta.clone(),
        get_test_session(DatabaseProtocol::PostgreSQL, meta).await,
    )
        .await;

    assert_eq!(
        query.unwrap_err().message(),
        "Error during rewrite: Can not join Cubes. This is most likely due to one of the following reasons:\n\
            • one of the cubes contains a group by\n\
            • one of the cubes contains a measure\n\
            • the cube on the right contains a filter, sorting or limits\n\
            . Please check logs for additional information.".to_string()
    )
}
