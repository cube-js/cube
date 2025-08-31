use self::engine::CubeContext;

pub mod builder;
pub mod engine;
pub mod error;
pub mod parser;
pub mod plan;
mod protocol;
pub mod qtrace;
pub mod query_engine;
pub mod rewrite;
pub mod router;
pub mod service;
pub mod session;

// Internal API
mod date_parser;
pub mod test;

// Re-export for Public API
pub use error::*;
pub use plan::*;
pub use protocol::*;
pub use query_engine::*;
pub use rewrite::rewriter::Rewriter;
pub use router::*;
pub use session::*;

// Re-export base deps to minimise version maintenance for crate users such as cloud
pub use datafusion::{self, arrow};

#[cfg(test)]
mod tests {
    use super::{
        test::{get_test_session, get_test_tenant_ctx},
        *,
    };
    use crate::{
        compile::{
            engine::df::scan::MemberField,
            rewrite::rewriter::Rewriter,
            test::{get_sixteen_char_member_cube, get_string_cube_meta},
        },
        CubeError,
    };
    use chrono::Datelike;
    use cubeclient::models::{
        V1LoadRequestQuery, V1LoadRequestQueryFilterItem, V1LoadRequestQueryTimeDimension,
        V1LoadResponse, V1LoadResult, V1LoadResultAnnotation,
    };
    use datafusion::{arrow::datatypes::DataType, physical_plan::displayable};
    use itertools::Itertools;
    use pretty_assertions::assert_eq;
    use regex::Regex;
    use serde_json::json;
    use std::env;

    use crate::compile::test::{
        convert_select_to_query_plan, convert_select_to_query_plan_customized,
        convert_select_to_query_plan_with_meta, execute_queries_with_flags, execute_query,
        init_testing_logger, LogicalPlanTestUtils, TestContext,
    };

    #[tokio::test]
    async fn test_select_measure_via_function() {
        let query_plan = convert_select_to_query_plan(
            "SELECT MEASURE(maxPrice), MEASURE(minPrice), MEASURE(avgPrice) FROM KibanaSampleDataEcommerce".to_string(),
        DatabaseProtocol::PostgreSQL).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.minPrice".to_string(),
                    "KibanaSampleDataEcommerce.avgPrice".to_string(),
                ]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_select_dimensions_substring() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            // language=PostgreSQL
            r#"
                SELECT (SUBSTR(
                    dim_str0,
                    CAST(dim_num1 AS INTEGER),
                    CAST(dim_num2 AS INTEGER)
                )) AS result
                FROM MultiTypeCube
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
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![
                    "MultiTypeCube.dim_str0".to_string(),
                    "MultiTypeCube.dim_num1".to_string(),
                    "MultiTypeCube.dim_num2".to_string(),
                ]),
                order: Some(vec![]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_coalesce_two_dimensions() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            // language=PostgreSQL
            r#"
            SELECT COALESCE(dim_str0, dim_str1, '(none)')
            FROM MultiTypeCube
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
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![
                    "MultiTypeCube.dim_str0".to_string(),
                    "MultiTypeCube.dim_str1".to_string(),
                ]),
                order: Some(vec![]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_select_number() {
        let query_plan = convert_select_to_query_plan(
            "SELECT MEASURE(someNumber) as s1, SUM(someNumber) as s2, MIN(someNumber) as s3, MAX(someNumber) as s4, COUNT(someNumber) as s5 FROM NumberCube".to_string(),
            DatabaseProtocol::PostgreSQL).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["NumberCube.someNumber".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_select_null_if_measure_diff() {
        let query_plan = convert_select_to_query_plan(
            "SELECT MEASURE(count), NULLIF(MEASURE(count), 0) as t, MEASURE(count) / NULLIF(MEASURE(count), 0) FROM KibanaSampleDataEcommerce;".to_string(),
        DatabaseProtocol::PostgreSQL).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_select_compound_identifiers() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT MEASURE(`KibanaSampleDataEcommerce`.`maxPrice`) AS maxPrice, MEASURE(`KibanaSampleDataEcommerce`.`minPrice`) AS minPrice FROM KibanaSampleDataEcommerce".to_string(), DatabaseProtocol::MySQL
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.minPrice".to_string(),
                ]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_select_measure_aggregate_functions() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT MAX(maxPrice), MIN(minPrice), AVG(avgPrice) FROM KibanaSampleDataEcommerce"
                .to_string(),
            DatabaseProtocol::MySQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.minPrice".to_string(),
                    "KibanaSampleDataEcommerce.avgPrice".to_string(),
                ]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }
        );

        assert_eq!(
            logical_plan
                .schema()
                .fields()
                .iter()
                .map(|f| f.data_type().clone())
                .collect::<Vec<_>>(),
            vec![DataType::Float64, DataType::Float64, DataType::Float64]
        );
    }

    #[tokio::test]
    async fn test_starts_with() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce WHERE starts_with(customer_gender, 'fe')"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await;

        let cube_scan = query_plan.as_logical_plan().find_cube_scan();

        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("startsWith".to_string()),
                    values: Some(vec!["fe".to_string()]),
                    or: None,
                    and: None
                }]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_ends_with_query() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce WHERE ends_with(customer_gender, 'emale')"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await;

        let cube_scan = query_plan.as_logical_plan().find_cube_scan();

        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("endsWith".to_string()),
                    values: Some(vec!["emale".to_string()]),
                    or: None,
                    and: None
                }]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_lower_in_thoughtspot() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            "SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce WHERE LOWER(customer_gender) IN ('female')"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await.as_logical_plan();

        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;

        assert!(sql.contains("LOWER("));
        assert!(sql.contains(" IN ("));

        let logical_plan = convert_select_to_query_plan(
            "SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce WHERE LOWER(customer_gender) IN ('female', 'male')".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await.as_logical_plan();

        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;

        assert!(sql.contains("LOWER("));
        assert!(sql.contains(" IN ("));
    }

    #[tokio::test]
    async fn test_lower_equals_thoughtspot() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            "SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce WHERE LOWER(customer_gender) = 'female'"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await.as_logical_plan();

        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;

        assert!(sql.contains("LOWER("));
    }

    #[tokio::test]
    async fn test_order_alias_for_measure_default() {
        let query_plan = convert_select_to_query_plan(
            "SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce ORDER BY cnt".to_string(),
            DatabaseProtocol::MySQL,
        )
        .await;

        assert_eq!(
            query_plan.as_logical_plan().find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "asc".to_string(),
                ]]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_order_by() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let supported_orders = vec![
            // test_order_alias_for_dimension_default
            (
                "SELECT taxful_total_price as total_price FROM KibanaSampleDataEcommerce ORDER BY total_price".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    ]),
                    order: Some(vec![vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                        "asc".to_string(),
                    ]]),
                    ungrouped: Some(true),
                    ..Default::default()
                }
            ),
            (
                "SELECT COUNT(*) count, customer_gender, order_date FROM KibanaSampleDataEcommerce GROUP BY customer_gender, order_date ORDER BY order_date".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.customer_gender".to_string(),
                        "KibanaSampleDataEcommerce.order_date".to_string(),
                    ]),
                    order: Some(vec![vec![
                        "KibanaSampleDataEcommerce.order_date".to_string(),
                        "asc".to_string(),
                    ]]),
                    ..Default::default()
                }
            ),
            // test_order_indentifier_default
            (
                "SELECT taxful_total_price FROM KibanaSampleDataEcommerce ORDER BY taxful_total_price".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    ]),
                    order: Some(vec![vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                        "asc".to_string(),
                    ]]),
                    ungrouped: Some(true),
                    ..Default::default()
                }
            ),
            // test_order_compound_identifier_default
            (
                "SELECT taxful_total_price FROM `db`.`KibanaSampleDataEcommerce` ORDER BY `KibanaSampleDataEcommerce`.`taxful_total_price`".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    ]),
                    order: Some(vec![vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                        "asc".to_string(),
                    ]]),
                    ungrouped: Some(true),
                    ..Default::default()
                }
            ),
            // test_order_indentifier_asc
            (
                "SELECT taxful_total_price FROM KibanaSampleDataEcommerce ORDER BY taxful_total_price ASC".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    ]),
                    order: Some(vec![vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                        "asc".to_string(),
                    ]]),
                    ungrouped: Some(true),
                    ..Default::default()
                }
            ),
            // test_order_indentifier_desc
            (
                "SELECT taxful_total_price FROM KibanaSampleDataEcommerce ORDER BY taxful_total_price DESC".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    ]),
                    order: Some(vec![vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                        "desc".to_string(),
                    ]]),
                    ungrouped: Some(true),
                    ..Default::default()
                }
            ),
            // test_order_identifer_alias_ident_no_escape
            (
                "SELECT taxful_total_price as alias1 FROM KibanaSampleDataEcommerce ORDER BY alias1 DESC".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    ]),
                    order: Some(vec![vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                        "desc".to_string(),
                    ]]),
                    ungrouped: Some(true),
                    ..Default::default()
                }
            ),
            // test_order_identifer_alias_ident_escape
            (
                "SELECT taxful_total_price as `alias1` FROM KibanaSampleDataEcommerce ORDER BY `alias1` DESC".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    ]),
                    order: Some(vec![vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                        "desc".to_string(),
                    ]]),
                    ungrouped: Some(true),
                    ..Default::default()
                }
            ),
        ];

        for (sql, expected_request) in supported_orders.iter() {
            let query_plan =
                convert_select_to_query_plan(sql.to_string(), DatabaseProtocol::MySQL).await;

            assert_eq!(
                &query_plan.as_logical_plan().find_cube_scan().request,
                expected_request
            )
        }
    }

    #[tokio::test]
    async fn test_order_function_date() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT DATE(order_date) FROM KibanaSampleDataEcommerce ORDER BY DATE(order_date) DESC"
                .to_string(),
            DatabaseProtocol::MySQL,
        )
        .await;

        assert_eq!(
            query_plan.as_logical_plan().find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("day".to_owned()),
                    date_range: None
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "desc".to_string(),
                ]]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );

        let query_plan = convert_select_to_query_plan(
            "SELECT DATE(order_date) FROM KibanaSampleDataEcommerce GROUP BY DATE(order_date) ORDER BY DATE(order_date) DESC"
                .to_string(),
            DatabaseProtocol::MySQL,
        ).await;

        assert_eq!(
            query_plan.as_logical_plan().find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("day".to_owned()),
                    date_range: None
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "desc".to_string(),
                ]]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_select_all_fields_by_asterisk_limit_100() {
        let query_plan = convert_select_to_query_plan(
            "SELECT * FROM KibanaSampleDataEcommerce LIMIT 100".to_string(),
            DatabaseProtocol::MySQL,
        )
        .await;

        assert_eq!(
            query_plan
                .as_logical_plan()
                .find_cube_scan()
                .request
                .dimensions,
            Some(vec![
                "KibanaSampleDataEcommerce.id".to_string(),
                "KibanaSampleDataEcommerce.order_date".to_string(),
                "KibanaSampleDataEcommerce.last_mod".to_string(),
                "KibanaSampleDataEcommerce.customer_gender".to_string(),
                "KibanaSampleDataEcommerce.notes".to_string(),
                "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                "KibanaSampleDataEcommerce.has_subscription".to_string(),
            ])
        )
    }

    #[tokio::test]
    async fn test_select_all_fields_by_asterisk_limit_100_offset_50() {
        let query_plan = convert_select_to_query_plan(
            "SELECT * FROM KibanaSampleDataEcommerce LIMIT 100 OFFSET 50".to_string(),
            DatabaseProtocol::MySQL,
        )
        .await;

        assert_eq!(
            query_plan
                .as_logical_plan()
                .find_cube_scan()
                .request
                .dimensions,
            Some(vec![
                "KibanaSampleDataEcommerce.id".to_string(),
                "KibanaSampleDataEcommerce.order_date".to_string(),
                "KibanaSampleDataEcommerce.last_mod".to_string(),
                "KibanaSampleDataEcommerce.customer_gender".to_string(),
                "KibanaSampleDataEcommerce.notes".to_string(),
                "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                "KibanaSampleDataEcommerce.has_subscription".to_string(),
            ])
        )
    }

    #[tokio::test]
    async fn test_select_two_fields() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        let query_plan = convert_select_to_query_plan(
            "SELECT order_date, customer_gender FROM KibanaSampleDataEcommerce".to_string(),
            DatabaseProtocol::MySQL,
        )
        .await;

        assert_eq!(
            query_plan.as_logical_plan().find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                ]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_select_fields_alias() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        let query_plan = convert_select_to_query_plan(
            "SELECT order_date as order_date, customer_gender as customer_gender FROM KibanaSampleDataEcommerce"
                .to_string(), DatabaseProtocol::MySQL
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                ]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );

        // assert_eq!(
        //     logical_plan.schema().clone(),
        //     Arc::new(
        //         DFSchema::new_with_metadata(
        //             vec![
        //                 DFField::new(None, "order_date", DataType::Utf8, false),
        //                 DFField::new(None, "customer_gender", DataType::Utf8, false),
        //             ],
        //             HashMap::new()
        //         )
        //         .unwrap()
        //     ),
        // );
    }

    #[tokio::test]
    async fn test_select_where_false() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT * FROM KibanaSampleDataEcommerce WHERE 1 = 0".to_string(),
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
                    "KibanaSampleDataEcommerce.sumPrice".to_string(),
                    "KibanaSampleDataEcommerce.minPrice".to_string(),
                    "KibanaSampleDataEcommerce.avgPrice".to_string(),
                    "KibanaSampleDataEcommerce.countDistinct".to_string(),
                ]),
                segments: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.id".to_string(),
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "KibanaSampleDataEcommerce.last_mod".to_string(),
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "KibanaSampleDataEcommerce.notes".to_string(),
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    "KibanaSampleDataEcommerce.has_subscription".to_string(),
                ]),
                order: Some(vec![]),
                limit: Some(0),
                ungrouped: Some(true),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn tableau_projection_with_casts() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT \
             CAST(\"KibanaSampleDataEcommerce\".\"customer_gender\" AS TEXT) AS \"customer_gender\",\
             \"KibanaSampleDataEcommerce\".\"count\" AS \"count\",\
             \"KibanaSampleDataEcommerce\".\"maxPrice\" AS \"maxPrice\",\
             \"KibanaSampleDataEcommerce\".\"minPrice\" AS \"minPrice\",\
             \"KibanaSampleDataEcommerce\".\"avgPrice\" AS \"avgPrice\",\
             \"KibanaSampleDataEcommerce\".\"order_date\" AS \"order_date\",\
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price1\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price2\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price3\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price4\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price5\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price6\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price7\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price8\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price9\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price10\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price11\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price12\"
             FROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.minPrice".to_string(),
                    "KibanaSampleDataEcommerce.avgPrice".to_string(),
                ]),
                segments: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                ]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn tableau_min_max() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT MIN(\"KibanaSampleDataEcommerce\".\"order_date\") AS \"tmn:timestamp:min\", MAX(\"KibanaSampleDataEcommerce\".\"order_date\") AS \"tmn:timestamp:max\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn tableau_min_max_number() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT MIN(\"KibanaSampleDataEcommerce\".\"taxful_total_price\") AS \"tmn:timestamp:min\", MAX(\"KibanaSampleDataEcommerce\".\"taxful_total_price\") AS \"tmn:timestamp:max\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn tableau_filter_and_group_by() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price\" FROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\" WHERE (CAST(\"KibanaSampleDataEcommerce\".\"customer_gender\" AS TEXT) = 'female') GROUP BY 1".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                ]),
                order: Some(vec![]),
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

    #[tokio::test]
    async fn sum_to_count_push_down() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT SUM(1) AS \"count\" FROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn tableau_having_count_on_cube_without_count() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT COUNT(DISTINCT \"Logs\".\"agentCount\") AS \"sum:count:ok\" FROM \"public\".\"Logs\" \"Logs\" HAVING (COUNT(1) > 0)".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn tableau_having_count_with_sum_on_cube_without_count() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT COUNT(DISTINCT \"Logs\".\"agentCount\") AS \"sum:count:ok\", SUM(1) AS \"count:ok\" FROM \"public\".\"Logs\" \"Logs\" HAVING (COUNT(1) > 0)".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn tableau_boolean_filter_inplace_where() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:count:ok\" FROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\" WHERE \"KibanaSampleDataEcommerce\".\"is_female\" HAVING (COUNT(1) > 0)".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec!["KibanaSampleDataEcommerce.is_female".to_string()]),
                dimensions: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.count".to_string()),
                    operator: Some("gt".to_string()),
                    values: Some(vec!["0".to_string()]),
                    or: None,
                    and: None,
                }]),
                ..Default::default()
            }
        );

        let query_plan = convert_select_to_query_plan(
            "SELECT SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:count:ok\" FROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\" WHERE NOT(\"KibanaSampleDataEcommerce\".\"has_subscription\") HAVING (COUNT(1) > 0)".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.has_subscription".to_string()),
                        operator: Some("equals".to_string()),
                        values: Some(vec!["false".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.count".to_string()),
                        operator: Some("gt".to_string()),
                        values: Some(vec!["0".to_string()]),
                        or: None,
                        and: None,
                    }
                ]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn tableau_not_null_filter() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price\" FROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\" WHERE (NOT (\"KibanaSampleDataEcommerce\".\"taxful_total_price\" IS NULL)) GROUP BY 1".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                ]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                    operator: Some("set".to_string()),
                    values: None,
                    or: None,
                    and: None,
                }]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn tableau_current_timestamp() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS \"COL\"".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = &query_plan.print(true).unwrap();

        assert_eq!(
            logical_plan,
            "Projection: CAST(utctimestamp() AS current_timestamp AS Timestamp(Nanosecond, None)) AS COL\
            \n  EmptyRelation",
        );
    }

    #[tokio::test]
    async fn tableau_time_filter() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:count:ok\" FROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\" WHERE ((\"KibanaSampleDataEcommerce\".\"order_date\" >= (TIMESTAMP '2020-12-25 22:48:48.000')) AND (\"KibanaSampleDataEcommerce\".\"order_date\" <= (TIMESTAMP '2022-04-01 00:00:00.000')))".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2020-12-25T22:48:48.000Z".to_string(),
                        "2022-04-01T00:00:00.000Z".to_string()
                    ]))
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn superset_pg_time_filter() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT DATE_TRUNC('week', \"order_date\") AS __timestamp,
               count(count) AS \"COUNT(count)\"
FROM public.\"KibanaSampleDataEcommerce\"
WHERE \"order_date\" >= TO_TIMESTAMP('2021-05-15 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
  AND \"order_date\" < TO_TIMESTAMP('2022-05-15 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
GROUP BY DATE_TRUNC('week', \"order_date\")
ORDER BY \"COUNT(count)\" DESC"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("week".to_string()),
                    date_range: Some(json!(vec![
                        "2021-05-15T00:00:00.000Z".to_string(),
                        "2022-05-14T23:59:59.999Z".to_string()
                    ]))
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "desc".to_string()
                ]]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn superset_pg_time_filter_with_filter() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT DATE_TRUNC('week', \"order_date\") AS __timestamp,
               count(count) AS \"COUNT(count)\"
FROM public.\"KibanaSampleDataEcommerce\"
WHERE \"customer_gender\" = 'female' AND \"order_date\" >= TO_TIMESTAMP('2021-05-15 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
  AND \"order_date\" < TO_TIMESTAMP('2022-05-15 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
GROUP BY DATE_TRUNC('week', \"order_date\")
ORDER BY \"COUNT(count)\" DESC"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("week".to_string()),
                    date_range: Some(json!(vec![
                        "2021-05-15T00:00:00.000Z".to_string(),
                        "2022-05-14T23:59:59.999Z".to_string()
                    ]))
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "desc".to_string()
                ]]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["female".to_string()]),
                    or: None,
                    and: None
                }]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn superset_pg_time_filter_with_in_filter() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT \"notes\", DATE_TRUNC('week', \"order_date\") AS __timestamp,
               count(count) AS \"COUNT(count)\"
FROM public.\"KibanaSampleDataEcommerce\"
WHERE \"notes\" IN ('1', '2', '3', '4', '5') AND \"is_female\" = true AND \"order_date\" >= TO_TIMESTAMP('2021-05-15 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
  AND \"order_date\" < TO_TIMESTAMP('2022-05-15 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
GROUP BY \"notes\", DATE_TRUNC('week', \"order_date\")
ORDER BY \"COUNT(count)\" DESC LIMIT 10000"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec!["KibanaSampleDataEcommerce.is_female".to_string()]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.notes".to_string()]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("week".to_string()),
                    date_range: Some(json!(vec![
                        "2021-05-15T00:00:00.000Z".to_string(),
                        "2022-05-14T23:59:59.999Z".to_string()
                    ]))
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "desc".to_string()
                ]]),
                limit: Some(10000),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.notes".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec![
                        "1".to_string(),
                        "2".to_string(),
                        "3".to_string(),
                        "4".to_string(),
                        "5".to_string()
                    ]),
                    or: None,
                    and: None
                }]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn superset_pg_time_filter_with_generalized_filters() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT DATE_TRUNC('week', \"order_date\") AS __timestamp,
               count(count) AS \"COUNT(count)\"
FROM public.\"KibanaSampleDataEcommerce\"
WHERE \"customer_gender\" = 'female'\
 AND \"order_date\" >= TO_TIMESTAMP('2021-05-15 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
 AND \"customer_gender\" LIKE '%fem%'
 AND \"customer_gender\" LIKE '%fe%'
 AND \"order_date\" < TO_TIMESTAMP('2022-05-15 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
 AND \"customer_gender\" LIKE '%f%'
GROUP BY DATE_TRUNC('week', \"order_date\")
ORDER BY \"COUNT(count)\" DESC"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("week".to_string()),
                    date_range: Some(json!(vec![
                        "2021-05-15T00:00:00.000Z".to_string(),
                        "2022-05-14T23:59:59.999Z".to_string()
                    ]))
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "desc".to_string()
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
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("contains".to_string()),
                        values: Some(vec!["fem".to_string()]),
                        or: None,
                        and: None
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("contains".to_string()),
                        values: Some(vec!["fe".to_string()]),
                        or: None,
                        and: None
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("contains".to_string()),
                        values: Some(vec!["f".to_string()]),
                        or: None,
                        and: None
                    }
                ]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn power_bi_dimension_only() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "select \"_\".\"customer_gender\"\r\nfrom \r\n(\r\n    select \"rows\".\"customer_gender\" as \"customer_gender\"\r\n    from \r\n    (\r\n        select \"customer_gender\"\r\n        from \"public\".\"KibanaSampleDataEcommerce\" \"$Table\"\r\n    ) \"rows\"\r\n    group by \"customer_gender\"\r\n) \"_\"\r\norder by \"_\".\"customer_gender\"\r\nlimit 1001".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "asc".to_string(),
                ],],),
                limit: Some(1001),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn power_bi_is_not_empty() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "select sum(\"rows\".\"count\") as \"a0\" from (select \"_\".\"count\" from \"public\".\"KibanaSampleDataEcommerce\" \"_\" where (not \"_\".\"customer_gender\" is null and not \"_\".\"customer_gender\" = '' or not (not \"_\".\"customer_gender\" is null))) \"rows\"".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: None,
                            operator: None,
                            values: None,
                            or: None,
                            and: Some(vec![
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some(
                                        "KibanaSampleDataEcommerce.customer_gender".to_string()
                                    ),
                                    operator: Some("set".to_string()),
                                    values: None,
                                    or: None,
                                    and: None,
                                }),
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some(
                                        "KibanaSampleDataEcommerce.customer_gender".to_string()
                                    ),
                                    operator: Some("notEquals".to_string()),
                                    values: Some(vec!["".to_string()]),
                                    or: None,
                                    and: None,
                                })
                            ])
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notSet".to_string()),
                            values: None,
                            or: None,
                            and: None,
                        })
                    ]),
                    and: None,
                },]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    #[cfg(debug_assertions)]
    async fn non_cube_filters_cast_kept() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT id FROM information_schema.testing_dataset WHERE id > CAST('0' AS INTEGER)"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.print(true).unwrap();
        assert!(
            logical_plan.contains("CAST"),
            "{:?} doesn't contain CAST",
            logical_plan
        );
    }

    #[tokio::test]
    async fn tableau_default_having() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:count:ok\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"\nHAVING (COUNT(1) > 0)".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        let cube_scan = logical_plan.find_cube_scan();
        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.count".to_string()),
                    operator: Some("gt".to_string()),
                    values: Some(vec!["0".to_string()]),
                    or: None,
                    and: None,
                }]),
                ..Default::default()
            }
        );

        assert_eq!(
            cube_scan
                .schema
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect::<Vec<_>>(),
            vec!["sum:count:ok".to_string(),]
        );
        assert_eq!(
            &cube_scan.member_fields,
            &vec![MemberField::regular(
                "KibanaSampleDataEcommerce.count".to_string()
            )]
        );
    }

    #[tokio::test]
    async fn tableau_group_by_month() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT COUNT(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:bytesBilled:ok\",\n  DATE_TRUNC( 'MONTH', CAST(\"KibanaSampleDataEcommerce\".\"order_date\" AS TIMESTAMP) ) AS \"tmn:timestamp:ok\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"\nGROUP BY 2".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: None,
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn tableau_group_by_month_and_dimension() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT CAST(\"KibanaSampleDataEcommerce\".\"customer_gender\" AS TEXT) AS \"query\",\n  SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:bytesBilled:ok\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"\nGROUP BY 1".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                order: Some(vec![]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn tableau_extract_year() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT CAST(TRUNC(EXTRACT(YEAR FROM \"KibanaSampleDataEcommerce\".\"order_date\")) AS INTEGER) AS \"yr:timestamp:ok\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"\nGROUP BY 1".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("year".to_string()),
                    date_range: None,
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        );

        let query_plan = convert_select_to_query_plan(
            "SELECT CAST(TRUNC(EXTRACT(YEAR FROM \"KibanaSampleDataEcommerce\".\"order_date\")) AS INTEGER) AS \"yr:timestamp:ok\", SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:teraBytesBilled:ok\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"\nGROUP BY 1".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("year".to_string()),
                    date_range: None,
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn tableau_week() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT CAST((DATE_TRUNC( 'day', CAST(\"KibanaSampleDataEcommerce\".\"order_date\" AS DATE) ) + (-EXTRACT(DOW FROM \"KibanaSampleDataEcommerce\".\"order_date\") * INTERVAL '1 DAY')) AS DATE) AS \"yr:timestamp:ok\", SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:teraBytesBilled:ok\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"\nGROUP BY 1".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("week".to_string()),
                    date_range: None,
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn tableau_filter() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:freeCount:ok\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"\nWHERE (CAST(\"KibanaSampleDataEcommerce\".\"customer_gender\" AS TEXT) = 'female')".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                order: Some(vec![]),
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

    #[tokio::test]
    async fn tableau_contains_filter() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:freeCount:ok\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"\nWHERE (STRPOS(CAST(LOWER(CAST(CAST(\"KibanaSampleDataEcommerce\".\"customer_gender\" AS TEXT) AS TEXT)) AS TEXT),CAST('fem' AS TEXT)) > 0)".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("contains".to_string()),
                    values: Some(vec!["fem".to_string()]),
                    or: None,
                    and: None,
                }]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn tableau_mul_null_by_timestamp() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT ((CAST('1900-01-01 00:00:00' AS TIMESTAMP) + NULL * INTERVAL '1 DAY') + 1 * INTERVAL '1 DAY') AS \"TEMP(Test)(4169571243)(0)\" FROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\" HAVING (COUNT(1) > 0)".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.count".to_string()),
                    operator: Some("gt".to_string()),
                    values: Some(vec!["0".to_string()]),
                    or: None,
                    and: None,
                }]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn tableau_gte_constant() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT
  (
    "KibanaSampleDataEcommerce"."order_date" >= DATE_TRUNC(
      'MONTH',
      CAST(CAST('2024-01-01' AS DATE) AS TIMESTAMP)
    )
  ) AS "Calculation_2760495522668597250"
FROM
  "public"."KibanaSampleDataEcommerce" "KibanaSampleDataEcommerce"
GROUP BY
  1
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
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                order: Some(vec![]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn measure_used_on_dimension() {
        init_testing_logger();

        let meta = get_test_tenant_ctx();
        let create_query = convert_sql_to_cube_query(
            &"SELECT MEASURE(customer_gender) FROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"".to_string(),
            meta.clone(),
            get_test_session(DatabaseProtocol::PostgreSQL, meta).await,
        ).await;

        assert_eq!(
            create_query.err().unwrap().message(),
            "Error during rewrite: Dimension 'customer_gender' was used with the aggregate function 'MEASURE()'. Please use a measure instead. Please check logs for additional information.",
        );
    }

    #[tokio::test]
    async fn powerbi_contains_filter() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "select \"rows\".\"customer_gender\" as \"customer_gender\",
\n    sum(\"rows\".\"count\") as \"a0\"\
\nfrom\
\n(\
\n    select \"_\".\"count\",\
\n        \"_\".\"customer_gender\"\
\n    from \"public\".\"KibanaSampleDataEcommerce\" \"_\"\
\n    where strpos((case\
\n        when \"_\".\"customer_gender\" is not null\
\n        then \"_\".\"customer_gender\"\
\n        else ''\
\n    end), 'fem') > 0\
\n) \"rows\"\
\ngroup by \"customer_gender\"\
\nlimit 1000001"
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
                order: Some(vec![]),
                limit: Some(1000001),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("contains".to_string()),
                    values: Some(vec!["fem".to_string()]),
                    or: None,
                    and: None,
                }]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn powerbi_inner_wrapped_dates() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "select \"_\".\"created_at_day\",\
\n    \"_\".\"a0\"\
\nfrom \
\n(\
\n    select \"rows\".\"created_at_day\" as \"created_at_day\",\
\n        sum(\"rows\".\"cnt\") as \"a0\"\
\n    from \
\n    (\
\n        select count(*) cnt,date_trunc('day', order_date) as created_at_day, date_trunc('month', order_date) as created_at_month from public.KibanaSampleDataEcommerce group by 2, 3\
\n    ) \"rows\"\
\n    group by \"created_at_day\"\
\n) \"_\"\
\nwhere not \"_\".\"a0\" is null\
\nlimit 1000001"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
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
                order: Some(vec![]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn powerbi_inner_wrapped_asterisk() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "select \"rows\".\"customer_gender\" as \"customer_gender\",\
\n    \"rows\".\"created_at_month\" as \"created_at_month\"\
\nfrom \
\n(\
\n    select \"_\".\"count\",\
\n        \"_\".\"minPrice\",\
\n        \"_\".\"maxPrice\",\
\n        \"_\".\"avgPrice\",\
\n        \"_\".\"order_date\",\
\n        \"_\".\"customer_gender\",\
\n        \"_\".\"created_at_day\",\
\n        \"_\".\"created_at_month\"\
\n    from \
\n    (\
\n        select *, date_trunc('day', order_date) created_at_day, date_trunc('month', order_date) created_at_month from public.KibanaSampleDataEcommerce\
\n    ) \"_\"\
\n    where \"_\".\"created_at_month\" < timestamp '2022-06-13 00:00:00' and \"_\".\"created_at_month\" >= timestamp '2021-12-16 00:00:00'\
\n) \"rows\"\
\ngroup by \"customer_gender\",\
\n    \"created_at_month\"\
\nlimit 1000001"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: Some(json!(vec![
                        "2021-12-16T00:00:00.000Z".to_string(),
                        "2022-06-12T23:59:59.999Z".to_string()
                    ])),
                }]),
                order: Some(vec![]),
                limit: Some(1000001),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn powerbi_sum_wrap() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"select
  "_"."dim1",
  "_"."a0",
  "_"."a1",
  "_"."a2",
  "_"."a3"
from
  (
    select
      "rows"."dim1" as "dim1",
      sum(cast("rows"."measure1" as decimal)) as "a0",
      sum(cast("rows"."measure2" as decimal)) as "a1",
      sum(
        cast("rows"."measure3" as decimal)
      ) as "a2",
      sum(cast("rows"."measure4" as decimal)) as "a3"
    from
      (
        select
          "_"."dim0",
          "_"."measure1",
          "_"."measure2",
          "_"."measure3",
          "_"."measure4",
          "_"."measure5",
          "_"."measure6",
          "_"."measure7",
          "_"."measure8",
          "_"."measure9",
          "_"."measure10",
          "_"."measure11",
          "_"."measure12",
          "_"."measure13",
          "_"."measure14",
          "_"."measure15",
          "_"."measure16",
          "_"."measure17",
          "_"."measure18",
          "_"."measure19",
          "_"."measure20",
          "_"."dim1",
          "_"."dim2",
          "_"."dim3",
          "_"."dim4",
          "_"."dim5",
          "_"."dim6",
          "_"."dim7",
          "_"."dim8",
          "_"."dim9",
          "_"."dim10",
          "_"."dim11",
          "_"."dim12",
          "_"."dim13",
          "_"."dim14",
          "_"."dim15",
          "_"."dim16",
          "_"."dim17",
          "_"."dim18",
          "_"."__user",
          "_"."__cubeJoinField"
        from
          "public"."WideCube" "_"
        where
          "_"."dim1" = 'Jewelry'
      ) "rows"
    group by
      "dim1"
  ) "_"
where
  (
    not "_"."a0" is null
    or not "_"."a1" is null
  )
  or (
    not "_"."a2" is null
    or not "_"."a3" is null
  )
limit
  1000001"#
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "WideCube.measure1".to_string(),
                    "WideCube.measure2".to_string(),
                    "WideCube.measure3".to_string(),
                    "WideCube.measure4".to_string(),
                ]),
                dimensions: Some(vec!["WideCube.dim1".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                limit: Some(1000001),
                filters: Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("WideCube.dim1".to_string()),
                        operator: Some("equals".to_string()),
                        values: Some(vec!["Jewelry".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: None,
                        operator: None,
                        values: None,
                        or: Some(vec![
                            json!(V1LoadRequestQueryFilterItem {
                                member: Some("WideCube.measure1".to_string()),
                                operator: Some("set".to_string()),
                                values: None,
                                or: None,
                                and: None,
                            }),
                            json!(V1LoadRequestQueryFilterItem {
                                member: Some("WideCube.measure2".to_string()),
                                operator: Some("set".to_string()),
                                values: None,
                                or: None,
                                and: None,
                            }),
                            json!(V1LoadRequestQueryFilterItem {
                                member: Some("WideCube.measure3".to_string()),
                                operator: Some("set".to_string()),
                                values: None,
                                or: None,
                                and: None,
                            }),
                            json!(V1LoadRequestQueryFilterItem {
                                member: Some("WideCube.measure4".to_string()),
                                operator: Some("set".to_string()),
                                values: None,
                                or: None,
                                and: None,
                            })
                        ]),
                        and: None,
                    },
                ]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn powerbi_year_month_split() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"select
  "_"."order_date_year_month" as "c11",
  "_"."a0" as "a0"
from
  (
    select
      "_"."order_date_year_month",
      "_"."a0"
    from
      (
        select
          "_"."order_date_year_month",
          "_"."a0"
        from
          (
            select
              "rows"."order_date_year_month" as "order_date_year_month",
              sum(cast("rows"."sumPrice" as decimal)) as "a0"
            from
              (
                select
                  "_"."sumPrice" as "sumPrice",
                  (
                    case
                      when left(
                        cast(
                          extract(
                            year
                            from
                              "_"."order_date"
                          ) as varchar
                        ),
                        4000
                      ) is not null then left(
                        cast(
                          extract(
                            year
                            from
                              "_"."order_date"
                          ) as varchar
                        ),
                        4000
                      )
                      else ''
                    end
                  ) || (
                    '-' || (
                      case
                        when left(
                          cast(
                            extract(
                              month
                              from
                                "_"."order_date"
                            ) as varchar
                          ),
                          4000
                        ) is not null then left(
                          cast(
                            extract(
                              month
                              from
                                "_"."order_date"
                            ) as varchar
                          ),
                          4000
                        )
                        else ''
                      end
                    )
                  ) as "order_date_year_month"
                from
                  (
                    select
                      "_"."sumPrice",
                      "_"."order_date"
                    from
                      (
                        select
                          "sumPrice",
                          "order_date"
                        from
                          "public"."KibanaSampleDataEcommerce" "$Table"
                      ) "_"
                    where
                      "_"."order_date" < timestamp '2023-10-08 00:00:00'
                      and "_"."order_date" >= timestamp '2023-07-08 00:00:00'
                  ) "_"
              ) "rows"
            group by
              "order_date_year_month"
          ) "_"
        where
          not "_"."a0" is null
      ) "_"
  ) "_"
order by
  "_"."a0" desc,
  "_"."order_date_year_month"
limit
  1001"#
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        // let physical_plan = query_plan.as_physical_plan().await.unwrap();
        // println!(
        //     "Physical plan: {}",
        //     displayable(physical_plan.as_ref()).indent()
        // );

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.sumPrice".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![
                    V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some("year".to_string()),
                        date_range: Some(json!(vec![
                            "2023-07-08T00:00:00.000Z".to_string(),
                            "2023-10-07T23:59:59.999Z".to_string()
                        ])),
                    },
                    V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some("month".to_string()),
                        date_range: Some(json!(vec![
                            "2023-07-08T00:00:00.000Z".to_string(),
                            "2023-10-07T23:59:59.999Z".to_string()
                        ])),
                    }
                ]),
                order: Some(vec![]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn powerbi_date_range_min_max() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"select
  max("rows"."order_date") as "a0",
  min("rows"."order_date") as "a1"
from
  (
    select
      "order_date"
    from
      "public"."KibanaSampleDataEcommerce" "$Table"
  ) "rows" "#
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn powerbi_date_range_min_max_ungrouped() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"select
  count(distinct("rows"."sumPrice")) + max(
    case
      when "rows"."sumPrice" is null then 1
      else 0
    end
  ) as "a0",
  min("rows"."sumPrice") as "a1",
  max("rows"."sumPrice") as "a2"
from
  (
    select
      "sumPrice"
    from
      "public"."KibanaSampleDataEcommerce" "$Table"
  ) "rows"
  "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );
    }

    //     #[tokio::test]
    //     async fn push_down_measure_filter() {
    //         if !Rewriter::sql_push_down_enabled() {
    //             return;
    //         }
    //         init_testing_logger();
    //
    //         let query_plan = convert_select_to_query_plan(
    //             r#"SELECT
    //   SUM("KibanaSampleDataEcommerce"."sumPrice") AS "TEMP(Calculation_2760495522911424520)(2472686499)(0)",
    //   SUM(
    //     (
    //       CASE
    //         WHEN (
    //           "KibanaSampleDataEcommerce"."order_date" < (TIMESTAMP '2024-01-01 00:00:00.000')
    //         ) THEN "KibanaSampleDataEcommerce"."sumPrice"
    //         ELSE NULL
    //       END
    //     )
    //   ) AS "TEMP(Calculation_2760495522922868746)(243454951)(0)"
    // FROM
    //   "public"."KibanaSampleDataEcommerce" "KibanaSampleDataEcommerce"
    // HAVING
    //   (COUNT(1) > 0)
    //   "#
    //             .to_string(),
    //             DatabaseProtocol::PostgreSQL,
    //         )
    //         .await;
    //
    //         let physical_plan = query_plan.as_physical_plan().await.unwrap();
    //         println!(
    //             "Physical plan: {}",
    //             displayable(physical_plan.as_ref()).indent()
    //         );
    //
    //         let logical_plan = query_plan.as_logical_plan();
    //         assert_eq!(
    //             logical_plan.find_cube_scan().request,
    //             V1LoadRequestQuery {
    //                 measures: Some(vec![]),
    //                 dimensions: Some(vec![]),
    //                 segments: Some(vec![]),
    //                 time_dimensions: None,
    //                 order: Some(vec![]),
    //                 limit: None,
    //                 offset: None,
    //                 filters: None,
    //                 ungrouped: Some(true),
    //             }
    //         );
    //     }

    #[tokio::test]
    async fn powerbi_inner_decimal_cast() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "select \"_\".\"customer_gender\",\r\n    \"_\".\"a0\"\r\nfrom \r\n(\r\n    select \"rows\".\"customer_gender\" as \"customer_gender\",\r\n        sum(cast(\"rows\".\"count\" as decimal)) as \"a0\"\r\n    from \"public\".\"KibanaSampleDataEcommerce\" \"rows\"\r\n    group by \"customer_gender\"\r\n) \"_\"\r\nwhere not \"_\".\"a0\" is null\r\nlimit 1000001"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                limit: Some(1000001),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.count".to_string()),
                    operator: Some("set".to_string()),
                    values: None,
                    or: None,
                    and: None,
                }]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn powerbi_cast_and_timestamp_equals_filter() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"select
  "_"."customer_gender",
  "_"."notes",
  "_"."a0"
from
  (
    select
      "rows"."customer_gender" as "customer_gender",
      "rows"."notes" as "notes",
      sum(cast("rows"."sumPrice" as decimal)) as "a0"
    from
      (
        select
          "_"."customer_gender",
          "_"."notes",
          "_"."count",
          "_"."order_date",
          "_"."maxPrice",
          "_"."minPrice",
          "_"."sumPrice",
          "_"."__user",
          "_"."__cubeJoinField"
        from
          "public"."KibanaSampleDataEcommerce" "_"
        where
          "_"."order_date" = timestamp '2024-01-01 00:00:00'
      ) "rows"
    group by
      "customer_gender",
      "notes"
  ) "_"
where
  not "_"."a0" is null
limit
  1000001"#
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.sumPrice".to_string()]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "KibanaSampleDataEcommerce.notes".to_string()
                ]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2024-01-01T00:00:00.000Z".to_string(),
                        "2024-01-01T00:00:00.000Z".to_string()
                    ])),
                }]),
                order: Some(vec![]),
                limit: Some(1000001),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.sumPrice".to_string()),
                    operator: Some("set".to_string()),
                    values: None,
                    or: None,
                    and: None,
                }]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn powerbi_push_down_aggregate() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"select
  "rows"."customer_gender" as "customer_gender"
from
  (
    select
      "_"."customer_gender",
      "_"."taxful_total_price2"
    from
      (
        select
          "_"."customer_gender" as "customer_gender",
          left(cast("_"."taxful_total_price" as varchar), 4000) as "taxful_total_price2"
        from
          (
            select
              "customer_gender",
              "taxful_total_price"
            from
              "public"."KibanaSampleDataEcommerce" "$Table"
          ) "_"
      ) "_"
  ) "rows"
group by
  "customer_gender"
limit
  1000001"#
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                limit: Some(1000001),
                ..Default::default()
            }
        );
    }

    //     #[tokio::test]
    //     async fn powerbi_push_down_aggregate_with_filter() {
    //         init_testing_logger();
    //
    //         let query_plan = convert_select_to_query_plan(
    //             r#"select
    //   "rows"."customer_gender" as "customer_gender"
    // from
    //   (
    //     select
    //       "_"."customer_gender",
    //       "_"."taxful_total_price2"
    //     from
    //       (
    //         select
    //           "_"."customer_gender" as "customer_gender",
    //           left(cast("_"."taxful_total_price" as varchar), 4000) as "taxful_total_price2"
    //         from
    //           (
    //             select
    //               "customer_gender",
    //               "taxful_total_price"
    //             from
    //               "public"."KibanaSampleDataEcommerce" "$Table"
    //           ) "_"
    //       ) "_"
    //     where
    //       "_"."taxful_total_price2" = '1'
    //   ) "rows"
    // group by
    //   "customer_gender"
    // limit
    //   1000001"#
    //                 .to_string(),
    //             DatabaseProtocol::PostgreSQL,
    //         )
    //         .await;
    //
    //         let physical_plan = query_plan.as_physical_plan().await.unwrap();
    //         println!(
    //             "Physical plan: {}",
    //             displayable(physical_plan.as_ref()).indent()
    //         );
    //
    //         let logical_plan = query_plan.as_logical_plan();
    //         assert_eq!(
    //             logical_plan.find_cube_scan().request,
    //             V1LoadRequestQuery {
    //                 measures: Some(vec!["KibanaSampleDataEcommerce.sumPrice".to_string()]),
    //                 dimensions: Some(vec![
    //                     "KibanaSampleDataEcommerce.customer_gender".to_string(),
    //                     "KibanaSampleDataEcommerce.notes".to_string()
    //                 ]),
    //                 segments: Some(vec![]),
    //                 time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
    //                     dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
    //                     granularity: None,
    //                     date_range: Some(json!(vec![
    //                         "2024-01-01T00:00:00.000Z".to_string(),
    //                         "2024-01-01T00:00:00.000Z".to_string()
    //                     ])),
    //                 }]),
    //                 order: Some(vec![]),
    //                 limit: Some(1000001),
    //                 offset: None,
    //                 filters: Some(vec![V1LoadRequestQueryFilterItem {
    //                     member: Some("KibanaSampleDataEcommerce.sumPrice".to_string()),
    //                     operator: Some("set".to_string()),
    //                     values: None,
    //                     or: None,
    //                     and: None,
    //                 }]),
    //                 ungrouped: None,
    //             }
    //         );
    //     }

    #[tokio::test]
    async fn test_select_aggregations() {
        let variants = vec![
            (
                "SELECT COUNT(*) FROM KibanaSampleDataEcommerce".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    order: Some(vec![]),
                    ..Default::default()
                },
            ),
            (
                "SELECT COUNT(*) FROM db.KibanaSampleDataEcommerce".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    order: Some(vec![]),
                    ..Default::default()
                },
            ),
            (
                "SELECT COUNT(1) FROM KibanaSampleDataEcommerce".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    order: Some(vec![]),
                    ..Default::default()
                },
            ),
            (
                "SELECT COUNT(count) FROM KibanaSampleDataEcommerce".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    order: Some(vec![]),
                    ..Default::default()
                },
            ),
            (
                "SELECT COUNT(DISTINCT agentCount) FROM Logs".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec!["Logs.agentCount".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    order: Some(vec![]),
                    ..Default::default()
                },
            ),
            (
                "SELECT COUNT(DISTINCT agentCountApprox) FROM Logs".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec!["Logs.agentCountApprox".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    order: Some(vec![]),
                    ..Default::default()
                },
            ),
            (
                "SELECT MAX(`maxPrice`) FROM KibanaSampleDataEcommerce".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.maxPrice".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    order: Some(vec![]),
                    ..Default::default()
                },
            ),
        ];

        for (input_query, expected_request) in variants.iter() {
            let logical_plan =
                convert_select_to_query_plan(input_query.clone(), DatabaseProtocol::MySQL)
                    .await
                    .as_logical_plan();

            assert_eq!(&logical_plan.find_cube_scan().request, expected_request);
        }
    }

    #[tokio::test]
    async fn test_string_measure() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan_with_meta(
            r#"
            SELECT MIN(StringCube.someString), MAX(StringCube.someString) FROM StringCube
            "#
            .to_string(),
            get_string_cube_meta(),
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["StringCube.someString".to_string(),]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_sixteen_char_trunc() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan_with_meta(
            r#"
            SELECT MIN(a.sixteen_charchar), MAX(a.sixteen_charchar_foo), MAX(a.sixteen_charchar_bar) FROM (SELECT * FROM SixteenChar) a
            "#
            .to_string(),
            get_sixteen_char_member_cube(),
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        assert_eq!(
            query_plan.as_logical_plan().find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );

        assert!(query_plan
            .as_logical_plan()
            .find_cube_scan_wrapped_sql()
            .wrapped_sql
            .sql
            .contains("sixteen_charchar_1"));

        assert!(query_plan
            .as_logical_plan()
            .find_cube_scan_wrapped_sql()
            .wrapped_sql
            .sql
            .contains("sixteen_charchar_2"));
    }

    #[tokio::test]
    async fn test_select_error() {
        let variants: &[(&str, _)] = &[
            // TODO are there any errors that we could test for?
        ];

        for (input_query, expected_error) in variants {
            let meta = get_test_tenant_ctx();
            let query = convert_sql_to_cube_query(
                input_query,
                meta.clone(),
                get_test_session(DatabaseProtocol::PostgreSQL, meta).await,
            )
            .await;

            match query {
                Ok(_) => panic!("Query ({}) should return error", input_query),
                Err(e) => assert_eq!(&e.with_meta(None), expected_error, "for {}", input_query),
            }
        }
    }

    #[tokio::test]
    async fn test_group_by_date_trunc() {
        let supported_granularities = vec![
            // all variants
            [
                "DATE_TRUNC('second', order_date)".to_string(),
                "second".to_string(),
            ],
            [
                "DATE_TRUNC('minute', order_date)".to_string(),
                "minute".to_string(),
            ],
            [
                "DATE_TRUNC('hour', order_date)".to_string(),
                "hour".to_string(),
            ],
            [
                "DATE_TRUNC('week', order_date)".to_string(),
                "week".to_string(),
            ],
            [
                "DATE_TRUNC('month', order_date)".to_string(),
                "month".to_string(),
            ],
            [
                "DATE_TRUNC('quarter', order_date)".to_string(),
                "quarter".to_string(),
            ],
            [
                "DATE_TRUNC('qtr', order_date)".to_string(),
                "quarter".to_string(),
            ],
            [
                "DATE_TRUNC('year', order_date)".to_string(),
                "year".to_string(),
            ],
            // with escaping
            [
                "DATE_TRUNC('second', `order_date`)".to_string(),
                "second".to_string(),
            ],
        ];

        for [subquery, expected_granularity] in supported_granularities.iter() {
            let logical_plan = convert_select_to_query_plan(
                format!("SELECT COUNT(*), {} AS __timestamp FROM KibanaSampleDataEcommerce GROUP BY __timestamp", subquery), DatabaseProtocol::MySQL
            ).await.as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some(expected_granularity.to_string()),
                        date_range: None,
                    }]),
                    order: Some(vec![]),
                    ..Default::default()
                }
            );

            // assert_eq!(
            //     logical_plan
            //         .find_cube_scan()
            //         .schema
            //         .fields()
            //         .iter()
            //         .map(|f| f.name().to_string())
            //         .collect::<Vec<_>>(),
            //     vec!["COUNT(UInt8(1))", "__timestamp"]
            // );

            // assert_eq!(
            //     logical_plan.find_cube_scan().member_fields,
            //     vec![
            //         "KibanaSampleDataEcommerce.count",
            //         &format!(
            //             "KibanaSampleDataEcommerce.order_date.{}",
            //             expected_granularity
            //         )
            //     ]
            // );
        }
    }

    #[tokio::test]
    async fn test_date_part_quarter_granularity() {
        let logical_plan = convert_select_to_query_plan(
            "
            SELECT CAST(TRUNC(EXTRACT(QUARTER FROM KibanaSampleDataEcommerce.order_date)) AS INTEGER)
            FROM KibanaSampleDataEcommerce
            GROUP BY 1
            ".to_string(),
            DatabaseProtocol::PostgreSQL
        ).await.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("quarter".to_string()),
                    date_range: None,
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_where_filter_daterange() {
        init_testing_logger();

        let to_check = vec![
            // Filter push down to TD (day) - Superset
            (
                "COUNT(*), DATE(order_date) AS __timestamp".to_string(),
                "order_date >= STR_TO_DATE('2021-08-31 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f') AND order_date < STR_TO_DATE('2021-09-07 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f')".to_string(),
                Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: Some(json!(vec![
                        "2021-08-31T00:00:00.000Z".to_string(),
                        "2021-09-06T23:59:59.999Z".to_string()
                    ])),
                }])
            ),
            // Filter push down to TD (day) - Superset
            (
                "COUNT(*), DATE(order_date) AS __timestamp".to_string(),
                // Now replaced with exact date
                "`KibanaSampleDataEcommerce`.`order_date` >= date(date_add(date('2021-09-30 00:00:00.000000'), INTERVAL -30 day)) AND `KibanaSampleDataEcommerce`.`order_date` < date('2021-09-07 00:00:00.000000')".to_string(),
                Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: Some(json!(vec![
                        "2021-08-31T00:00:00.000Z".to_string(),
                        "2021-09-06T23:59:59.999Z".to_string()
                    ])),
                }])
            ),
            // Column precedence vs projection alias
            (
                "COUNT(*), DATE(order_date) AS order_date".to_string(),
                // Now replaced with exact date
                "`KibanaSampleDataEcommerce`.`order_date` >= date(date_add(date('2021-09-30 00:00:00.000000'), INTERVAL -30 day)) AND `KibanaSampleDataEcommerce`.`order_date` < date('2021-09-07 00:00:00.000000')".to_string(),
                Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: Some(json!(vec![
                        "2021-08-31T00:00:00.000Z".to_string(),
                        "2021-09-06T23:59:59.999Z".to_string()
                    ])),
                }])
            ),
            // Create a new TD (dateRange filter pushdown)
            (
                "COUNT(*)".to_string(),
                "order_date >= STR_TO_DATE('2021-08-31 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f') AND order_date < STR_TO_DATE('2021-09-07 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f')".to_string(),
                Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2021-08-31T00:00:00.000Z".to_string(),
                        "2021-09-06T23:59:59.999Z".to_string()
                    ])),
                }])
            ),
            // Create a new TD (dateRange filter pushdown from right side of CompiledFilterTree::And)
            (
                "COUNT(*)".to_string(),
                "customer_gender = 'FEMALE' AND (order_date >= STR_TO_DATE('2021-08-31 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f') AND order_date < STR_TO_DATE('2021-09-07 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f'))".to_string(),
                Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2021-08-31T00:00:00.000Z".to_string(),
                        "2021-09-06T23:59:59.999Z".to_string()
                    ])),
                }])
            ),
            // similar as below but from left side
            (
                "COUNT(*)".to_string(),
                "(order_date >= STR_TO_DATE('2021-08-31 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f') AND order_date < STR_TO_DATE('2021-09-07 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f')) AND customer_gender = 'FEMALE'".to_string(),
                Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2021-08-31T00:00:00.000Z".to_string(),
                        "2021-09-06T23:59:59.999Z".to_string()
                    ])),
                }])
            ),
            // Stacked chart
            (
                "COUNT(*), customer_gender, DATE(order_date) AS __timestamp".to_string(),
                "customer_gender = 'FEMALE' AND (order_date >= STR_TO_DATE('2021-08-31 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f') AND order_date < STR_TO_DATE('2021-09-07 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f'))".to_string(),
                Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: Some(json!(vec![
                        "2021-08-31T00:00:00.000Z".to_string(),
                        "2021-09-06T23:59:59.999Z".to_string()
                    ])),
                }])
            ),
        ];

        for (sql_projection, sql_filter, expected_tdm) in to_check.iter() {
            let query = format!(
                "SELECT
                {}
                FROM KibanaSampleDataEcommerce
                WHERE {}
                {}",
                sql_projection,
                sql_filter,
                if sql_projection.contains("__timestamp")
                    && sql_projection.contains("customer_gender")
                {
                    "GROUP BY customer_gender, __timestamp"
                } else if sql_projection.contains("__timestamp") {
                    "GROUP BY __timestamp"
                } else if sql_projection.contains("order_date") {
                    "GROUP BY DATE(order_date)"
                } else {
                    ""
                }
            );
            let logical_plan = convert_select_to_query_plan(query, DatabaseProtocol::MySQL)
                .await
                .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request.time_dimensions,
                *expected_tdm
            )
        }
    }

    #[tokio::test]
    async fn test_where_filter_or() {
        let query_plan = convert_select_to_query_plan(
            "SELECT
                COUNT(*), DATE(order_date) AS __timestamp
                FROM KibanaSampleDataEcommerce
                WHERE order_date >= STR_TO_DATE('2021-08-31 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f') OR order_date < STR_TO_DATE('2021-09-07 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f')
                GROUP BY __timestamp"
            .to_string(), DatabaseProtocol::MySQL
        ).await;

        assert_eq!(
            query_plan
                .as_logical_plan()
                .find_cube_scan()
                .request
                .filters,
            Some(vec![V1LoadRequestQueryFilterItem {
                member: None,
                operator: None,
                values: None,
                or: Some(vec![
                    json!(V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                        operator: Some("afterOrOnDate".to_string()),
                        values: Some(vec!["2021-08-31T00:00:00.000Z".to_string()]),
                        or: None,
                        and: None,
                    }),
                    json!(V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                        operator: Some("beforeDate".to_string()),
                        values: Some(vec!["2021-09-07T00:00:00.000Z".to_string()]),
                        or: None,
                        and: None,
                    })
                ]),
                and: None,
            },])
        )
    }

    #[tokio::test]
    async fn test_where_filter_simple() {
        init_testing_logger();

        let to_check = vec![
            // Binary expression with Measures
            (
                "maxPrice = 5".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["5".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "maxPrice > 5".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                    operator: Some("gt".to_string()),
                    values: Some(vec!["5".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            // Binary expression with Dimensions
            (
                "customer_gender = 'FEMALE'".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["FEMALE".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "taxful_total_price > 5".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                    operator: Some("gt".to_string()),
                    values: Some(vec!["5".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "taxful_total_price >= 5".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                    operator: Some("gte".to_string()),
                    values: Some(vec!["5".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "taxful_total_price < 5".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                    operator: Some("lt".to_string()),
                    values: Some(vec!["5".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "taxful_total_price <= 5".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                    operator: Some("lte".to_string()),
                    values: Some(vec!["5".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "taxful_total_price = -1".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["-1".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "taxful_total_price <> -1".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                    operator: Some("notEquals".to_string()),
                    values: Some(vec!["-1".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            // IN
            (
                "customer_gender IN ('FEMALE', 'MALE')".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["FEMALE".to_string(), "MALE".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "customer_gender NOT IN ('FEMALE', 'MALE')".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("notEquals".to_string()),
                    values: Some(vec!["FEMALE".to_string(), "MALE".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            // NULL
            (
                "customer_gender IS NULL".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("notSet".to_string()),
                    values: None,
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "customer_gender IS NOT NULL".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("set".to_string()),
                    values: None,
                    or: None,
                    and: None,
                }]),
                None,
            ),
            // Date
            // (
            //     "order_date = '2021-08-31'".to_string(),
            //     Some(vec![V1LoadRequestQueryFilterItem {
            //         member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            //         operator: Some("equals".to_string()),
            //         values: Some(vec!["2021-08-31T00:00:00.000Z".to_string()]),
            //         or: None,
            //         and: None,
            //     }]),
            //     None,
            // ),
            // (
            //     "order_date <> '2021-08-31'".to_string(),
            //     Some(vec![V1LoadRequestQueryFilterItem {
            //         member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            //         operator: Some("notEquals".to_string()),
            //         values: Some(vec!["2021-08-31T00:00:00.000Z".to_string()]),
            //         or: None,
            //         and: None,
            //     }]),
            //     None,
            // ),
            // BETWEEN
            // (
            //     "order_date BETWEEN '2021-08-31' AND '2021-09-07'".to_string(),
            //     // This filter will be pushed to time_dimension
            //     None,
            //     Some(vec![V1LoadRequestQueryTimeDimension {
            //         dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
            //         granularity: None,
            //         date_range: Some(json!(vec![
            //             "2021-08-31T00:00:00.000Z".to_string(),
            //             "2021-09-07T00:00:00.000Z".to_string(),
            //         ])),
            //     }]),
            // ),
            // (
            //     "order_date NOT BETWEEN '2021-08-31' AND '2021-09-07'".to_string(),
            //     Some(vec![V1LoadRequestQueryFilterItem {
            //         member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            //         operator: Some("notInDateRange".to_string()),
            //         values: Some(vec![
            //             "2021-08-31T00:00:00.000Z".to_string(),
            //             "2021-09-07T00:00:00.000Z".to_string(),
            //         ]),
            //         or: None,
            //         and: None,
            //     }]),
            //     None,
            // ),
            // SIMILAR as BETWEEN but manually
            // (
            //     "order_date >= '2021-08-31' AND order_date < '2021-09-07'".to_string(),
            //     // This filter will be pushed to time_dimension
            //     None,
            //     Some(vec![V1LoadRequestQueryTimeDimension {
            //         dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
            //         granularity: None,
            //         date_range: Some(json!(vec![
            //             "2021-08-31T00:00:00.000Z".to_string(),
            //             // -1 milleseconds hack for cube.js
            //             "2021-09-06T23:59:59.999Z".to_string(),
            //         ])),
            //     }]),
            // ),
            // //  SIMILAR as BETWEEN but without -1 nanosecond because <=
            // (
            //     "order_date >= '2021-08-31' AND order_date <= '2021-09-07'".to_string(),
            //     None,
            //     Some(vec![V1LoadRequestQueryTimeDimension {
            //         dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
            //         granularity: None,
            //         date_range: Some(json!(vec![
            //             "2021-08-31T00:00:00.000Z".to_string(),
            //             // without -1 because <=
            //             "2021-09-07T00:00:00.000Z".to_string(),
            //         ])),
            //     }]),
            // ),
            // LIKE
            (
                "customer_gender LIKE 'female'".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["female".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "customer_gender LIKE 'female%'".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("startsWith".to_string()),
                    values: Some(vec!["female".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "customer_gender LIKE '%female'".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("endsWith".to_string()),
                    values: Some(vec!["female".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "customer_gender LIKE '%female%'".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("contains".to_string()),
                    values: Some(vec!["female".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "customer_gender NOT LIKE 'male'".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("notEquals".to_string()),
                    values: Some(vec!["male".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "customer_gender NOT LIKE 'male%'".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("notStartsWith".to_string()),
                    values: Some(vec!["male".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "customer_gender NOT LIKE '%male'".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("notEndsWith".to_string()),
                    values: Some(vec!["male".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "customer_gender NOT LIKE '%male%'".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("notContains".to_string()),
                    values: Some(vec!["male".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            // Segment
            (
                "is_male = true".to_string(),
                // This filter will be pushed to segments
                None,
                None,
            ),
            (
                "is_male = true AND is_female = true".to_string(),
                // This filters will be pushed to segments
                None,
                None,
            ),
        ];

        for (sql, expected_fitler, expected_time_dimensions) in to_check.iter() {
            let logical_plan = convert_select_to_query_plan(
                format!(
                    "SELECT
                COUNT(*)
                FROM KibanaSampleDataEcommerce
                WHERE {}",
                    sql
                ),
                DatabaseProtocol::MySQL,
            )
            .await
            .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request.filters,
                *expected_fitler,
                "Filters for {}",
                sql
            );
            assert_eq!(
                logical_plan.find_cube_scan().request.time_dimensions,
                *expected_time_dimensions,
                "Time dimensions for {}",
                sql
            );
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_filter_error() {
        let to_check = vec![
            // Binary expr
            (
                "order_date >= 'WRONG_DATE'".to_string(),
                CompilationError::user("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            (
                "order_date <= 'WRONG_DATE'".to_string(),
                CompilationError::user("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            (
                "order_date < 'WRONG_DATE'".to_string(),
                CompilationError::user("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            (
                "order_date <= 'WRONG_DATE'".to_string(),
                CompilationError::user("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            (
                "order_date = 'WRONG_DATE'".to_string(),
                CompilationError::user("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            (
                "order_date <> 'WRONG_DATE'".to_string(),
                CompilationError::user("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            // Between
            (
                "order_date BETWEEN 'WRONG_DATE' AND '2021-01-01'".to_string(),
                CompilationError::user("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            (
                "order_date BETWEEN '2021-01-01' AND 'WRONG_DATE'".to_string(),
                CompilationError::user("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
        ];

        for (sql, expected_error) in to_check.iter() {
            let meta = get_test_tenant_ctx();
            let query = convert_sql_to_cube_query(
                &format!(
                    "SELECT
                    COUNT(*), DATE(order_date) AS __timestamp
                    FROM KibanaSampleDataEcommerce
                    WHERE {}
                    GROUP BY __timestamp",
                    sql
                ),
                meta.clone(),
                get_test_session(DatabaseProtocol::MySQL, meta).await,
            )
            .await;

            match &query {
                Ok(_) => panic!("Query ({}) should return error", sql),
                Err(e) => assert_eq!(e, expected_error, "{}", sql),
            }
        }
    }

    #[tokio::test]
    async fn test_where_filter_complex() {
        let to_check = vec![
            (
                "customer_gender = 'FEMALE' AND customer_gender = 'MALE'".to_string(),
                vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("equals".to_string()),
                        values: Some(vec!["FEMALE".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("equals".to_string()),
                        values: Some(vec!["MALE".to_string()]),
                        or: None,
                        and: None,
                    }
                ],
            ),
            (
                "customer_gender = 'FEMALE' OR customer_gender = 'MALE'".to_string(),
                vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("equals".to_string()),
                            values: Some(vec!["FEMALE".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("equals".to_string()),
                            values: Some(vec!["MALE".to_string()]),
                            or: None,
                            and: None,
                        })
                    ]),
                    and: None,
                }],
            ),
            (
                "customer_gender = 'FEMALE' AND customer_gender = 'MALE' AND customer_gender = 'UNKNOWN'".to_string(),
                vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("equals".to_string()),
                        values: Some(vec!["FEMALE".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("equals".to_string()),
                        values: Some(vec!["MALE".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("equals".to_string()),
                        values: Some(vec!["UNKNOWN".to_string()]),
                        or: None,
                        and: None,
                    }
                ],
            ),
            (
                "customer_gender = 'FEMALE' OR customer_gender = 'MALE' OR customer_gender = 'UNKNOWN'".to_string(),
                vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("equals".to_string()),
                            values: Some(vec!["FEMALE".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("equals".to_string()),
                            values: Some(vec!["MALE".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("equals".to_string()),
                            values: Some(vec!["UNKNOWN".to_string()]),
                            or: None,
                            and: None,
                        })
                    ]),
                    and: None,
                }],
            ),
            (
                "customer_gender = 'FEMALE' OR (customer_gender = 'MALE' AND taxful_total_price > 5)".to_string(),
                vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("equals".to_string()),
                            values: Some(vec!["FEMALE".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: None,
                            operator: None,
                            values: None,
                            or: None,
                            and: Some(vec![
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                                    operator: Some("equals".to_string()),
                                    values: Some(vec!["MALE".to_string()]),
                                    or: None,
                                    and: None,
                                }),
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                                    operator: Some("gt".to_string()),
                                    values: Some(vec!["5".to_string()]),
                                    or: None,
                                    and: None,
                                })
                            ]),
                        })
                    ]),
                    and: None,
                }],
            ),
            (
                "customer_gender = 'FEMALE' OR (customer_gender = 'MALE' AND taxful_total_price > 5 AND taxful_total_price < 100)".to_string(),
                vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("equals".to_string()),
                            values: Some(vec!["FEMALE".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: None,
                            operator: None,
                            values: None,
                            or: None,
                            and: Some(vec![
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                                    operator: Some("equals".to_string()),
                                    values: Some(vec!["MALE".to_string()]),
                                    or: None,
                                    and: None,
                                }),
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                                    operator: Some("gt".to_string()),
                                    values: Some(vec!["5".to_string()]),
                                    or: None,
                                    and: None,
                                }),
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                                    operator: Some("lt".to_string()),
                                    values: Some(vec!["100".to_string()]),
                                    or: None,
                                    and: None,
                                })
                            ]),
                        })
                    ]),
                    and: None,
                }]
            ),
        ];

        for (sql, expected_fitler) in to_check.iter() {
            let logical_plan = convert_select_to_query_plan(
                format!(
                    "SELECT
                COUNT(*), DATE(order_date) AS __timestamp
                FROM KibanaSampleDataEcommerce
                WHERE {}
                GROUP BY __timestamp",
                    sql
                ),
                DatabaseProtocol::MySQL,
            )
            .await
            .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request.filters,
                Some(expected_fitler.clone())
            )
        }
    }

    #[tokio::test]
    async fn test_date_minus_date_postgres() {
        async fn check_date_minus_date(left: &str, right: &str, expected: &str) {
            let column_name = "result";
            assert_eq!(
                execute_query(
                    format!(
                        r#"SELECT (CAST('{left}' AS TIMESTAMP) -
                     CAST('{right}' AS TIMESTAMP)) AS "{column_name}""#
                    ),
                    DatabaseProtocol::PostgreSQL
                )
                .await
                .unwrap(),
                format!(
                    "+-{empty:-<width$}-+\
                    \n| {column_name:width$} |\
                    \n+-{empty:-<width$}-+\
                    \n| {expected:width$} |\
                    \n+-{empty:-<width$}-+",
                    empty = "",
                    width = expected.len().max(column_name.len())
                )
            );
        }

        // TODO: Postgres output: "28 days 00:34:56.123457" here and below.
        check_date_minus_date(
            "2021-03-02 12:34:56.123456789",
            "2021-02-02 12:00:00.000",
            "0 years 0 mons 28 days 0 hours 34 mins 56.123457 secs",
        )
        .await;

        // TODO: The formatting of this fractional seconds value is incorrect (with its negative sign).
        // Postgres output: "-28 days -00:34:56.123457"
        check_date_minus_date(
            "2021-02-02 12:00:00.000",
            "2021-03-02 12:34:56.123456789",
            "0 years 0 mons -28 days 0 hours -34 mins -56.123457 secs",
        )
        .await;

        // Postgres output: "89 days 01:34:56.123457"
        check_date_minus_date(
            "2021-05-02 13:34:56.123456789",
            "2021-02-02 12:00:00.000",
            "0 years 0 mons 89 days 1 hours 34 mins 56.123457 secs",
        )
        .await;

        // Postgres output: "-89 days -01:34:56.123457"
        check_date_minus_date(
            "2021-02-02 12:00:00.000",
            "2021-05-02 13:34:56.123456789",
            "0 years 0 mons -89 days -1 hours -34 mins -56.123457 secs",
        )
        .await;

        // Postgres output: "819 days 01:34:56.123457"
        check_date_minus_date(
            "2023-05-02 13:34:56.123456789",
            "2021-02-02 12:00:00.000",
            "0 years 0 mons 819 days 1 hours 34 mins 56.123457 secs",
        )
        .await;

        // Postgres output: "-819 days -01:34:56.123457"
        check_date_minus_date(
            "2021-02-02 12:00:00.000",
            "2023-05-02 13:34:56.123456789",
            "0 years 0 mons -819 days -1 hours -34 mins -56.123457 secs",
        )
        .await;

        // Check the result being zero, of course.
        // Postgres output: "00:00:00"
        check_date_minus_date(
            "2021-02-02 12:34:56",
            "2021-02-02 12:34:56.000",
            "0 years 0 mons 0 days 0 hours 0 mins 0.00 secs",
        )
        .await;
        check_date_minus_date(
            "2021-02-02 12:34:56.789112358",
            "2021-02-02 12:34:56.789112358",
            "0 years 0 mons 0 days 0 hours 0 mins 0.00 secs",
        )
        .await;

        // Postgres treats 60 seconds the same here.
        check_date_minus_date(
            "2001-02-04 00:00:00.000",
            "2001-02-03 23:59:60.000",
            "0 years 0 mons 0 days 0 hours 0 mins 0.00 secs",
        )
        .await;

        // Perhaps out of scope for this test, document pluralizaton of interval rendering.
        // Postgres output: "1 day 01:01:01"
        // Note the lack of pluralization.
        check_date_minus_date(
            "2000-02-29 01:01:01.000",
            "2000-02-28 00:00:00.000",
            "0 years 0 mons 1 days 1 hours 1 mins 1.00 secs",
        )
        .await;

        // Postgres output: "-1 days -01:01:01"
        check_date_minus_date(
            "2000-02-28 00:00:00.000",
            "2000-02-29 01:01:01.000",
            "0 years 0 mons -1 days -1 hours -1 mins -1.00 secs",
        )
        .await;

        // Postgres output: 00:00:00.001
        check_date_minus_date(
            "2000-02-29 00:00:00.000",
            "2000-02-28 23:59:59.999",
            "0 years 0 mons 0 days 0 hours 0 mins 0.001000 secs",
        )
        .await;

        check_date_minus_date(
            "2000-02-25 14:00:00.000",
            "2000-02-25 13:59:59.999",
            "0 years 0 mons 0 days 0 hours 0 mins 0.001000 secs",
        )
        .await;

        check_date_minus_date(
            "2000-02-25 14:00:00.000",
            "2000-02-25 13:59:59.900",
            "0 years 0 mons 0 days 0 hours 0 mins 0.100000 secs",
        )
        .await;

        check_date_minus_date(
            "2000-02-25 14:00:00.000123956",
            "2000-02-25 13:59:59.900123456",
            "0 years 0 mons 0 days 0 hours 0 mins 0.100000 secs",
        )
        .await;

        check_date_minus_date(
            "2000-02-25 14:00:00.000124956",
            "2000-02-25 13:59:59.900123456",
            "0 years 0 mons 0 days 0 hours 0 mins 0.100002 secs",
        )
        .await;

        check_date_minus_date(
            "2000-02-25 13:59:59.900123456",
            "2000-02-25 14:00:00.000123956",
            "0 years 0 mons 0 days 0 hours 0 mins -0.100000 secs",
        )
        .await;

        check_date_minus_date(
            "2000-02-25 13:59:59.900123456",
            "2000-02-25 14:00:00.000124956",
            "0 years 0 mons 0 days 0 hours 0 mins -0.100002 secs",
        )
        .await;
    }

    #[tokio::test]
    async fn test_extract_epoch_from_interval() {
        // Note that we haven't implemented any other extract fields on intervals, aside from epoch.
        async fn check_extract_epoch(timestamp: &str, expected: &str) {
            let column_name = "result";
            assert_eq!(
                execute_query(
                    format!(r#"SELECT EXTRACT(EPOCH FROM CAST('{timestamp}' AS INTERVAL)) AS "{column_name}""#),
                    DatabaseProtocol::PostgreSQL
                ).await.unwrap(),
                format!("+-{empty:-<width$}-+\
                    \n| {column_name:width$} |\
                    \n+-{empty:-<width$}-+\
                    \n| {expected:width$} |\
                    \n+-{empty:-<width$}-+",
                    empty = "",
                    width = expected.len().max(column_name.len())
                )
            );
        }

        // Postgres is rendered: "5.000000"
        check_extract_epoch("5 seconds", "5").await;
        check_extract_epoch("0 seconds", "0").await;
        check_extract_epoch("1 day", "86400").await;
        check_extract_epoch("1 day 25 seconds", "86425").await;
        for i in 0..11 {
            check_extract_epoch(&format!("{} month", i), &(86400 * 30 * i).to_string()).await;
            check_extract_epoch(
                &format!("{} month 1 day", i),
                &(86400 * (30 * i + 1)).to_string(),
            )
            .await;
            check_extract_epoch(
                &format!("{} month 32 day", i),
                &(86400 * (30 * i + 32)).to_string(),
            )
            .await;
            check_extract_epoch(
                &format!("{} months", i + 12),
                &(86400 * (365 + 30 * i) + 86400 / 4).to_string(),
            )
            .await;
            check_extract_epoch(
                &format!("{} months 32 days", i + 12),
                &(86400 * (365 + 30 * i + 32) + 86400 / 4).to_string(),
            )
            .await;
        }

        // TODO: Postgres surely does 5.123457.
        check_extract_epoch("5.123456789 seconds", "5.123456789").await;
    }

    #[tokio::test]
    async fn test_ts_minus_date_postgres() {
        async fn check_ts_minus_date(left: &str, right: &str, expected: &str) {
            let column_name = "result";
            assert_eq!(
                execute_query(
                    format!(
                        r#"SELECT (CAST('{left}' AS TIMESTAMP) -
                     CAST('{right}' AS DATE)) AS "{column_name}""#
                    ),
                    DatabaseProtocol::PostgreSQL
                )
                .await
                .unwrap(),
                format!(
                    "+-{empty:-<width$}-+\
                    \n| {column_name:width$} |\
                    \n+-{empty:-<width$}-+\
                    \n| {expected:width$} |\
                    \n+-{empty:-<width$}-+",
                    empty = "",
                    width = expected.len().max(column_name.len())
                )
            );
        }

        // Postgres output: "5 days 12:00:00"
        check_ts_minus_date(
            "2020-02-20 12:00:00.000",
            "2020-02-15",
            "0 years 0 mons 5 days 12 hours 0 mins 0.00 secs",
        )
        .await;

        // Postgres output: "-5 days -12:00:00"
        check_ts_minus_date(
            "2020-02-20 12:00:00.000",
            "2020-02-26",
            "0 years 0 mons -5 days -12 hours 0 mins 0.00 secs",
        )
        .await;

        // Postgres output: "5 days 12:34:56.123"
        check_ts_minus_date(
            "2020-02-20 12:34:56.123",
            "2020-02-15",
            "0 years 0 mons 5 days 12 hours 34 mins 56.123000 secs",
        )
        .await;

        // Postgres output: "-5 days -11:25:03.877"
        check_ts_minus_date(
            "2020-02-20 12:34:56.123",
            "2020-02-26",
            "0 years 0 mons -5 days -11 hours -25 mins -3.877000 secs",
        )
        .await;
    }

    #[tokio::test]
    async fn test_date_add_sub_postgres() {
        async fn check_fun(op: &str, t: &str, i: &str, expected: &str) {
            assert_eq!(
                execute_query(
                    format!(
                        "SELECT Str_to_date('{}', '%Y-%m-%d %H:%i:%s') {} INTERVAL '{}' as result",
                        t, op, i
                    ),
                    DatabaseProtocol::PostgreSQL
                )
                .await
                .unwrap(),
                format!(
                    "+-------------------------+\n\
                | result                  |\n\
                +-------------------------+\n\
                | {} |\n\
                +-------------------------+",
                    expected
                )
            );
        }

        async fn check_adds_to(t: &str, i: &str, expected: &str) {
            check_fun("+", t, i, expected).await
        }

        async fn check_subs_to(t: &str, i: &str, expected: &str) {
            check_fun("-", t, i, expected).await
        }

        check_adds_to("2021-01-01 00:00:00", "1 second", "2021-01-01T00:00:01.000").await;
        check_adds_to("2021-01-01 00:00:00", "1 minute", "2021-01-01T00:01:00.000").await;
        check_adds_to("2021-01-01 00:00:00", "1 hour", "2021-01-01T01:00:00.000").await;
        check_adds_to("2021-01-01 00:00:00", "1 day", "2021-01-02T00:00:00.000").await;
        check_adds_to(
            "2021-01-01 00:00:00",
            "-1 second",
            "2020-12-31T23:59:59.000",
        )
        .await;
        check_adds_to(
            "2021-01-01 00:00:00",
            "-1 minute",
            "2020-12-31T23:59:00.000",
        )
        .await;
        check_adds_to("2021-01-01 00:00:00", "-1 hour", "2020-12-31T23:00:00.000").await;
        check_adds_to("2021-01-01 00:00:00", "-1 day", "2020-12-31T00:00:00.000").await;

        check_adds_to(
            "2021-01-01 00:00:00",
            "1 day 1 hour 1 minute 1 second",
            "2021-01-02T01:01:01.000",
        )
        .await;
        check_subs_to(
            "2021-01-02 01:01:01",
            "1 day 1 hour 1 minute 1 second",
            "2021-01-01T00:00:00.000",
        )
        .await;

        check_adds_to("2021-01-01 00:00:00", "1 month", "2021-02-01T00:00:00.000").await;

        check_adds_to("2021-01-01 00:00:00", "1 year", "2022-01-01T00:00:00.000").await;
        check_subs_to("2022-01-01 00:00:00", "1 year", "2021-01-01T00:00:00.000").await;

        check_adds_to("2021-01-01 00:00:00", "13 month", "2022-02-01T00:00:00.000").await;
        check_subs_to("2022-02-01 00:00:00", "13 month", "2021-01-01T00:00:00.000").await;

        check_adds_to("2021-01-01 23:59:00", "1 minute", "2021-01-02T00:00:00.000").await;
        check_subs_to("2021-01-02 00:00:00", "1 minute", "2021-01-01T23:59:00.000").await;

        check_adds_to("2021-12-01 00:00:00", "1 month", "2022-01-01T00:00:00.000").await;
        check_subs_to("2022-01-01 00:00:00", "1 month", "2021-12-01T00:00:00.000").await;

        check_adds_to("2021-12-31 00:00:00", "1 day", "2022-01-01T00:00:00.000").await;
        check_subs_to("2022-01-01 00:00:00", "1 day", "2021-12-31T00:00:00.000").await;

        // Feb 29 on leap and non-leap years.
        check_adds_to("2020-02-29 00:00:00", "1 day", "2020-03-01T00:00:00.000").await;
        check_subs_to("2020-03-01 00:00:00", "1 day", "2020-02-29T00:00:00.000").await;

        check_adds_to("2020-02-28 00:00:00", "1 day", "2020-02-29T00:00:00.000").await;
        check_subs_to("2020-02-29 00:00:00", "1 day", "2020-02-28T00:00:00.000").await;

        check_adds_to("2021-02-28 00:00:00", "1 day", "2021-03-01T00:00:00.000").await;
        check_subs_to("2021-03-01 00:00:00", "1 day", "2021-02-28T00:00:00.000").await;

        check_adds_to("2020-02-29 00:00:00", "1 year", "2021-02-28T00:00:00.000").await;
        check_subs_to("2020-02-29 00:00:00", "1 year", "2019-02-28T00:00:00.000").await;

        check_adds_to("2020-01-30 00:00:00", "1 month", "2020-02-29T00:00:00.000").await;
        check_subs_to("2020-03-30 00:00:00", "1 month", "2020-02-29T00:00:00.000").await;

        check_adds_to("2020-01-29 00:00:00", "1 month", "2020-02-29T00:00:00.000").await;
        check_subs_to("2020-03-29 00:00:00", "1 month", "2020-02-29T00:00:00.000").await;

        check_adds_to("2021-01-29 00:00:00", "1 month", "2021-02-28T00:00:00.000").await;
        check_subs_to("2021-03-29 00:00:00", "1 month", "2021-02-28T00:00:00.000").await;
    }

    #[tokio::test]
    async fn test_information_schema_tables_mysql() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_tables_mysql",
            execute_query(
                "SELECT * FROM information_schema.tables".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_role_table_grants_pg() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_role_table_grants_postgresql",
            execute_query(
                "SELECT * FROM information_schema.role_table_grants".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_observable() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "observable_grants",
            execute_query(
                "SELECT DISTINCT privilege_type
                FROM information_schema.role_table_grants
                WHERE grantee = user
                UNION
                SELECT DISTINCT privilege_type
                FROM information_schema.role_column_grants
                WHERE grantee = user
              "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_role_column_grants_pg() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_role_column_grants_postgresql",
            execute_query(
                "SELECT * FROM information_schema.role_column_grants".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_columns_mysql() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_columns_mysql",
            execute_query(
                "SELECT * FROM information_schema.columns WHERE TABLE_SCHEMA = 'db'".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_schemata() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_schemata",
            execute_query(
                "SELECT * FROM information_schema.schemata".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_stats_for_columns() -> Result<(), CubeError> {
        // This query is used by metabase for introspection
        insta::assert_snapshot!(
            "test_information_schema_stats_for_columns",
            execute_query("
            SELECT
                A.TABLE_SCHEMA TABLE_CAT, NULL TABLE_SCHEM, A.TABLE_NAME, A.COLUMN_NAME, B.SEQ_IN_INDEX KEY_SEQ, B.INDEX_NAME PK_NAME
            FROM INFORMATION_SCHEMA.COLUMNS A, INFORMATION_SCHEMA.STATISTICS B
            WHERE A.COLUMN_KEY in ('PRI','pri') AND B.INDEX_NAME='PRIMARY'  AND (ISNULL(database()) OR (A.TABLE_SCHEMA = database())) AND (ISNULL(database()) OR (B.TABLE_SCHEMA = database())) AND A.TABLE_NAME = 'OutlierFingerprints'  AND B.TABLE_NAME = 'OutlierFingerprints'  AND A.TABLE_SCHEMA = B.TABLE_SCHEMA AND A.TABLE_NAME = B.TABLE_NAME AND A.COLUMN_NAME = B.COLUMN_NAME
            ORDER BY A.COLUMN_NAME".to_string(), DatabaseProtocol::MySQL).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_redshift_svv_tables() -> Result<(), CubeError> {
        // This query is used by metabase for introspection
        insta::assert_snapshot!(
            "redshift_svv_tables",
            execute_query(
                "SELECT * FROM svv_tables ORDER BY table_name DESC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_redshift_svv_table_info() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "redshift_svv_table_info",
            execute_query(
                "SELECT * FROM svv_table_info ORDER BY table_id ASC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_redshift_stl_ddltext() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "redshift_stl_ddltext",
            execute_query(
                "SELECT * FROM stl_ddltext ORDER BY xid ASC, sequence ASC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_redshift_stl_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "redshift_stl_query",
            execute_query(
                "SELECT * FROM stl_query ORDER BY query ASC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_redshift_stl_querytext() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "redshift_stl_querytext",
            execute_query(
                "SELECT * FROM stl_querytext ORDER BY query ASC, sequence ASC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sha1_redshift() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "sha1_redshift",
            execute_query(
                "
                SELECT
                    relname,
                    SHA1(relname) hash
                FROM pg_class
                ORDER BY oid ASC
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_monte_carlo_table_introspection() -> Result<(), CubeError> {
        // This query is used by Monte Carlo for introspection
        insta::assert_snapshot!(
            "monte_carlo_table_introspection",
            execute_query(
                r#"
                SELECT
                    "database",
                    "table",
                    "table_id",
                    "schema",
                    "size",
                    "tbl_rows",
                    "estimated_visible_rows"
                FROM svv_table_info
                WHERE (
                    "database" = 'cubedb'
                    AND "schema" = 'public'
                    AND "table" = 'KibanaSampleDataEcommerce'
                ) ORDER BY "table_id"
                "#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_monte_carlo_ddl_introspection() -> Result<(), CubeError> {
        // This query is used by Monte Carlo for introspection
        insta::assert_snapshot!(
            "monte_carlo_ddl_introspection",
            execute_query(
                r#"
                SELECT
                    SHA1(
                        pg_user.usename
                        || '-'
                        || stl_ddltext.xid
                        || '-'
                        || stl_ddltext.pid
                        || '-'
                        || stl_ddltext.starttime
                        || '-'
                        || stl_ddltext.endtime
                    ) as query,
                    stl_ddltext.sequence,
                    stl_ddltext.text,
                    pg_user.usename,
                    stl_ddltext.starttime,
                    stl_ddltext.endtime
                FROM stl_ddltext
                INNER JOIN pg_user ON stl_ddltext.userid = pg_user.usesysid
                WHERE
                    endtime >= '2022-11-15 16:18:47.814515'
                    AND endtime < '2022-11-15 16:31:47.814515'
                ORDER BY 1, 2
                "#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_monte_carlo_query_introspection() -> Result<(), CubeError> {
        // This query is used by Monte Carlo for introspection
        insta::assert_snapshot!(
            "monte_carlo_query_introspection",
            execute_query(
                r#"
                SELECT
                    stl_query.query,
                    stl_querytext.sequence,
                    stl_querytext.text,
                    stl_query.database,
                    pg_user.usename,
                    stl_query.starttime,
                    stl_query.endtime,
                    stl_query.aborted
                FROM stl_query
                INNER JOIN pg_user ON stl_query.userid = pg_user.usesysid
                INNER JOIN stl_querytext USING (query)
                WHERE
                    endtime >= '2022-11-15 16:18:47.814515'
                    AND endtime < '2022-11-15 16:31:47.814515'
                    AND stl_querytext.userid > 1
                ORDER BY 1, 2
                "#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_literal_filter_simplify() -> Result<(), CubeError> {
        if !Rewriter::sql_push_down_enabled() {
            return Ok(());
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "
                SELECT
                  \"customer_gender\"
                FROM \"KibanaSampleDataEcommerce\"
                WHERE TRUE = TRUE
                LIMIT 1000;"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                order: Some(vec![]),
                limit: Some(1000),
                ungrouped: Some(true),
                ..Default::default()
            }
        );
        assert_eq!(
            logical_plan.find_filter().is_none(),
            true,
            "Filter must be eliminated"
        );

        let query_plan = convert_select_to_query_plan(
            "
                SELECT
                  \"customer_gender\"
                FROM \"KibanaSampleDataEcommerce\"
                WHERE TRUE = TRUE AND customer_gender = 'male'
                LIMIT 1000;"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                order: Some(vec![]),
                limit: Some(1000),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["male".to_string()]),
                    or: None,
                    and: None,
                }]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_limit_push_down() -> Result<(), CubeError> {
        if !Rewriter::sql_push_down_enabled() {
            return Ok(());
        }
        // 1 level push down
        let query_plan = convert_select_to_query_plan(
            "SELECT l1.*, 1 as projection_should_exist_l1 FROM (\
                    SELECT
                      \"customer_gender\"
                    FROM \"KibanaSampleDataEcommerce\"
                    WHERE TRUE = TRUE
                ) as l1 LIMIT 1000"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                order: Some(vec![]),
                limit: Some(1000),
                ungrouped: Some(true),
                ..Default::default()
            }
        );

        // 2 levels push down
        let query_plan = convert_select_to_query_plan(
            "SELECT l2.*, 1 as projection_should_exist_l2 FROM (\
                SELECT l1.*, 1 as projection_should_exist FROM (\
                    SELECT
                    \"customer_gender\"
                    FROM \"KibanaSampleDataEcommerce\"
                    WHERE TRUE = TRUE
                ) as l1
             ) as l2 LIMIT 1000"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                order: Some(vec![]),
                limit: Some(1000),
                ungrouped: Some(true),
                ..Default::default()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_thought_spot_cte() -> Result<(), CubeError> {
        init_testing_logger();

        // CTE called qt_1 is used as ta_2, under the hood DF will use * projection
        let query_plan = convert_select_to_query_plan(
            "WITH \"qt_1\" AS (
                  SELECT
                    \"ta_1\".\"customer_gender\" \"ca_2\",
                    CASE
                      WHEN sum(\"ta_1\".\"count\") IS NOT NULL THEN sum(\"ta_1\".\"count\")
                      ELSE 0
                    END \"ca_3\"
                  FROM \"db\".\"public\".\"KibanaSampleDataEcommerce\" \"ta_1\"
                  GROUP BY \"ca_2\"
                )
                SELECT
                  \"qt_1\".\"ca_2\" \"ca_4\",
                  \"qt_1\".\"ca_3\" \"ca_5\"
                FROM \"qt_1\"
                WHERE TRUE = TRUE
                LIMIT 1000;"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                order: Some(vec![]),
                limit: Some(1000),
                ..Default::default()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_thought_spot_cte_flatten_alias() -> Result<(), CubeError> {
        if !Rewriter::sql_push_down_enabled() {
            return Ok(());
        }
        init_testing_logger();

        // CTE called qt_1 is used as ta_2, under the hood DF will use * projection
        let query_plan = convert_select_to_query_plan(
            r#"
WITH "qt_0" AS (
  SELECT
    "ta_1"."read" "ca_1",
    CASE
      WHEN sum("ta_2"."sumPrice") IS NOT NULL THEN sum("ta_2"."sumPrice")
      ELSE 0
    END "ca_2",
    ((CASE
      WHEN sum("ta_2"."sumPrice") IS NOT NULL THEN sum("ta_2"."sumPrice")
      ELSE 0
    END - CASE
      WHEN sum("ta_2"."count") IS NOT NULL THEN sum("ta_2"."count")
      ELSE 0
    END) > 500000.0) "ca_3"
  FROM "db"."public"."KibanaSampleDataEcommerce" "ta_2"
    JOIN "db"."public"."Logs" "ta_1"
      ON "ta_2"."__cubeJoinField" = "ta_1"."__cubeJoinField"
    JOIN "db"."public"."NumberCube" "ta_3"
      ON "ta_2"."__cubeJoinField" = "ta_3"."__cubeJoinField"
  GROUP BY "ca_1"
)
SELECT "ta_4"."ca_3" "ca_4"
FROM "qt_0" "ta_4"
GROUP BY "ca_4"
ORDER BY "ca_4" ASC
        "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.sumPrice".to_string(),
                    "KibanaSampleDataEcommerce.count".to_string()
                ]),
                segments: Some(vec![]),
                dimensions: Some(vec!["Logs.read".to_string()]),
                order: Some(vec![]),
                join_hints: Some(vec![
                    vec!["KibanaSampleDataEcommerce".to_string(), "Logs".to_string(),],
                    vec![
                        "KibanaSampleDataEcommerce".to_string(),
                        "NumberCube".to_string(),
                    ],
                ]),
                ..Default::default()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_thought_spot_cte_flatten_replacer() -> Result<(), CubeError> {
        init_testing_logger();

        // CTE called qt_1 is used as ta_2, under the hood DF will use * projection
        let query_plan = convert_select_to_query_plan(
            r#"
WITH "qt_0" AS (
  SELECT
    "ta_1"."read" "ca_1",
    DATE_TRUNC('month', "ta_2"."order_date") "ca_2",
    CASE
      WHEN sum("ta_2"."sumPrice") IS NOT NULL THEN sum("ta_2"."sumPrice")
      ELSE 0
    END "ca_3"
  FROM "public"."KibanaSampleDataEcommerce" "ta_2"
    JOIN "tpch5k"."public"."Logs" "ta_1"
      ON "ta_2"."__cubeJoinField" = "ta_1"."__cubeJoinField"
  GROUP BY
    "ca_1",
    "ca_2"

)
SELECT
  "ta_3"."ca_1" "ca_4",
  DATE_TRUNC('month', DATEADD(day, CAST(2 AS int), DATE '2014-01-01')) "ca_5",
  CASE
    WHEN sum(3) IS NOT NULL THEN sum(3)
    ELSE 0
  END "ca_6"
FROM "qt_0" "ta_3"
GROUP BY "ca_4"
ORDER BY
  "ca_4" ASC,
  "ca_6" ASC
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
                segments: Some(vec![]),
                dimensions: Some(vec!["Logs.read".to_string()]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("month".to_string()),
                    date_range: None,
                }]),
                order: Some(vec![]),
                join_hints: Some(vec![vec![
                    "KibanaSampleDataEcommerce".to_string(),
                    "Logs".to_string(),
                ],]),
                ..Default::default()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_thought_spot_qrt_granularity() -> Result<(), CubeError> {
        init_testing_logger();

        // CTE called qt_1 is used as ta_2, under the hood DF will use * projection
        let query_plan = convert_select_to_query_plan(
            "SELECT
            \"ta_1\".\"count\" \"ca_1\",
            DATE_TRUNC('qtr', \"ta_1\".\"order_date\") \"ca_2\"
            FROM \"db\".\"public\".\"KibanaSampleDataEcommerce\" \"ta_1\"
            GROUP BY ca_1, ca_2"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("quarter".to_string()),
                    date_range: None,
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_thought_spot_dow_granularity() -> Result<(), CubeError> {
        if !Rewriter::sql_push_down_enabled() {
            return Ok(());
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT
              (((DATEDIFF(day, DATE '1970-01-01', \"ta_1\".\"order_date\") + 3) % 7) + 1) \"ca_1\"
            FROM \"db\".\"public\".\"KibanaSampleDataEcommerce\" \"ta_1\"
            GROUP BY \"ca_1\""
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_thought_spot_doy_granularity() -> Result<(), CubeError> {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"SELECT
              (DATEDIFF(day,
                DATEADD(
                    month,
                    CAST(((EXTRACT(MONTH FROM "ta_1"."order_date") - 1) * -1) AS int),
                    CAST(CAST(((((EXTRACT(YEAR FROM "ta_1"."order_date") * 100) + EXTRACT(MONTH FROM "ta_1"."order_date")) * 100) + 1) AS varchar) AS date)),
                    "ta_1"."order_date"
                ) + 1
              ) "ca_1",
              CASE
                WHEN sum("ta_1"."count") IS NOT NULL THEN sum("ta_1"."count")
                ELSE 0
              END "ca_2"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
            GROUP BY "ca_1"
            LIMIT 5000"#
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("day".to_string()),
                    date_range: None,
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_thought_spot_yearly_granularity() -> Result<(), CubeError> {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"SELECT
              CAST(CAST(((((EXTRACT(YEAR FROM "ta_1"."order_date") * 100) + 1) * 100) + 1) AS varchar) AS date) "ca_1",
              CASE
                WHEN sum("ta_1"."count") IS NOT NULL THEN sum("ta_1"."count")
                ELSE 0
              END "ca_2"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
            GROUP BY "ca_1";"#
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("year".to_string()),
                    date_range: None,
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        );

        Ok(())
    }

    // same as test_thought_spot_cte, but with realiasing
    #[tokio::test]
    async fn test_thought_spot_cte_with_realiasing() -> Result<(), CubeError> {
        init_testing_logger();

        // CTE called qt_1 is used as ta_2, under the hood DF will use * projection
        let query_plan = convert_select_to_query_plan(
            "WITH \"qt_1\" AS (
                  SELECT
                    \"ta_1\".\"customer_gender\" \"ca_2\",
                    CASE
                      WHEN sum(\"ta_1\".\"count\") IS NOT NULL THEN sum(\"ta_1\".\"count\")
                      ELSE 0
                    END \"ca_3\"
                  FROM \"db\".\"public\".\"KibanaSampleDataEcommerce\" \"ta_1\"
                  GROUP BY \"ca_2\"
                )
                SELECT
                  \"ta_2\".\"ca_2\" \"ca_4\",
                  \"ta_2\".\"ca_3\" \"ca_5\"
                FROM \"qt_1\" \"ta_2\"
                WHERE TRUE = TRUE
                LIMIT 1000;"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                order: Some(vec![]),
                limit: Some(1000),
                ..Default::default()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_thought_spot_introspection() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "thought_spot_tables",
            execute_query(
                "SELECT * FROM (SELECT CAST(current_database() AS VARCHAR(124)) AS TABLE_CAT, table_schema AS TABLE_SCHEM, table_name AS TABLE_NAME, CAST( CASE table_type WHEN 'BASE TABLE' THEN CASE WHEN table_schema = 'pg_catalog' OR table_schema = 'information_schema' THEN 'SYSTEM TABLE' WHEN table_schema = 'pg_toast' THEN 'SYSTEM TOAST TABLE' WHEN table_schema ~ '^pg_' AND table_schema != 'pg_toast' THEN 'TEMPORARY TABLE' ELSE 'TABLE' END WHEN 'VIEW' THEN CASE WHEN table_schema = 'pg_catalog' OR table_schema = 'information_schema' THEN 'SYSTEM VIEW' WHEN table_schema = 'pg_toast' THEN NULL WHEN table_schema ~ '^pg_' AND table_schema != 'pg_toast' THEN 'TEMPORARY VIEW' ELSE 'VIEW' END WHEN 'EXTERNAL TABLE' THEN 'EXTERNAL TABLE' END AS VARCHAR(124)) AS TABLE_TYPE, REMARKS, '' as TYPE_CAT, '' as TYPE_SCHEM, '' as TYPE_NAME,  '' AS SELF_REFERENCING_COL_NAME, '' AS REF_GENERATION  FROM svv_tables) WHERE true  AND current_database() = 'cubedb' AND TABLE_TYPE IN ( 'TABLE', 'VIEW', 'EXTERNAL TABLE')  ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "thought_spot_svv_external_schemas",
            execute_query(
                "select 1 from svv_external_schemas where schemaname like 'public'".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "thought_spot_table_columns",
            execute_query(
                "SELECT * FROM ( SELECT current_database() AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname as TABLE_NAME , a.attname as COLUMN_NAME, CAST(case typname when 'text' THEN 12 when 'bit' THEN -7 when 'bool' THEN -7 when 'boolean' THEN -7 when 'varchar' THEN 12 when 'character varying' THEN 12 when 'char' THEN 1 when '\"char\"' THEN 1 when 'character' THEN 1 when 'nchar' THEN 12 when 'bpchar' THEN 1 when 'nvarchar' THEN 12 when 'date' THEN 91 when 'timestamp' THEN 93 when 'timestamp without time zone' THEN 93 when 'smallint' THEN 5 when 'int2' THEN 5 when 'integer' THEN 4 when 'int' THEN 4 when 'int4' THEN 4 when 'bigint' THEN -5 when 'int8' THEN -5 when 'decimal' THEN 3 when 'real' THEN 7 when 'float4' THEN 7 when 'double precision' THEN 8 when 'float8' THEN 8 when 'float' THEN 6 when 'numeric' THEN 2 when '_float4' THEN 2003 when 'timestamptz' THEN 2014 when 'timestamp with time zone' THEN 2014 when '_aclitem' THEN 2003 when '_text' THEN 2003 when 'bytea' THEN -2 when 'oid' THEN -5 when 'name' THEN 12 when '_int4' THEN 2003 when '_int2' THEN 2003 when 'ARRAY' THEN 2003 when 'geometry' THEN -4 when 'super' THEN -16 else 1111 END as SMALLINT) AS DATA_TYPE, t.typname as TYPE_NAME, case typname when 'int4' THEN 10 when 'bit' THEN 1 when 'bool' THEN 1 when 'varchar' THEN atttypmod -4 when 'character varying' THEN atttypmod -4 when 'char' THEN atttypmod -4 when 'character' THEN atttypmod -4 when 'nchar' THEN atttypmod -4 when 'bpchar' THEN atttypmod -4 when 'nvarchar' THEN atttypmod -4 when 'date' THEN 13 when 'timestamp' THEN 29 when 'smallint' THEN 5 when 'int2' THEN 5 when 'integer' THEN 10 when 'int' THEN 10 when 'int4' THEN 10 when 'bigint' THEN 19 when 'int8' THEN 19 when 'decimal' then (atttypmod - 4) >> 16 when 'real' THEN 8 when 'float4' THEN 8 when 'double precision' THEN 17 when 'float8' THEN 17 when 'float' THEN 17 when 'numeric' THEN (atttypmod - 4) >> 16 when '_float4' THEN 8 when 'timestamptz' THEN 35 when 'oid' THEN 10 when '_int4' THEN 10 when '_int2' THEN 5 when 'geometry' THEN NULL when 'super' THEN NULL else 2147483647 end as COLUMN_SIZE , null as BUFFER_LENGTH , case typname when 'float4' then 8 when 'float8' then 17 when 'numeric' then (atttypmod - 4) & 65535 when 'timestamp' then 6 when 'geometry' then NULL when 'super' then NULL else 0 end as DECIMAL_DIGITS, 10 AS NUM_PREC_RADIX , case a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) when 'false' then 1 when NULL then 2 else 0 end AS NULLABLE , dsc.description as REMARKS , pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS COLUMN_DEF, CAST(case typname when 'text' THEN 12 when 'bit' THEN -7 when 'bool' THEN -7 when 'boolean' THEN -7 when 'varchar' THEN 12 when 'character varying' THEN 12 when '\"char\"' THEN 1 when 'char' THEN 1 when 'character' THEN 1 when 'nchar' THEN 1 when 'bpchar' THEN 1 when 'nvarchar' THEN 12 when 'date' THEN 91 when 'timestamp' THEN 93 when 'timestamp without time zone' THEN 93 when 'smallint' THEN 5 when 'int2' THEN 5 when 'integer' THEN 4 when 'int' THEN 4 when 'int4' THEN 4 when 'bigint' THEN -5 when 'int8' THEN -5 when 'decimal' THEN 3 when 'real' THEN 7 when 'float4' THEN 7 when 'double precision' THEN 8 when 'float8' THEN 8 when 'float' THEN 6 when 'numeric' THEN 2 when '_float4' THEN 2003 when 'timestamptz' THEN 2014 when 'timestamp with time zone' THEN 2014 when '_aclitem' THEN 2003 when '_text' THEN 2003 when 'bytea' THEN -2 when 'oid' THEN -5 when 'name' THEN 12 when '_int4' THEN 2003 when '_int2' THEN 2003 when 'ARRAY' THEN 2003 when 'geometry' THEN -4 when 'super' THEN -16 else 1111 END as SMALLINT) AS SQL_DATA_TYPE, CAST(NULL AS SMALLINT) as SQL_DATETIME_SUB , case typname when 'int4' THEN 10 when 'bit' THEN 1 when 'bool' THEN 1 when 'varchar' THEN atttypmod -4 when 'character varying' THEN atttypmod -4 when 'char' THEN atttypmod -4 when 'character' THEN atttypmod -4 when 'nchar' THEN atttypmod -4 when 'bpchar' THEN atttypmod -4 when 'nvarchar' THEN atttypmod -4 when 'date' THEN 13 when 'timestamp' THEN 29 when 'smallint' THEN 5 when 'int2' THEN 5 when 'integer' THEN 10 when 'int' THEN 10 when 'int4' THEN 10 when 'bigint' THEN 19 when 'int8' THEN 19 when 'decimal' then ((atttypmod - 4) >> 16) & 65535 when 'real' THEN 8 when 'float4' THEN 8 when 'double precision' THEN 17 when 'float8' THEN 17 when 'float' THEN 17 when 'numeric' THEN ((atttypmod - 4) >> 16) & 65535 when '_float4' THEN 8 when 'timestamptz' THEN 35 when 'oid' THEN 10 when '_int4' THEN 10 when '_int2' THEN 5 when 'geometry' THEN NULL when 'super' THEN NULL else 2147483647 end as CHAR_OCTET_LENGTH , a.attnum AS ORDINAL_POSITION, case a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) when 'false' then 'YES' when NULL then '' else 'NO' end AS IS_NULLABLE, null as SCOPE_CATALOG , null as SCOPE_SCHEMA , null as SCOPE_TABLE, t.typbasetype AS SOURCE_DATA_TYPE , CASE WHEN left(pg_catalog.pg_get_expr(def.adbin, def.adrelid), 16) = 'default_identity' THEN 'YES' ELSE 'NO' END AS IS_AUTOINCREMENT, IS_AUTOINCREMENT AS IS_GENERATEDCOLUMN FROM pg_catalog.pg_namespace n  JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid) JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid) JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid) LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum) LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid) LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class') LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog') WHERE a.attnum > 0 AND NOT a.attisdropped     AND current_database() = 'cubedb' AND n.nspname LIKE 'public' AND c.relname LIKE 'KibanaSampleDataEcommerce' ORDER BY TABLE_SCHEM,c.relname,attnum )  UNION ALL SELECT current_database()::VARCHAR(128) AS TABLE_CAT, schemaname::varchar(128) AS table_schem, tablename::varchar(128) AS table_name, columnname::varchar(128) AS column_name, CAST(CASE columntype_rep WHEN 'text' THEN 12 WHEN 'bit' THEN -7 WHEN 'bool' THEN -7 WHEN 'boolean' THEN -7 WHEN 'varchar' THEN 12 WHEN 'character varying' THEN 12 WHEN 'char' THEN 1 WHEN 'character' THEN 1 WHEN 'nchar' THEN 1 WHEN 'bpchar' THEN 1 WHEN 'nvarchar' THEN 12 WHEN '\"char\"' THEN 1 WHEN 'date' THEN 91 WHEN 'timestamp' THEN 93 WHEN 'timestamp without time zone' THEN 93 WHEN 'timestamp with time zone' THEN 2014 WHEN 'smallint' THEN 5 WHEN 'int2' THEN 5 WHEN 'integer' THEN 4 WHEN 'int' THEN 4 WHEN 'int4' THEN 4 WHEN 'bigint' THEN -5 WHEN 'int8' THEN -5 WHEN 'decimal' THEN 3 WHEN 'real' THEN 7 WHEN 'float4' THEN 7 WHEN 'double precision' THEN 8 WHEN 'float8' THEN 8 WHEN 'float' THEN 6 WHEN 'numeric' THEN 2 WHEN 'timestamptz' THEN 2014 WHEN 'bytea' THEN -2 WHEN 'oid' THEN -5 WHEN 'name' THEN 12 WHEN 'ARRAY' THEN 2003 WHEN 'geometry' THEN -4 WHEN 'super' THEN -16 ELSE 1111 END AS SMALLINT) AS DATA_TYPE, COALESCE(NULL,CASE columntype WHEN 'boolean' THEN 'bool' WHEN 'character varying' THEN 'varchar' WHEN '\"char\"' THEN 'char' WHEN 'smallint' THEN 'int2' WHEN 'integer' THEN 'int4'WHEN 'bigint' THEN 'int8' WHEN 'real' THEN 'float4' WHEN 'double precision' THEN 'float8' WHEN 'timestamp without time zone' THEN 'timestamp' WHEN 'timestamp with time zone' THEN 'timestamptz' ELSE columntype END) AS TYPE_NAME,  CASE columntype_rep WHEN 'int4' THEN 10  WHEN 'bit' THEN 1    WHEN 'bool' THEN 1WHEN 'boolean' THEN 1WHEN 'varchar' THEN regexp_substr (columntype,'[0-9]+',7)::INTEGER WHEN 'character varying' THEN regexp_substr (columntype,'[0-9]+',7)::INTEGER WHEN 'char' THEN regexp_substr (columntype,'[0-9]+',4)::INTEGER WHEN 'character' THEN regexp_substr (columntype,'[0-9]+',4)::INTEGER WHEN 'nchar' THEN regexp_substr (columntype,'[0-9]+',7)::INTEGER WHEN 'bpchar' THEN regexp_substr (columntype,'[0-9]+',7)::INTEGER WHEN 'nvarchar' THEN regexp_substr (columntype,'[0-9]+',7)::INTEGER WHEN 'date' THEN 13 WHEN 'timestamp' THEN 29 WHEN 'timestamp without time zone' THEN 29 WHEN 'smallint' THEN 5 WHEN 'int2' THEN 5 WHEN 'integer' THEN 10 WHEN 'int' THEN 10 WHEN 'int4' THEN 10 WHEN 'bigint' THEN 19 WHEN 'int8' THEN 19 WHEN 'decimal' THEN regexp_substr (columntype,'[0-9]+',7)::INTEGER WHEN 'real' THEN 8 WHEN 'float4' THEN 8 WHEN 'double precision' THEN 17 WHEN 'float8' THEN 17 WHEN 'float' THEN 17WHEN 'numeric' THEN regexp_substr (columntype,'[0-9]+',7)::INTEGER WHEN '_float4' THEN 8 WHEN 'timestamptz' THEN 35 WHEN 'timestamp with time zone' THEN 35 WHEN 'oid' THEN 10 WHEN '_int4' THEN 10 WHEN '_int2' THEN 5 WHEN 'geometry' THEN NULL WHEN 'super' THEN NULL ELSE 2147483647 END AS COLUMN_SIZE, NULL AS BUFFER_LENGTH, CASE columntype WHEN 'real' THEN 8 WHEN 'float4' THEN 8 WHEN 'double precision' THEN 17 WHEN 'float8' THEN 17 WHEN 'timestamp' THEN 6 WHEN 'timestamp without time zone' THEN 6 WHEN 'geometry' THEN NULL WHEN 'super' THEN NULL ELSE 0 END AS DECIMAL_DIGITS, 10 AS NUM_PREC_RADIX, NULL AS NULLABLE,  NULL AS REMARKS,   NULL AS COLUMN_DEF, CAST(CASE columntype_rep WHEN 'text' THEN 12 WHEN 'bit' THEN -7 WHEN 'bool' THEN -7 WHEN 'boolean' THEN -7 WHEN 'varchar' THEN 12 WHEN 'character varying' THEN 12 WHEN 'char' THEN 1 WHEN 'character' THEN 1 WHEN 'nchar' THEN 12 WHEN 'bpchar' THEN 1 WHEN 'nvarchar' THEN 12 WHEN '\"char\"' THEN 1 WHEN 'date' THEN 91 WHEN 'timestamp' THEN 93 WHEN 'timestamp without time zone' THEN 93 WHEN 'timestamp with time zone' THEN 2014 WHEN 'smallint' THEN 5 WHEN 'int2' THEN 5 WHEN 'integer' THEN 4 WHEN 'int' THEN 4 WHEN 'int4' THEN 4 WHEN 'bigint' THEN -5 WHEN 'int8' THEN -5 WHEN 'decimal' THEN 3 WHEN 'real' THEN 7 WHEN 'float4' THEN 7 WHEN 'double precision' THEN 8 WHEN 'float8' THEN 8 WHEN 'float' THEN 6 WHEN 'numeric' THEN 2 WHEN 'timestamptz' THEN 2014 WHEN 'bytea' THEN -2 WHEN 'oid' THEN -5 WHEN 'name' THEN 12 WHEN 'ARRAY' THEN 2003 WHEN 'geometry' THEN -4 WHEN 'super' THEN -4 ELSE 1111 END AS SMALLINT) AS SQL_DATA_TYPE, CAST(NULL AS SMALLINT) AS SQL_DATETIME_SUB, CASE WHEN LEFT (columntype,7) = 'varchar' THEN regexp_substr (columntype,'[0-9]+',7)::INTEGER WHEN LEFT (columntype,4) = 'char' THEN regexp_substr (columntype,'[0-9]+',4)::INTEGER WHEN columntype = 'string' THEN 16383  ELSE NULL END AS CHAR_OCTET_LENGTH, columnnum AS ORDINAL_POSITION, NULL AS IS_NULLABLE,  NULL AS SCOPE_CATALOG,  NULL AS SCOPE_SCHEMA, NULL AS SCOPE_TABLE, NULL AS SOURCE_DATA_TYPE, 'NO' AS IS_AUTOINCREMENT, 'NO' as IS_GENERATEDCOLUMN FROM (select lbv_cols.schemaname, lbv_cols.tablename, lbv_cols.columnname,REGEXP_REPLACE(REGEXP_REPLACE(lbv_cols.columntype,'\\\\(.*\\\\)'),'^_.+','ARRAY') as columntype_rep,columntype, lbv_cols.columnnum from pg_get_late_binding_view_cols() lbv_cols( schemaname name, tablename name, columnname name, columntype text, columnnum int)) lbv_columns   WHERE true  AND current_database() = 'cubedb' AND schemaname LIKE 'public' AND tablename LIKE 'KibanaSampleDataEcommerce';".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "thought_spot_attributes",
            execute_query(
                "SELECT
                    current_database() AS PKTABLE_CAT,
                    pkn.nspname AS PKTABLE_SCHEM,
                    pkc.relname AS PKTABLE_NAME,
                    pka.attname AS PKCOLUMN_NAME,
                    current_database() AS FKTABLE_CAT,
                    fkn.nspname AS FKTABLE_SCHEM,
                    fkc.relname AS FKTABLE_NAME,
                    fka.attname AS FKCOLUMN_NAME,
                    pos.n AS KEY_SEQ,
                    CASE
                        con.confupdtype
                        WHEN 'c' THEN 0
                        WHEN 'n' THEN 2
                        WHEN 'd' THEN 4
                        WHEN 'r' THEN 1
                        WHEN 'p' THEN 1
                        WHEN 'a' THEN 3
                        ELSE NULL
                    END AS UPDATE_RULE,
                    CASE
                        con.confdeltype
                        WHEN 'c' THEN 0
                        WHEN 'n' THEN 2
                        WHEN 'd' THEN 4
                        WHEN 'r' THEN 1
                        WHEN 'p' THEN 1
                        WHEN 'a' THEN 3
                        ELSE NULL
                    END AS DELETE_RULE,
                    con.conname AS FK_NAME,
                    pkic.relname AS PK_NAME,
                    CASE
                        WHEN con.condeferrable
                        AND con.condeferred THEN 5
                        WHEN con.condeferrable THEN 6
                        ELSE 7
                    END AS DEFERRABILITY
                FROM
                    pg_catalog.pg_namespace pkn,
                    pg_catalog.pg_class pkc,
                    pg_catalog.pg_attribute pka,
                    pg_catalog.pg_namespace fkn,
                    pg_catalog.pg_class fkc,
                    pg_catalog.pg_attribute fka,
                    pg_catalog.pg_constraint con,
                    pg_catalog.generate_series(1, 32) pos(n),
                    pg_catalog.pg_class pkic,
                    pg_catalog.pg_depend dep
                WHERE
                    pkn.oid = pkc.relnamespace
                    AND pkc.oid = pka.attrelid
                    AND pka.attnum = con.confkey [pos.n]
                    AND con.confrelid = pkc.oid
                    AND fkn.oid = fkc.relnamespace
                    AND fkc.oid = fka.attrelid
                    AND fka.attnum = con.conkey [pos.n]
                    AND con.conrelid = fkc.oid
                    AND con.contype = 'f'
                    AND pkic.relkind = 'i'
                    AND con.oid = dep.objid
                    AND pkic.oid = dep.refobjid
                    AND dep.classid = 'pg_constraint' :: regclass :: oid
                    AND dep.refclassid = 'pg_class' :: regclass :: oid
                    AND fkn.nspname = 'public'
                    AND fkc.relname = 'KibanaSampleDataEcommerce'
                ORDER BY
                    pkn.nspname,
                    pkc.relname,
                    con.conname,
                    pos.n"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_performance_schema_variables() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "performance_schema_session_variables",
            execute_query("SELECT * FROM performance_schema.session_variables WHERE VARIABLE_NAME = 'max_allowed_packet'".to_string(), DatabaseProtocol::MySQL).await?
        );

        insta::assert_snapshot!(
            "performance_schema_global_variables",
            execute_query("SELECT * FROM performance_schema.global_variables WHERE VARIABLE_NAME = 'max_allowed_packet'".to_string(), DatabaseProtocol::MySQL).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_collations() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_collations",
            execute_query(
                "SELECT * FROM information_schema.collations".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_processlist() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_processlist",
            execute_query(
                "SELECT * FROM information_schema.processlist".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_gdata_studio() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "test_gdata_studio",
            execute_query(
                // This query I saw in Google Data Studio
                "/* mysql-connector-java-5.1.49 ( Revision: ad86f36e100e104cd926c6b81c8cab9565750116 ) */
                SELECT  \
                    @@session.auto_increment_increment AS auto_increment_increment, \
                    @@character_set_client AS character_set_client, \
                    @@character_set_connection AS character_set_connection, \
                    @@character_set_results AS character_set_results, \
                    @@character_set_server AS character_set_server, \
                    @@collation_server AS collation_server, \
                    @@collation_connection AS collation_connection, \
                    @@init_connect AS init_connect, \
                    @@interactive_timeout AS interactive_timeout, \
                    @@license AS license, \
                    @@lower_case_table_names AS lower_case_table_names, \
                    @@max_allowed_packet AS max_allowed_packet, \
                    @@net_buffer_length AS net_buffer_length, \
                    @@net_write_timeout AS net_write_timeout, \
                    @@sql_mode AS sql_mode, \
                    @@system_time_zone AS system_time_zone, \
                    @@time_zone AS time_zone, \
                    @@transaction_isolation AS transaction_isolation, \
                    @@wait_timeout AS wait_timeout
                "
                .to_string(), DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_show_variable() -> Result<(), CubeError> {
        // Postgres escaped with quotes
        insta::assert_snapshot!(
            "show_variable_quoted",
            execute_query(
                "show \"max_allowed_packet\";".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        // psqlodbc
        insta::assert_snapshot!(
            "show_max_identifier_length",
            execute_query(
                "show max_identifier_length;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_set_variable() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_set_app_show",
            execute_queries_with_flags(
                vec![
                    "set application_name = 'testing app'".to_string(),
                    "show application_name".to_string()
                ],
                DatabaseProtocol::PostgreSQL
            )
            .await?
            .0
        );

        insta::assert_snapshot!(
            "pg_set_role_show",
            execute_queries_with_flags(
                vec!["SET ROLE NONE".to_string(), "SHOW ROLE".to_string()],
                DatabaseProtocol::PostgreSQL
            )
            .await?
            .0
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_set_user() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "set_good_user",
            execute_queries_with_flags(
                vec![
                    "SET user = 'good_user'".to_string(),
                    "select current_user".to_string()
                ],
                DatabaseProtocol::PostgreSQL
            )
            .await?
            .0
        );

        insta::assert_snapshot!(
            "set_bad_user",
            execute_queries_with_flags(
                vec!["SET user = 'bad_user'".to_string()],
                DatabaseProtocol::PostgreSQL
            )
            .await
            .err()
            .unwrap()
            .to_string()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_explain() -> Result<(), CubeError> {
        // SELECT with no tables (inline eval)
        insta::assert_snapshot!(
            execute_query("EXPLAIN SELECT 1+1;".to_string(), DatabaseProtocol::MySQL).await?
        );

        insta::assert_snapshot!(
            execute_query(
                "EXPLAIN VERBOSE SELECT 1+1;".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // Execute without asserting with fixture, because metrics can change
        execute_query(
            "EXPLAIN ANALYZE SELECT 1+1;".to_string(),
            DatabaseProtocol::MySQL,
        )
        .await?;

        // SELECT with table and specific columns
        execute_query(
            "EXPLAIN SELECT count, avgPrice FROM KibanaSampleDataEcommerce;".to_string(),
            DatabaseProtocol::MySQL,
        )
        .await?;

        // EXPLAIN for Postgres
        execute_query(
            "EXPLAIN SELECT 1+1;".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_tables_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_tables_postgres",
            execute_query(
                "SELECT * FROM information_schema.tables".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_columns_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_columns_postgres",
            execute_query(
                "SELECT * FROM information_schema.columns".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_character_sets_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_character_sets_postgres",
            execute_query(
                "SELECT * FROM information_schema.character_sets".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_key_column_usage_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_key_column_usage_postgres",
            execute_query(
                "SELECT * FROM information_schema.key_column_usage".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_referential_constraints_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_referential_constraints_postgres",
            execute_query(
                "SELECT * FROM information_schema.referential_constraints".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_table_constraints_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_table_constraints_postgres",
            execute_query(
                "SELECT * FROM information_schema.table_constraints".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgtables_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgtables_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_tables".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgprepared_statements_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgprepared_statements_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_prepared_statements".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgtype_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgtype_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_type ORDER BY oid ASC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgroles_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgroles_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_roles ORDER BY oid ASC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgnamespace_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgnamespace_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_namespace".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_am_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgam_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_am".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_dynamic_regclass() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "dynamic_regclass_postgres_utf8",
            execute_query(
                "SELECT cast(r.a as regclass) FROM (
                    SELECT 'pg_class' as a
                    UNION ALL
                    SELECT NULL
                ) as r"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "dynamic_regclass_postgres_int32",
            execute_query(
                "SELECT cast(r.a as regclass) FROM (
                    SELECT CAST(83 as int) as a
                ) as r"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "dynamic_regclass_postgres_int64",
            execute_query(
                "SELECT cast(r.a as regclass) FROM (
                    SELECT 83 as a
                ) as r"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_sequence_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgsequence_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_sequence".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgrange_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgrange_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_range".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgattrdef_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgattrdef_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_attrdef".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgattribute_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgattribute_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_attribute".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgindex_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgindex_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_index".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgclass_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgclass_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_class".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgproc_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgproc_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_proc".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgdescription_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgdescription_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_description".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgconstraint_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgconstraint_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_constraint".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgdepend_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgdepend_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_depend ORDER BY refclassid ASC, refobjid ASC"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgenum_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgenum_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_enum".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgmatviews_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgmatviews_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_matviews".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgdatabase_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgdatabase_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_database ORDER BY oid ASC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgstatiousertables_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgstatiousertables_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_statio_user_tables ORDER BY relid ASC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgstat_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgstats_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_stats".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pg_stat_activity_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pg_stat_activity_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_stat_activity".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pguser_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pguser_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_user ORDER BY usesysid ASC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgextension_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgextension_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_extension".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgshdescription_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgshdescription_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_shdescription".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_constraint_column_usage_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "constraint_column_usage_postgres",
            execute_query(
                "SELECT * FROM information_schema.constraint_column_usage".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_views_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "views_postgres",
            execute_query(
                "SELECT * FROM information_schema.views".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_current_schema_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "current_schema_postgres",
            execute_query(
                "SELECT current_schema()".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_catalog_udf_search_path() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_catalog_udf_search_path",
            execute_query(
                "SELECT version() UNION ALL SELECT pg_catalog.version();".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_discard_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "discard_postgres_all",
            execute_query("DISCARD ALL;".to_string(), DatabaseProtocol::PostgreSQL).await?
        );
        insta::assert_snapshot!(
            "discard_postgres_plans",
            execute_query("DISCARD PLANS;".to_string(), DatabaseProtocol::PostgreSQL).await?
        );
        insta::assert_snapshot!(
            "discard_postgres_sequences",
            execute_query(
                "DISCARD SEQUENCES;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );
        insta::assert_snapshot!(
            "discard_postgres_temporary",
            execute_query(
                "DISCARD TEMPORARY;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );
        insta::assert_snapshot!(
            "discard_postgres_temp",
            execute_query("DISCARD TEMP;".to_string(), DatabaseProtocol::PostgreSQL).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_interval_mul() -> Result<(), CubeError> {
        let base_timestamp = "TO_TIMESTAMP('2020-01-01 00:00:00', 'yyyy-MM-dd HH24:mi:ss')";
        let units = ["year", "month", "week", "day", "hour", "minute", "second"];
        let multiplicands = ["1", "5", "-10", "1.5"];

        let selects = units
            .iter()
            .enumerate()
            .map(|(i, unit)| {
                let columns = multiplicands
                    .iter()
                    .map(|multiplicand| {
                        format!(
                            "{} + {} * interval '1 {}' AS \"i*{}\"",
                            base_timestamp, multiplicand, unit, multiplicand
                        )
                    })
                    .collect::<Vec<_>>();
                format!(
                    "SELECT {} AS id, '{}' AS unit, {}",
                    i,
                    unit,
                    columns.join(", ")
                )
            })
            .collect::<Vec<_>>();
        let query = format!("{} ORDER BY id ASC", selects.join(" UNION ALL "));
        insta::assert_snapshot!(
            "interval_mul",
            execute_query(query, DatabaseProtocol::PostgreSQL).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_interval_div() -> Result<(), CubeError> {
        let base_timestamp = "TO_TIMESTAMP('2020-01-01 00:00:00', 'yyyy-MM-dd HH24:mi:ss')";
        let units = ["year", "month", "week", "day", "hour", "minute", "second"];
        let divisors = ["1.5"];

        let selects = units
            .iter()
            .enumerate()
            .map(|(i, unit)| {
                let columns = divisors
                    .iter()
                    .map(|divisor| {
                        // Brackets around interval are necessary due to bug in sqlparser
                        // See https://github.com/sqlparser-rs/sqlparser-rs/issues/1345
                        // TODO remove brackets once fixed
                        format!(
                            "{base_timestamp} + (interval '1 {unit}') / {divisor} AS \"i/{divisor}\""
                        )
                    })
                    .collect::<Vec<_>>();
                format!(
                    "SELECT {i} AS id, '{unit}' AS unit, {}",
                    columns.join(", ")
                )
            })
            .collect::<Vec<_>>();
        let query = format!("{} ORDER BY id ASC", selects.join(" UNION ALL "));
        insta::assert_snapshot!(
            "interval_div",
            execute_query(query, DatabaseProtocol::PostgreSQL).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_interval_sum() -> Result<(), CubeError> {
        init_testing_logger();

        insta::assert_snapshot!(
            "interval_sum",
            execute_query(
                r#"
                SELECT
                    TO_TIMESTAMP('2019-01-01 00:00:00', 'yyyy-MM-dd HH24:mi:ss')
                    + INTERVAL '1 MONTH'
                    + INTERVAL '1 WEEK'
                    + INTERVAL '1 DAY'
                    AS t
                "#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_like_escape_symbol() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "like_escape_symbol",
            execute_query(
                "
                SELECT attname, test
                FROM (
                    SELECT
                        attname,
                        't%est' test
                    FROM pg_catalog.pg_attribute
                ) pga
                WHERE
                    attname LIKE 'is\\_%_ale' AND
                    test LIKE 't\\%e%'
                ORDER BY attname
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_psql_list() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "psql_list",
            execute_query(
                r#"
                SELECT
                    d.datname as "Name",
                    pg_catalog.pg_get_userbyid(d.datdba) as "Owner",
                    pg_catalog.pg_encoding_to_char(d.encoding) as "Encoding",
                    d.datcollate as "Collate",
                    d.datctype as "Ctype",
                    NULL as "ICU Locale",
                    'libc' AS "Locale Provider",
                    pg_catalog.array_to_string(d.datacl, E'\n') AS "Access privileges"
                FROM pg_catalog.pg_database d
                ORDER BY 1
                ;"#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_isnull_two_arg() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "isnull_two_arg",
            execute_query(
                r#"
                SELECT id, result
                FROM (
                    SELECT 1 id, isnull('left', 'right') result
                    UNION ALL
                    SELECT 2 id, isnull(NULL, 'right') result
                    UNION ALL
                    SELECT 3 id, isnull(NULL, NULL) result
                ) t
                ORDER BY id
                ;"#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_redshift_regexp_replace_default_replacer() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "redshift_regexp_replace_default_replacer",
            execute_query(
                "SELECT regexp_replace('Test test test', 'test')".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_df_compare_int_with_null() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "df_compare_int_with_null",
            execute_query(
                "SELECT
                    typname AS name,
                    oid,
                    typarray AS array_oid,
                    CAST(CAST(oid AS regtype) AS TEXT) AS regtype,
                    typdelim AS delimiter
                FROM
                    pg_type AS t
                WHERE
                    t.oid = to_regtype('nonexistent')
                ORDER BY
                    t.oid
                ;"
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn tableau_temporary_tables() {
        let meta = get_test_tenant_ctx();
        let create_query = convert_sql_to_cube_query(
            &"
            CREATE LOCAL TEMPORARY TABLE \"#Tableau_91262_83C81E14-EFF9-4FBD-AA5C-A9D7F5634757_2_Connect_C\" (
                \"COL\" INTEGER
            ) ON COMMIT PRESERVE ROWS
            ".to_string(),
            meta.clone(),
            get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await,
        ).await;
        match create_query {
            Err(CompilationError::Unsupported(msg, _)) => assert_eq!(msg, "Unsupported query type: CREATE LOCAL TEMPORARY TABLE \"#Tableau_91262_83C81E14-EFF9-4FBD-AA5C-A9D7F5634757_2_Connect_C\" (\"COL\" INT) ON COMMIT PRESERVE ROWS"),
            _ => panic!("CREATE TABLE should throw CompilationError::Unsupported"),
        };

        let select_into_query = convert_sql_to_cube_query(
            &"
            SELECT *
            INTO TEMPORARY TABLE \"#Tableau_91262_83C81E14-EFF9-4FBD-AA5C-A9D7F5634757_1_Connect_C\"
            FROM (SELECT 1 AS COL) AS CHECKTEMP
            LIMIT 1
            "
            .to_string(),
            meta.clone(),
            get_test_session(DatabaseProtocol::PostgreSQL, meta).await,
        )
        .await;
        assert!(select_into_query.is_ok());
    }

    // This tests asserts that our DF fork contains support for IS TRUE|FALSE
    #[tokio::test]
    async fn df_is_boolean() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "df_fork_is_boolean",
            execute_query(
                "SELECT r.v, r.v IS TRUE as is_true, r.v IS FALSE as is_false
                 FROM (SELECT true as v UNION ALL SELECT false as v) as r;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn df_cast_date32_additional_formats() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "df_fork_cast_date32_additional_formats",
            execute_query(
                "SELECT CAST('20220101' as DATE) as no_dim, CAST('2022/02/02' as DATE) as slash_dim,  CAST('2022|03|03' as DATE) as pipe_dim;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    // This tests asserts that our DF fork contains support for Coalesce
    #[tokio::test]
    async fn df_coalesce() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "df_fork_coalesce",
            execute_query(
                "SELECT COALESCE(null, 1) as t1, COALESCE(null, 1, null, 2) as t2".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    // This tests asserts that our DF fork contains support for nullif(scalar,scalar)
    #[tokio::test]
    async fn df_nullif() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "df_fork_nullif",
            execute_query(
                "SELECT nullif('test1', 'test1') as str_null, nullif('test1', 'test2') as str_first, nullif(3.0, 3.0) as float_null, nullif(3.0, 1.0) as float_first".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    // This tests asserts that our DF fork works correct with types
    #[tokio::test]
    async fn df_switch_case_coerc() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "df_fork_case_fixes",
            execute_query(
                "SELECT
                    CASE 'test' WHEN 'int4' THEN NULL ELSE 100 END as null_in_then,
                    CASE true WHEN 'false' THEN 'yes' ELSE 'no' END as bool_utf8_cast,
                    CASE true WHEN 'false' THEN 'yes' WHEN 'true' THEN true ELSE 'no' END as then_diff_types
                ".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    // This tests asserts that our DF fork contains support for >> && <<
    #[tokio::test]
    async fn df_is_bitwise_shit() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "df_fork_bitwise_shit",
            execute_query(
                "SELECT 2 << 10 as t1, 2048 >> 10 as t2;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    // This tests asserts that our DF fork contains support for escaped single quoted strings
    #[tokio::test]
    async fn df_escaped_strings() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "df_escaped_strings",
            execute_query(
                "SELECT 'test' LIKE e'%' as v1, 'payment_p2020_01' LIKE E'payment\\_p2020\\_01' as v2;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    // This tests asserts that our DF fork contains support for string-boolean coercion and cast
    #[tokio::test]
    async fn db_string_boolean_comparison() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "df_string_boolean_comparison",
            execute_query(
                "SELECT TRUE = 't' t, FALSE <> 'f' f;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_table_exists() -> Result<(), CubeError> {
        init_testing_logger();

        insta::assert_snapshot!(
            "metabase_table_exists",
            execute_query(
                r#"SELECT TRUE AS "_" FROM "public"."KibanaSampleDataEcommerce" WHERE 1 <> 1 LIMIT 0;"#
                    .to_string(),
                DatabaseProtocol::PostgreSQL,
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_substring() -> Result<(), CubeError> {
        if !Rewriter::sql_push_down_enabled() {
            return Ok(());
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT
                    \"source\".\"substring1\" AS \"substring2\",
                    \"source\".\"count\" AS \"count\"
                FROM (
                    SELECT
                        \"KibanaSampleDataEcommerce\".\"count\" AS \"count\",
                        SUBSTRING(\"KibanaSampleDataEcommerce\".\"customer_gender\" FROM 1 FOR 1234) AS \"substring1\"
                    FROM
                        \"public\".\"KibanaSampleDataEcommerce\"
                ) AS \"source\"".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_skyvia_reaggregate_date_part() -> Result<(), CubeError> {
        if !Rewriter::sql_push_down_enabled() {
            return Ok(());
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT EXTRACT(MONTH FROM t."order_date") AS expr1
            FROM public."KibanaSampleDataEcommerce" AS t
            ORDER BY expr1
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
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("month".to_string()),
                    date_range: None,
                }]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_doy() -> Result<(), CubeError> {
        let query_plan = convert_select_to_query_plan(
            "SELECT
                \"source\".\"order_date\" AS \"order_date\",
                \"source\".\"count\" AS \"count\"
            FROM
                (
                    SELECT
                        (
                            CAST(
                                extract(
                                    doy
                                    from
                                        \"public\".\"KibanaSampleDataEcommerce\".\"order_date\"
                                ) AS integer
                            )
                        ) AS \"order_date\",
                        count(*) AS \"count\"
                    FROM
                        \"public\".\"KibanaSampleDataEcommerce\"
                    GROUP BY CAST(
                        extract(
                            doy
                            from
                                \"public\".\"KibanaSampleDataEcommerce\".\"order_date\"
                        ) AS integer
                    )
                    ORDER BY CAST(
                        extract(
                            doy
                            from
                                \"public\".\"KibanaSampleDataEcommerce\".\"order_date\"
                        ) AS integer
                    ) ASC
                ) \"source\"
            WHERE
                \"source\".\"count\" IS NOT NULL
            ORDER BY
                \"source\".\"count\" ASC
            LIMIT
                100"
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("day".to_string()),
                    date_range: None,
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_binary_expr_projection_split() -> Result<(), CubeError> {
        if !Rewriter::sql_push_down_enabled() {
            return Ok(());
        }
        let operators = ["+", "-", "*", "/"];

        for operator in operators {
            let query_plan = convert_select_to_query_plan(
                format!("SELECT
                    (
                        CAST(
                             \"public\".\"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS integer
                        ) {} 100
                    ) AS \"taxful_total_price\"
                FROM
                    \"public\".\"KibanaSampleDataEcommerce\"", operator),
                DatabaseProtocol::PostgreSQL,
            )
                .await;

            let logical_plan = query_plan.as_logical_plan();
            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                    ]),
                    order: Some(vec![]),
                    ungrouped: Some(true),
                    ..Default::default()
                }
            );
        }

        Ok(())
    }

    // Tests that incoming query with 'qtr' (or another synonym) that is not reachable
    // by any rewrites in egraph will be executable anyway
    // TODO implement and test more complex queries, like dynamic granularity
    #[tokio::test]
    async fn test_nonrewritable_date_trunc() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let context = TestContext::new(DatabaseProtocol::PostgreSQL).await;

        // language=PostgreSQL
        let query = r#"
            WITH count_by_month AS (
                SELECT
                    DATE_TRUNC('month', dim_date0) month0,
                    COUNT(*) month_count
                FROM MultiTypeCube
                GROUP BY month0
            )
            SELECT
                DATE_TRUNC('qtr', count_by_month.month0) quarter0,
                MIN(month_count) min_month_count
            FROM count_by_month
            GROUP BY quarter0
            ORDER BY quarter0 ASC
        "#;

        let expected_cube_scan = V1LoadRequestQuery {
            measures: Some(vec!["MultiTypeCube.count".to_string()]),
            segments: Some(vec![]),
            dimensions: Some(vec![]),
            time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                dimension: "MultiTypeCube.dim_date0".to_owned(),
                granularity: Some("month".to_string()),
                date_range: None,
            }]),
            order: Some(vec![]),
            ..Default::default()
        };

        context
            .add_cube_load_mock(
                expected_cube_scan.clone(),
                simple_load_response(vec![
                    json!({
                        "MultiTypeCube.dim_date0.month": "2024-01-01T00:00:00",
                        "MultiTypeCube.count": "3",
                    }),
                    json!({
                        "MultiTypeCube.dim_date0.month": "2024-02-01T00:00:00",
                        "MultiTypeCube.count": "2",
                    }),
                    json!({
                        "MultiTypeCube.dim_date0.month": "2024-03-01T00:00:00",
                        "MultiTypeCube.count": "1",
                    }),
                    json!({
                        "MultiTypeCube.dim_date0.month": "2024-04-01T00:00:00",
                        "MultiTypeCube.count": "10",
                    }),
                ]),
            )
            .await;

        assert_eq!(
            context
                .convert_sql_to_cube_query(&query)
                .await
                .unwrap()
                .as_logical_plan()
                .find_cube_scan()
                .request,
            expected_cube_scan
        );

        // Expect that query is executable, and properly groups months by quarter
        insta::assert_snapshot!(context.execute_query(query).await.unwrap());
    }

    #[tokio::test]
    async fn test_metabase_dow() -> Result<(), CubeError> {
        let query_plan = convert_select_to_query_plan(
            "SELECT
                \"source\".\"order_date\" AS \"order_date\",
                \"source\".\"count\" AS \"count\"
            FROM
                (
                    SELECT
                        (
                            CAST(
                                extract(
                                    dow
                                    from
                                        \"public\".\"KibanaSampleDataEcommerce\".\"order_date\"
                                ) AS integer
                            ) + 1
                        ) AS \"order_date\",
                        count(*) AS \"count\"
                    FROM
                        \"public\".\"KibanaSampleDataEcommerce\"
                    GROUP BY (
                        CAST(
                            extract(
                                dow
                                from
                                    \"public\".\"KibanaSampleDataEcommerce\".\"order_date\"
                            ) AS integer
                        ) + 1
                    )
                    ORDER BY (
                        CAST(
                            extract(
                                dow
                                from
                                    \"public\".\"KibanaSampleDataEcommerce\".\"order_date\"
                            ) AS integer
                        ) + 1
                    ) ASC
                ) \"source\"
            WHERE
                \"source\".\"count\" IS NOT NULL
            ORDER BY
                \"source\".\"count\" ASC
            LIMIT
                100"
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("day".to_string()),
                    date_range: None,
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_subquery_with_same_name_excel() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "subquery_with_same_name_excel",
            execute_query(
                "SELECT oid, (SELECT oid FROM pg_type WHERE typname like 'geography') as dd FROM pg_type WHERE typname like 'geometry'".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_join_where_and_or() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "join_where_and_or",
            execute_query(
                "
                SELECT
                    att.attname,
                    att.attnum,
                    cl.oid
                FROM pg_attribute att
                JOIN pg_class cl ON
                    cl.oid = attrelid AND (
                        cl.relkind = 's' OR
                        cl.relkind = 'r'
                    )
                ORDER BY
                    cl.oid ASC,
                    att.attnum ASC
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_pg_type_any_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "metabase_pg_type_any",
            execute_query(
                "SELECT n.nspname = ANY(current_schemas(true)), n.nspname, t.typname
                FROM pg_catalog.pg_type t
                JOIN pg_catalog.pg_namespace n
                ON t.typnamespace = n.oid WHERE t.oid = 25;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_regproc_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "metabase_regproc_query",
            execute_query(
                "SELECT typinput='array_in'::regproc as is_array, typtype, typname, pg_type.oid
                FROM pg_catalog.pg_type
                LEFT JOIN (
                    select
                        ns.oid as nspoid,
                        ns.nspname,
                        r.r
                    from pg_namespace as ns
                    join (
                        select
                            s.r,
                            (current_schemas(false))[s.r] as nspname
                        from generate_series(1, array_upper(current_schemas(false), 1)) as s(r)
                    ) as r
                    using ( nspname )
                ) as sp
                ON sp.nspoid = typnamespace
                /* I've changed oid = to oid IN to verify is_array column */
                WHERE pg_type.oid IN (25, 1016)
                ORDER BY sp.r, pg_type.oid DESC;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_pg_namespace_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "metabase_pg_namespace",
            execute_query(
                "SELECT nspname AS TABLE_SCHEM, NULL AS TABLE_CATALOG
                FROM pg_catalog.pg_namespace
                WHERE nspname <> 'pg_toast'
                AND (nspname !~ '^pg_temp_'  OR nspname = (pg_catalog.current_schemas(true))[1])
                AND (nspname !~ '^pg_toast_temp_'  OR nspname = replace((pg_catalog.current_schemas(true))[1], 'pg_temp_', 'pg_toast_temp_'))
                ORDER BY TABLE_SCHEM;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_pg_class_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "metabase_pg_class_query",
            execute_query(
                "
                SELECT *
                    FROM (
                        SELECT  n.nspname,
                                c.relname,
                                a.attname,
                                a.atttypid,
                                a.attnotnull or (t.typtype = 'd' AND t.typnotnull) AS attnotnull,
                                a.atttypmod,
                                a.attlen,
                                t.typtypmod,
                                row_number() OVER (partition BY a.attrelid ORDER BY a.attnum) AS attnum,
                                NULLIF(a.attidentity, '') AS attidentity,
                                pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,
                                dsc.description,
                                t.typbasetype,
                                t.typtype
                            FROM pg_catalog.pg_namespace n
                            JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)
                            JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid)
                            JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)
                            LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum)
                            LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)
                            LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class')
                            LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog')
                        WHERE c.relkind IN ('r', 'p', 'v', 'f', 'm') AND a.attnum > 0 AND NOT a.attisdropped AND n.nspname LIKE 'public' AND c.relname LIKE 'KibanaSampleDataEcommerce') c
                WHERE true
                ORDER BY nspname, c.relname, attnum;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_thoughtspot_dateadd_literal_date32() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "thoughtspot_dateadd_literal_date32",
            execute_query(
                "
                SELECT
                    DATE_TRUNC('month', DATEADD(day, CAST(50 AS int), DATE '2014-01-01')) \"ca_1\",
                    CASE
                        WHEN sum(3) IS NOT NULL THEN sum(3)
                        ELSE 0
                    END \"ca_2\"
                ORDER BY \"ca_2\" ASC
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_quicksight_to_timestamp_format() -> Result<(), CubeError> {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT
                date_trunc('day', "order_date") AS "uuid.order_date_tg",
                COUNT(*) AS "count"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE
                "order_date" >= date_trunc('second', TO_TIMESTAMP('2019-01-01 00:00:00', 'yyyy-MM-dd HH24:mi:ss')) AND
                "order_date" < date_trunc('second', TO_TIMESTAMP('2020-01-01 00:00:00', 'yyyy-MM-dd HH24:mi:ss'))
            GROUP BY date_trunc('day', "order_date")
            ORDER BY date_trunc('day', "order_date") DESC NULLS LAST
            LIMIT 2500
            ;"#.to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: Some(json!(vec![
                        "2019-01-01T00:00:00.000Z".to_string(),
                        "2019-12-31T23:59:59.999Z".to_string()
                    ])),
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "desc".to_string()
                ]]),
                limit: Some(2500),
                ..Default::default()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_quicksight_dense_rank() -> Result<(), CubeError> {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT "faabeaae-5980-4f8f-a5ba-12f56f191f1e.order_date", "isotherrow_1", "faabeaae-5980-4f8f-a5ba-12f56f191f1e.avgPrice_avg", "$otherbucket_group_count", "count"
            FROM (
            SELECT "$f4" AS "faabeaae-5980-4f8f-a5ba-12f56f191f1e.order_date", "$f5", "$f6" AS "isotherrow_1", SUM("$weighted_avg_unit_4") AS "faabeaae-5980-4f8f-a5ba-12f56f191f1e.avgPrice_avg", COUNT(*) AS "$otherbucket_group_count", SUM("count") AS "count"
            FROM (
            SELECT "count", CASE WHEN "$RANK_1" > 2500 THEN NULL ELSE "faabeaae-5980-4f8f-a5ba-12f56f191f1e.order_date" END AS "$f4", CASE WHEN "$RANK_1" > 2500 THEN NULL ELSE "$RANK_1" END AS "$f5", CASE WHEN "$RANK_1" > 2500 THEN 1 ELSE 0 END AS "$f6", CAST("$weighted_avg_count_3" AS FLOAT) / NULLIF(CAST(SUM("$weighted_avg_count_3") OVER (PARTITION BY CASE WHEN "$RANK_1" > 2500 THEN NULL ELSE "faabeaae-5980-4f8f-a5ba-12f56f191f1e.order_date" END, CASE WHEN "$RANK_1" > 2500 THEN NULL ELSE "$RANK_1" END, CASE WHEN "$RANK_1" > 2500 THEN 1 ELSE 0 END) AS FLOAT), 0) * "faabeaae-5980-4f8f-a5ba-12f56f191f1e.avgPrice_avg" AS "$weighted_avg_unit_4"
            FROM (
            SELECT "order_date" AS "faabeaae-5980-4f8f-a5ba-12f56f191f1e.order_date", COUNT(*) AS "count", AVG("avgPrice") AS "faabeaae-5980-4f8f-a5ba-12f56f191f1e.avgPrice_avg", DENSE_RANK() OVER (ORDER BY AVG("avgPrice") DESC NULLS LAST, "order_date" NULLS FIRST) AS "$RANK_1", COUNT("avgPrice") AS "$weighted_avg_count_3"
            FROM "public"."KibanaSampleDataEcommerce"
            GROUP BY "order_date"
            ) AS "t"
            ) AS "t0"
            GROUP BY "$f4", "$f5", "$f6"
            ORDER BY "$f5" NULLS FIRST
            ) AS "t1"
            ;"#.to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "KibanaSampleDataEcommerce.avgPrice".to_string()
                ]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                order: Some(vec![]),
                ..Default::default()
            }
        );

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!("Physical plan: {:?}", physical_plan);

        Ok(())
    }

    #[tokio::test]
    async fn test_localtimestamp() -> Result<(), CubeError> {
        // TODO: the value will be different with the introduction of TZ support
        insta::assert_snapshot!(
            "localtimestamp",
            execute_query(
                "SELECT localtimestamp = current_timestamp".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_current_date() -> Result<(), CubeError> {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT CURRENT_DATE AS \"COL\"".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = &query_plan.print(true).unwrap();

        let re = Regex::new(r#"Date32\("\d+"\)"#).unwrap();
        let logical_plan = re
            .replace_all(logical_plan, "Date32(\"0\")")
            .as_ref()
            .to_string();

        assert_eq!(
            logical_plan,
            "Projection: currentdate() AS COL\
            \n  EmptyRelation",
        );

        insta::assert_snapshot!(
            "current_date",
            execute_query(
                "SELECT current_timestamp::date = current_date".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_union_ctes() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "union_ctes",
            execute_query(
                "
                WITH w AS (SELECT 1 l)
                SELECT w.l
                FROM w
                UNION ALL (SELECT w.l FROM w)
                ;"
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cast_decimal_default_precision() -> Result<(), CubeError> {
        if !Rewriter::sql_push_down_enabled() {
            return Ok(());
        }
        insta::assert_snapshot!(
            "cast_decimal_default_precision",
            execute_query(
                "
                SELECT \"rows\".b as \"plan\", count(1) as \"a0\"
                FROM (SELECT * FROM (select 1 \"teamSize\", 2 b UNION ALL select 1011 \"teamSize\", 3 b) \"_\"
                WHERE ((CAST(\"_\".\"teamSize\" as DECIMAL) = CAST(1011 as DECIMAL)))) \"rows\"
                GROUP BY \"plan\";
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        let query_plan = convert_select_to_query_plan(
            "SELECT count FROM KibanaSampleDataEcommerce WHERE (CAST(maxPrice AS Decimal) = CAST(100 AS Decimal));"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["100".to_string()]),
                    or: None,
                    and: None,
                }]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_decimal128_literal_push_down() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        async fn check_fn(cast_expr: &str, expected_search_expr: &str) {
            // Has some fluff to induce query push down.
            let logical_plan = convert_select_to_query_plan(
                format!("SELECT COUNT(*) + {} as cnt FROM KibanaSampleDataEcommerce WHERE LOWER(customer_gender) IN ('female')", cast_expr),
                DatabaseProtocol::PostgreSQL
            ).await.as_logical_plan();

            let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
            assert!(
                sql.contains(expected_search_expr),
                "cast_expr is {}, expected_search_expr is {}",
                cast_expr,
                expected_search_expr
            );
        }

        check_fn("CAST(2.0 AS NUMERIC)", "CAST('2' AS DECIMAL(38,10))").await;
        check_fn("CAST(2.73 AS NUMERIC)", "CAST('2.73' AS DECIMAL(38,10))").await;
        check_fn(
            "CAST(2.73 AS NUMERIC(5, 2))",
            "CAST('2.73' AS DECIMAL(5,2))",
        )
        .await;
        check_fn(
            "CAST(-2.73 AS NUMERIC(5, 2))",
            "CAST('-2.73' AS DECIMAL(5,2))",
        )
        .await;
        check_fn("CAST(0 AS NUMERIC(5, 2))", "CAST('0' AS DECIMAL(5,2))").await;
        check_fn("CAST(0 AS NUMERIC(2, 2))", "CAST('0' AS DECIMAL(2,2))").await;
        check_fn(
            "CAST(0.340 AS NUMERIC(2, 2))",
            "CAST('0.34' AS DECIMAL(2,2))",
        )
        .await;
        check_fn(
            "CAST(0.342 AS NUMERIC(2, 2))",
            "CAST('0.34' AS DECIMAL(2,2))",
        )
        .await;
        // TODO: Make these tests pass -- they aren't problems with literal generation, they're
        // before that.
        // check_fn("CAST(0.345 AS NUMERIC(2, 2))", "CAST('0.35' AS DECIMAL(2,2))").await;
        // check_fn("CAST(-0.345 AS NUMERIC(5, 2))", "CAST('-0.35' AS DECIMAL(5,2))").await;
    }

    #[tokio::test]
    async fn test_triple_ident() -> Result<(), CubeError> {
        if !Rewriter::sql_push_down_enabled() {
            return Ok(());
        }
        let query_plan = convert_select_to_query_plan(
            "select count
            from \"public\".\"KibanaSampleDataEcommerce\"
            where (\"public\".\"KibanaSampleDataEcommerce\".\"maxPrice\" > 100 and \"public\".\"KibanaSampleDataEcommerce\".\"maxPrice\" < 150);
            ".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                        operator: Some("gt".to_string()),
                        values: Some(vec!["100".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                        operator: Some("lt".to_string()),
                        values: Some(vec!["150".to_string()]),
                        or: None,
                        and: None,
                    }
                ]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn metabase_interval_date_range_filter() {
        let logical_plan = convert_select_to_query_plan(
            "
            SELECT COUNT(*)
            FROM KibanaSampleDataEcommerce
            WHERE KibanaSampleDataEcommerce.order_date >= CAST((CAST(now() AS timestamp) + (INTERVAL '-30 day')) AS date);
            ".to_string(),
            DatabaseProtocol::PostgreSQL
        ).await.as_logical_plan();

        let filters = logical_plan
            .find_cube_scan()
            .request
            .filters
            .unwrap_or_default();
        let filter_vals = if filters.len() > 0 {
            filters[0].values.clone()
        } else {
            None
        };

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                    operator: Some("afterOrOnDate".to_string()),
                    values: filter_vals,
                    or: None,
                    and: None,
                },]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn superset_timeout_reached() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            "
            SELECT \"KibanaSampleDataEcommerce\".\"count\" AS \"count\",\
             \"KibanaSampleDataEcommerce\".\"order_date\" AS \"order_date\", \
             \"KibanaSampleDataEcommerce\".\"is_male\" AS \"is_male\",\
             \"KibanaSampleDataEcommerce\".\"is_female\" AS \"is_female\",\
             \"KibanaSampleDataEcommerce\".\"maxPrice\" AS \"maxPrice\",\
             \"KibanaSampleDataEcommerce\".\"minPrice\" AS \"minPrice\",\
             \"KibanaSampleDataEcommerce\".\"avgPrice\" AS \"avgPrice\"\
             FROM public.\"KibanaSampleDataEcommerce\" WHERE \"order_date\" >= str_to_date('2021-06-30 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US') AND \"order_date\" < str_to_date('2022-06-30 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US') AND \"is_male\" = true ORDER BY \"order_date\" DESC LIMIT 10000
            ".to_string(),
            DatabaseProtocol::PostgreSQL
        ).await.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.minPrice".to_string(),
                    "KibanaSampleDataEcommerce.avgPrice".to_string(),
                ]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                segments: Some(vec!["KibanaSampleDataEcommerce.is_male".to_string()]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2021-06-30T00:00:00.000Z".to_string(),
                        "2022-06-29T23:59:59.999Z".to_string()
                    ]))
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "desc".to_string(),
                ]]),
                limit: Some(10000),
                ungrouped: Some(true),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn superset_ilike() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            "SELECT customer_gender AS customer_gender FROM public.\"KibanaSampleDataEcommerce\" WHERE customer_gender ILIKE '%fem%' GROUP BY customer_gender LIMIT 1000".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                limit: Some(1000),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("contains".to_string()),
                    values: Some(vec!["fem".to_string()]),
                    or: None,
                    and: None,
                }]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn metabase_limit_0() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            "SELECT true AS \"_\" FROM \"public\".\"KibanaSampleDataEcommerce\" WHERE 1 <> 1 LIMIT 0".to_string(),
            DatabaseProtocol::PostgreSQL
        ).await.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                limit: Some(0),
                ungrouped: Some(true),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn metabase_max_sum_wrapper() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
SELECT
  "source"."dim2" AS "dim2",
  "source"."dim3" AS "dim3",
  "source"."dim4" AS "dim4",
  "source"."pivot-grouping" AS "pivot-grouping",
  MAX("source"."measure1") AS "max",
  MAX("source"."measure2") AS "max_2",
  SUM("source"."measure3") AS "sum",
  MAX("source"."measure4") AS "max_3"
FROM
  (
    SELECT
      "public"."WideCube"."measure1" AS "measure1",
      "public"."WideCube"."measure2" AS "measure2",
      "public"."WideCube"."measure3" AS "measure3",
      "public"."WideCube"."measure4" AS "measure4",
      "public"."WideCube"."dim1" AS "dim1",
      "public"."WideCube"."dim2" AS "dim2",
      "public"."WideCube"."dim3" AS "dim3",
      "public"."WideCube"."dim4" AS "dim4",
      ABS(0) AS "pivot-grouping"
    FROM
      "public"."WideCube"
    WHERE
      "public"."WideCube"."dim1" = 'foo'
  ) AS "source"
GROUP BY
  "source"."dim2",
  "source"."dim3",
  "source"."dim4",
  "source"."pivot-grouping"
ORDER BY
  "source"."dim2" ASC,
  "source"."dim3" ASC,
  "source"."dim4" ASC,
  "source"."pivot-grouping" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        fn trivial_member_expr(cube: &str, member: &str, alias: &str) -> String {
            json!({
                "cubeName": cube,
                "alias": alias,
                "expr": {
                    "type": "SqlFunction",
                    "cubeParams": [cube],
                    "sql": format!("${{{cube}.{member}}}"),
                },
                "groupingSet": null,
            })
            .to_string()
        }

        assert_eq!(
            query_plan
                .as_logical_plan()
                .find_cube_scan_wrapped_sql()
                .request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    json!({
                        "cubeName": "WideCube",
                        "alias": "max_source_measu",
                        "expr": {
                            "type": "SqlFunction",
                            "cubeParams": ["WideCube"],
                            "sql": "${WideCube.measure1}",
                        },
                        "groupingSet": null,
                    })
                    .to_string(),
                    json!({
                        "cubeName": "WideCube",
                        "alias": "max_source_measu_1",
                        "expr": {
                            "type": "SqlFunction",
                            "cubeParams": ["WideCube"],
                            "sql": "${WideCube.measure2}",
                        },
                        "groupingSet": null,
                    })
                    .to_string(),
                    json!({
                        "cubeName": "WideCube",
                        "alias": "sum_source_measu",
                        "expr": {
                            "type": "SqlFunction",
                            "cubeParams": ["WideCube"],
                            "sql": "${WideCube.measure3}",
                        },
                        "groupingSet": null,
                    })
                    .to_string(),
                    json!({
                        "cubeName": "WideCube",
                        "alias": "max_source_measu_2",
                        "expr": {
                            "type": "SqlFunction",
                            "cubeParams": ["WideCube"],
                            "sql": "${WideCube.measure4}",
                        },
                        "groupingSet": null,
                    })
                    .to_string(),
                ]),
                dimensions: Some(vec![
                    trivial_member_expr("WideCube", "dim2", "dim2"),
                    trivial_member_expr("WideCube", "dim3", "dim3"),
                    trivial_member_expr("WideCube", "dim4", "dim4"),
                    json!({
                        "cubeName": "WideCube",
                        "alias": "pivot_grouping",
                        "expr": {
                            "type": "SqlFunction",
                            "cubeParams": [],
                            "sql": "0",
                        },
                        "groupingSet": null,
                    })
                    .to_string()
                ]),
                segments: Some(vec![]),
                order: Some(vec![
                    vec!["dim2".to_string(), "asc".to_string(),],
                    vec!["dim3".to_string(), "asc".to_string(),],
                    vec!["dim4".to_string(), "asc".to_string(),],
                    vec!["pivot_grouping".to_string(), "asc".to_string(),],
                ]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("WideCube.dim1".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["foo".to_string()]),
                    or: None,
                    and: None,
                },]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn metabase_sum_dim_wrapper() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
SELECT
  "source"."str0" AS "str0",
  SUM("source"."num1") AS "sum"
FROM (
  SELECT
    "public"."WideCube"."dim1" AS "str0",
    "public"."WideCube"."dim2" AS "num1"
  FROM "public"."WideCube"
) AS "source"
GROUP BY "source"."str0"
ORDER BY "source"."str0" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        assert_eq!(
            query_plan.as_logical_plan().find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "WideCube.dim1".to_string(),
                    "WideCube.dim2".to_string(),
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );
        assert!(!query_plan
            .as_logical_plan()
            .find_cube_scan_wrapped_sql()
            .wrapped_sql
            .sql
            .contains("ungrouped"));
    }

    #[tokio::test]
    async fn test_outer_aggr_simple_count() {
        let logical_plan = convert_select_to_query_plan(
            "
            SELECT CAST(TRUNC(EXTRACT(YEAR FROM order_date)) AS INTEGER), Count(1) FROM KibanaSampleDataEcommerce GROUP BY 1
            ".to_string(),
            DatabaseProtocol::PostgreSQL
        ).await.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("year".to_owned()),
                    date_range: None
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn metabase_date_filters() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let now = "str_to_date('2022-01-01 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')";
        let cases = vec![
            // last 30 days
            [
                format!("CAST(({} + (INTERVAL '-30 day')) AS date)", now),
                format!("CAST({} AS date)", now),
                "2021-12-02T00:00:00.000Z".to_string(),
                "2021-12-31T23:59:59.999Z".to_string(),
            ],
            // last 30 weeks
            [
                format!("(CAST(date_trunc('week', (({} + (INTERVAL '-30 week')) + (INTERVAL '1 day'))) AS timestamp) + (INTERVAL '-1 day'))", now),
                format!("(CAST(date_trunc('week', ({} + (INTERVAL '1 day'))) AS timestamp) + (INTERVAL '-1 day'))", now),
                "2021-05-30T00:00:00.000Z".to_string(),
                "2021-12-25T23:59:59.999Z".to_string(),
            ],
            // last 30 quarters
            [
                format!("date_trunc('quarter', ({} + (INTERVAL '-90 month')))", now),
                format!("date_trunc('quarter', {})", now),
                "2014-07-01T00:00:00.000Z".to_string(),
                "2021-12-31T23:59:59.999Z".to_string(),
            ],
            // this year
            [
                format!("date_trunc('year', {})", now),
                format!("date_trunc('year', ({} + (INTERVAL '1 year')))", now),
                "2022-01-01T00:00:00.000Z".to_string(),
                "2022-12-31T23:59:59.999Z".to_string(),
            ],
            // next 2 years including current
            [
                format!("date_trunc('year', {})", now),
                format!("date_trunc('year', ({} + (INTERVAL '3 year')))", now),
                "2022-01-01T00:00:00.000Z".to_string(),
                "2024-12-31T23:59:59.999Z".to_string(),
            ],
        ];
        for [lte, gt, from, to] in cases {
            let logical_plan = convert_select_to_query_plan(
                format!(
                    "SELECT count FROM (SELECT count FROM KibanaSampleDataEcommerce
                    WHERE (order_date >= {} AND order_date < {})) source",
                    lte, gt
                ),
                DatabaseProtocol::PostgreSQL,
            )
            .await
            .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: None,
                        date_range: Some(json!(vec![from, to])),
                    }]),
                    order: Some(vec![]),
                    ungrouped: Some(true),
                    ..Default::default()
                }
            );
        }

        let logical_plan = convert_select_to_query_plan(
            "SELECT \"source\".\"count\" AS \"count\"
            FROM (
                    SELECT \"public\".\"KibanaSampleDataEcommerce\".\"count\" AS \"count\" FROM \"public\".\"KibanaSampleDataEcommerce\"
                    WHERE \"public\".\"KibanaSampleDataEcommerce\".\"order_date\"
                    BETWEEN timestamp with time zone '2022-06-13T12:30:00.000Z'
                    AND timestamp with time zone '2022-06-29T12:30:00.000Z'
            )
            \"source\""
            .to_string(),
        DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2022-06-13T12:30:00.000Z".to_string(),
                        "2022-06-29T12:30:00.000Z".to_string()
                    ]))
                }]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );

        let cases = vec![
            // prev 5 days starting 4 weeks ago
            [
                "(INTERVAL '4 week')".to_string(),
                format!("CAST(({} + (INTERVAL '-5 day')) AS date)", now),
                format!("CAST({} AS date)", now),
                "2021-11-29T00:00:00.000Z".to_string(),
                "2021-12-04T00:00:00.000Z".to_string(),
            ],
            // prev 5 weeks starting 4 weeks ago
            [
                "(INTERVAL '4 week')".to_string(),
                format!("(CAST(date_trunc('week', (({} + (INTERVAL '-5 week')) + (INTERVAL '1 day'))) AS timestamp) + (INTERVAL '-1 day'))", now),
                format!("(CAST(date_trunc('week', ({} + (INTERVAL '1 day'))) AS timestamp) + (INTERVAL '-1 day'))", now),
                "2021-10-24T00:00:00.000Z".to_string(),
                "2021-11-28T00:00:00.000Z".to_string(),
            ],
            // prev 5 months starting 4 months ago
            [
                "(INTERVAL '4 month')".to_string(),
                format!("date_trunc('month', ({} + (INTERVAL '-5 month')))", now),
                format!("date_trunc('month', {})", now),
                "2021-04-01T00:00:00.000Z".to_string(),
                "2021-09-01T00:00:00.000Z".to_string(),
            ],
        ];

        for [interval, lowest, highest, from, to] in cases {
            let logical_plan = convert_select_to_query_plan(
                format!(
                    "SELECT \"source\".\"count\" AS \"count\"
                    FROM (
                        SELECT \"public\".\"KibanaSampleDataEcommerce\".\"count\" AS \"count\" FROM \"public\".\"KibanaSampleDataEcommerce\"
                        WHERE (\"public\".\"KibanaSampleDataEcommerce\".\"order_date\" + {}) BETWEEN {} AND {}
                    )
                    \"source\"",
                    interval, lowest, highest
                ),
                DatabaseProtocol::PostgreSQL,
            )
            .await
            .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: None,
                        date_range: Some(json!(vec![from, to])),
                    }]),
                    order: Some(vec![]),
                    ungrouped: Some(true),
                    ..Default::default()
                }
            );
        }

        let logical_plan = convert_select_to_query_plan(
            format!(
                "SELECT \"source\".\"order_date\" AS \"order_date\", \"source\".\"max\" AS \"max\"
                FROM (SELECT date_trunc('month', \"KibanaSampleDataEcommerce\".\"order_date\") AS \"order_date\", max(\"KibanaSampleDataEcommerce\".\"maxPrice\") AS \"max\" FROM \"KibanaSampleDataEcommerce\"
                GROUP BY date_trunc('month', \"KibanaSampleDataEcommerce\".\"order_date\")
                ORDER BY date_trunc('month', \"KibanaSampleDataEcommerce\".\"order_date\") ASC) \"source\"
                WHERE (CAST(date_trunc('month', \"source\".\"order_date\") AS timestamp) + (INTERVAL '60 minute')) BETWEEN date_trunc('minute', ({} + (INTERVAL '-30 minute')))
                AND date_trunc('minute', {})",
                now, now
            ),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.maxPrice".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: None,
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "asc".to_string(),
                ]]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_metabase_bins() {
        let logical_plan = convert_select_to_query_plan(
            "
            SELECT ((floor(((\"public\".\"KibanaSampleDataEcommerce\".\"taxful_total_price\" - 1.1) / 0.025)) * 0.025) + 1.1) AS \"taxful_total_price\", count(*) AS \"count\"
            FROM \"public\".\"KibanaSampleDataEcommerce\"
            GROUP BY ((floor(((\"public\".\"KibanaSampleDataEcommerce\".\"taxful_total_price\" - 1.1) / 0.025)) * 0.025) + 1.1)
            ORDER BY ((floor(((\"public\".\"KibanaSampleDataEcommerce\".\"taxful_total_price\" - 1.1) / 0.025)) * 0.025) + 1.1) ASC;
            ".to_string(),
            DatabaseProtocol::PostgreSQL
        ).await.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn metabase_contains_str_filters() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
                "SELECT \"public\".\"KibanaSampleDataEcommerce\".\"count\" AS \"count\"
                FROM \"public\".\"KibanaSampleDataEcommerce\"
                WHERE (lower(\"public\".\"KibanaSampleDataEcommerce\".\"customer_gender\") like '%female%')
                LIMIT 10"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                limit: Some(10),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("contains".to_string()),
                    values: Some(vec!["female".to_string()]),
                    or: None,
                    and: None,
                },]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );

        let logical_plan = convert_select_to_query_plan(
            "SELECT \"public\".\"KibanaSampleDataEcommerce\".\"count\" AS \"count\"
            FROM \"public\".\"KibanaSampleDataEcommerce\"
            WHERE (NOT (lower(\"public\".\"KibanaSampleDataEcommerce\".\"customer_gender\") like '%female%') OR \"public\".\"KibanaSampleDataEcommerce\".\"customer_gender\" IS NULL)
            LIMIT 10"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                limit: Some(10),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notContains".to_string()),
                            values: Some(vec!["female".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notSet".to_string()),
                            values: None,
                            or: None,
                            and: None,
                        })
                    ]),
                    and: None,
                },]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn metabase_between_numbers_filters() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
                "SELECT \"public\".\"KibanaSampleDataEcommerce\".\"count\" AS \"count\"
                FROM \"public\".\"KibanaSampleDataEcommerce\"
                WHERE \"public\".\"KibanaSampleDataEcommerce\".\"taxful_total_price\" BETWEEN 1 AND 2
                LIMIT 10"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                limit: Some(10),
                filters: Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                        operator: Some("gte".to_string()),
                        values: Some(vec!["1".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                        operator: Some("lte".to_string()),
                        values: Some(vec!["2".to_string()]),
                        or: None,
                        and: None,
                    }
                ]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );

        let logical_plan = convert_select_to_query_plan(
            "SELECT \"public\".\"KibanaSampleDataEcommerce\".\"count\" AS \"count\"
            FROM \"public\".\"KibanaSampleDataEcommerce\"
            WHERE \"public\".\"KibanaSampleDataEcommerce\".\"taxful_total_price\" NOT BETWEEN 1 AND 2
            LIMIT 10"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                limit: Some(10),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some(
                                "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                            ),
                            operator: Some("lt".to_string()),
                            values: Some(vec!["1".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some(
                                "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                            ),
                            operator: Some("gt".to_string()),
                            values: Some(vec!["2".to_string()]),
                            or: None,
                            and: None,
                        })
                    ]),
                    and: None,
                },]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn metabase_aggreagte_by_week_of_year() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
                "SELECT ceil((CAST(extract(doy from CAST(date_trunc('week', \"KibanaSampleDataEcommerce\".\"order_date\") AS timestamp)) AS integer) / 7.0)) AS \"order_date\",
                               min(\"KibanaSampleDataEcommerce\".\"minPrice\") AS \"min\"
                FROM \"KibanaSampleDataEcommerce\"
                GROUP BY ceil((CAST(extract(doy from CAST(date_trunc('week', \"KibanaSampleDataEcommerce\".\"order_date\") AS timestamp)) AS integer) / 7.0))
                ORDER BY ceil((CAST(extract(doy from CAST(date_trunc('week', \"KibanaSampleDataEcommerce\".\"order_date\") AS timestamp)) AS integer) / 7.0)) ASC"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.minPrice".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("week".to_string()),
                    date_range: None,
                },]),
                order: Some(vec![]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn datastudio_date_aggregations() {
        init_testing_logger();

        let supported_granularities = vec![
            // date
            [
                "CAST(DATE_TRUNC('SECOND', \"order_date\") AS DATE)",
                "second",
            ],
            // date, time
            ["DATE_TRUNC('SECOND', \"order_date\")", "second"],
            // date, hour, minute
            [
                "DATE_TRUNC('MINUTE', DATE_TRUNC('SECOND', \"order_date\"))",
                "minute",
            ],
            // month
            [
                "EXTRACT(MONTH FROM DATE_TRUNC('SECOND', \"order_date\"))::integer",
                "month",
            ],
            // minute
            [
                "EXTRACT(MINUTE FROM DATE_TRUNC('SECOND', \"order_date\"))::integer",
                "minute",
            ],
            // hour
            [
                "EXTRACT(HOUR FROM DATE_TRUNC('SECOND', \"order_date\"))::integer",
                "hour",
            ],
            // day of month
            [
                "EXTRACT(DAY FROM DATE_TRUNC('SECOND', \"order_date\"))::integer",
                "day",
            ],
            // iso week / iso year / day of year
            ["DATE_TRUNC('SECOND', \"order_date\")", "second"],
            // month, day
            [
                "CAST(TO_CHAR(DATE_TRUNC('SECOND', \"order_date\"), 'MMDD') AS BIGINT)",
                "second",
            ],
            // date, hour, minute
            [
                "DATE_TRUNC('MINUTE', DATE_TRUNC('SECOND', \"order_date\"))",
                "minute",
            ],
            // date, hour
            [
                "DATE_TRUNC('HOUR', DATE_TRUNC('SECOND', \"order_date\"))",
                "hour",
            ],
            // year, month
            [
                "CAST(DATE_TRUNC('MONTH', DATE_TRUNC('SECOND', \"order_date\")) AS DATE)",
                "month",
            ],
            // year
            [
                "CAST(DATE_TRUNC('YEAR', DATE_TRUNC('SECOND', \"order_date\")) AS DATE)",
                "year",
            ],
        ];

        for [expr, expected_granularity] in supported_granularities {
            let logical_plan = convert_select_to_query_plan(
                format!(
                    "SELECT {} AS \"qt_u3dj8wr1vc\", COUNT(1) AS \"__record_count\" FROM KibanaSampleDataEcommerce GROUP BY \"qt_u3dj8wr1vc\"",
                    expr
                ),
                DatabaseProtocol::PostgreSQL,
            )
            .await
            .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some(expected_granularity.to_string()),
                        date_range: None,
                    }]),
                    order: Some(vec![]),
                    ..Default::default()
                }
            )
        }
    }

    #[tokio::test]
    async fn test_datastudio_min_max_date() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        for fun in ["Max", "Min"].iter() {
            let logical_plan = convert_select_to_query_plan(
                format!(
                    "
                SELECT
                    CAST(Date_trunc('SECOND', \"order_date\") AS DATE) AS \"qt_m3uskv6gwc\",
                    {}(Date_trunc('SECOND', \"order_date\")) AS \"qt_d3yqo2towc\"
                FROM  KibanaSampleDataEcommerce
                GROUP BY \"qt_m3uskv6gwc\"
                ",
                    fun
                ),
                DatabaseProtocol::PostgreSQL,
            )
            .await
            .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    order: Some(vec![]),
                    ungrouped: Some(true),
                    ..Default::default()
                }
            )
        }
    }

    #[tokio::test]
    async fn test_datastudio_between_dates_filter() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            "
            SELECT
                CAST(Date_trunc('SECOND', \"order_date\") AS DATE) AS \"qt_m3uskv6gwc\",
                COUNT(1) AS \"__record_count\"
            FROM KibanaSampleDataEcommerce
            WHERE Date_trunc('SECOND', \"order_date\")
                BETWEEN
                    CAST('2022-07-11 18:00:00.000000' AS TIMESTAMP)
                AND CAST('2022-07-11 19:00:00.000000' AS TIMESTAMP)
            GROUP BY \"qt_m3uskv6gwc\";
            "
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("second".to_string()),
                    date_range: Some(json!(vec![
                        "2022-07-11T18:00:00.000Z".to_string(),
                        "2022-07-11T19:00:00.000Z".to_string()
                    ])),
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_datastudio_string_start_with_filter() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            "
            SELECT
                CAST(Date_trunc('SECOND', \"order_date\") AS DATE) AS \"qt_m3uskv6gwc\",
                COUNT(1) AS \"__record_count\",
                \"customer_gender\"
            FROM  KibanaSampleDataEcommerce
            WHERE (\"customer_gender\" ~ 'test')
            GROUP BY \"qt_m3uskv6gwc\", \"customer_gender\";
            "
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("second".to_string()),
                    date_range: None,
                }]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("startsWith".to_string()),
                    values: Some(vec!["test".to_string()]),
                    or: None,
                    and: None,
                }]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_extract_date_trunc_week() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let supported_granularities = vec![
            (
                "EXTRACT(WEEK FROM DATE_TRUNC('MONTH', \"order_date\"))::integer",
                "month",
            ),
            (
                "EXTRACT(MONTH FROM DATE_TRUNC('WEEK', \"order_date\"))::integer",
                "week",
            ),
        ];

        for (expr, granularity) in supported_granularities {
            let logical_plan = convert_select_to_query_plan(
                format!(
                    "SELECT {} AS \"qt_u3dj8wr1vc\" FROM KibanaSampleDataEcommerce GROUP BY \"qt_u3dj8wr1vc\"",
                    expr
                ),
                DatabaseProtocol::PostgreSQL,
            )
            .await
            .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some(granularity.to_string()),
                        date_range: None,
                    }]),
                    order: Some(vec![]),
                    ..Default::default()
                }
            )
        }
    }

    #[tokio::test]
    async fn test_metabase_unwrap_date_cast() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            "SELECT max(CAST(\"KibanaSampleDataEcommerce\".\"order_date\" AS date)) AS \"max\" FROM \"KibanaSampleDataEcommerce\"".to_string(),
            DatabaseProtocol::PostgreSQL
        ).await.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_metabase_substring_user() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            "SELECT \"source\".\"substring131715\" AS \"substring131715\"
                FROM (
                    SELECT
                        \"public\".\"KibanaSampleDataEcommerce\".\"__user\" AS \"__user\",
                        SUBSTRING(\"public\".\"KibanaSampleDataEcommerce\".\"__user\" FROM 1 FOR 1234) AS \"substring131715\"
                    FROM \"public\".\"KibanaSampleDataEcommerce\"
                ) AS \"source\"
                LIMIT 10000".to_string(),
            DatabaseProtocol::PostgreSQL
        ).await.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                limit: Some(10000),
                ungrouped: Some(true),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_select_asterisk_cross_join() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            "SELECT * FROM \"KibanaSampleDataEcommerce\" CROSS JOIN Logs".to_string(),
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
                ]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.id".to_string(),
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
                join_hints: Some(vec![vec![
                    "KibanaSampleDataEcommerce".to_string(),
                    "Logs".to_string(),
                ],]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_select_distinct_dimensions() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            "SELECT DISTINCT customer_gender FROM KibanaSampleDataEcommerce".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        println!("logical_plan: {:?}", logical_plan);

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }
        );

        let logical_plan = convert_select_to_query_plan(
            "SELECT DISTINCT customer_gender FROM KibanaSampleDataEcommerce LIMIT 100".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        println!("logical_plan: {:?}", logical_plan);

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                limit: Some(100),
                ..Default::default()
            }
        );

        let logical_plan = convert_select_to_query_plan(
            "SELECT DISTINCT * FROM (SELECT customer_gender FROM KibanaSampleDataEcommerce LIMIT 100) q_0".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        println!("logical_plan: {:?}", logical_plan);

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                limit: Some(100),
                ungrouped: Some(true),
                ..Default::default()
            }
        );

        let logical_plan = convert_select_to_query_plan(
            "SELECT DISTINCT customer_gender, order_date FROM KibanaSampleDataEcommerce"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        println!("logical_plan: {:?}", logical_plan);

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }
        );

        let logical_plan = convert_select_to_query_plan(
            "SELECT DISTINCT MAX(maxPrice) FROM KibanaSampleDataEcommerce".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        println!("logical_plan: {:?}", logical_plan);

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.maxPrice".to_string(),]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }
        );

        let logical_plan = convert_select_to_query_plan(
            "SELECT DISTINCT * FROM (SELECT customer_gender, MAX(maxPrice) FROM KibanaSampleDataEcommerce GROUP BY 1) q_0".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        println!("logical_plan: {:?}", logical_plan);

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.maxPrice".to_string(),]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }
        );

        let logical_plan = convert_select_to_query_plan(
            "SELECT DISTINCT * FROM KibanaSampleDataEcommerce".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        println!("logical_plan: {:?}", logical_plan);

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
                ]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.id".to_string(),
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "KibanaSampleDataEcommerce.last_mod".to_string(),
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "KibanaSampleDataEcommerce.notes".to_string(),
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    "KibanaSampleDataEcommerce.has_subscription".to_string(),
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_sort_relations() -> Result<(), CubeError> {
        init_testing_logger();

        insta::assert_snapshot!(
            "test_sort_relations_0",
            execute_query(
                "select pg_class.oid as oid from pg_class order by pg_class.oid asc".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "test_sort_relations_1",
            execute_query(
                "select * from (select pg_class.oid AS oid from pg_class order by pg_class.oid) source".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "test_sort_relations_2",
            execute_query(
                "select * from (select oid from pg_class order by pg_class.oid) t".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "test_sort_relations_3",
            execute_query(
                "select t.oid as oid from (select oid as oid from pg_class) t order by t.oid"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "test_sort_relations_4",
            execute_query(
                "select oid as oid from (select count(oid) as oid from pg_class order by count(pg_class.oid)) t".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "test_sort_relations_5",
            execute_query(
                "select oid as oid from (select count(oid) as oid from pg_class order by count(oid)) t".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "test_sort_relations_6",
            execute_query(
                "select pg_class.oid as oid from pg_class group by pg_class.oid order by pg_class.oid asc".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "test_sort_relations_7",
            execute_query(
                "select * from (select oid from pg_class group by pg_class.oid order by pg_class.oid) t".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_offset_limit() -> Result<(), CubeError> {
        init_testing_logger();

        insta::assert_snapshot!(
            "test_offset_limit_1",
            execute_query(
                "select n from generate_series(1, 1000) pos(n) limit 10 offset 10".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "test_offset_limit_2",
            execute_query(
                "select n from generate_series(1, 1000) pos(n) limit 10 offset 0".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "test_offset_limit_3",
            execute_query(
                "select n from generate_series(1, 1000) pos(n) limit 0 offset 10".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "test_offset_limit_4",
            execute_query(
                "select n from generate_series(1, 1000) pos(n) limit 100 offset 100".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "test_offset_limit_5",
            execute_query(
                "select n from generate_series(1, 1000) pos(n) limit 100 offset 990".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_superset_pagination() {
        init_testing_logger();

        // At first, Superset gets the total count (no more than 50k)
        let logical_plan = convert_select_to_query_plan(
            "SELECT COUNT(*) AS rowcount FROM (SELECT order_date as order_date FROM public.\"KibanaSampleDataEcommerce\" GROUP BY order_date LIMIT 50000) AS rowcount_qry".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await.as_logical_plan();

        let cube_scan = logical_plan.find_cube_scan();
        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                limit: Some(50000),
                ..Default::default()
            }
        );

        let logical_plan = convert_select_to_query_plan(
            "SELECT order_date AS order_date FROM public.\"KibanaSampleDataEcommerce\" GROUP BY order_date LIMIT 200 OFFSET 200".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await.as_logical_plan();

        let cube_scan = logical_plan.find_cube_scan();
        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                limit: Some(200),
                offset: Some(200),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_holistics_date_trunc_date32() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "holistics_date_trunc_date32",
            execute_query(
                "
                with \"h__dates\" AS (
                    SELECT
                        CAST ('2023-02-01' AS date) as \"start_range\",
                        CAST ( '2023-02-28' AS date ) as \"end_range\",
                        28 as \"length\"
                )
                SELECT
                    DATE_TRUNC( 'month', \"start_range\") AS \"dm_ddt_d_6e2110\",
                    MAX(\"length\") AS \"h_dates_length\"
                FROM \"h__dates\"
                GROUP BY 1
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_holistics_group_by_date() {
        init_testing_logger();

        for granularity in ["year", "quarter", "month", "week", "day", "hour", "minute"].iter() {
            let logical_plan = convert_select_to_query_plan(
                format!("
                    SELECT
                        TO_CHAR((CAST((DATE_TRUNC('{}', (CAST(\"table\".\"order_date\" AS timestamptz)) AT TIME ZONE 'Etc/UTC')) AT TIME ZONE 'Etc/UTC' AS timestamptz)) AT TIME ZONE 'Etc/UTC', 'YYYY-MM-DD HH24:MI:SS') AS \"dm_pu_ca_754b1e\",
                        MAX(\"table\".\"maxPrice\") AS \"a_pu_n_51f23b\"
                    FROM \"KibanaSampleDataEcommerce\" \"table\"
                    GROUP BY 1
                    ORDER BY 2 DESC
                    LIMIT 100000",
                    granularity),
                DatabaseProtocol::PostgreSQL
            ).await.as_logical_plan();

            let cube_scan = logical_plan.find_cube_scan();

            assert_eq!(
                cube_scan.request,
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.maxPrice".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some(granularity.to_string()),
                        date_range: None
                    }]),
                    order: Some(vec![]),
                    ..Default::default()
                }
            );
        }
    }

    #[tokio::test]
    async fn test_holistics_split_with_literals() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            "SELECT
                \"table\".\"maxPrice\" AS \"pu_mn_287b51__0\",
                MIN(\"table\".\"minPrice\") AS \"m_pu_mn_ad42df__1\",
                CAST ( NULL AS text ) AS \"h__placeholder_marker_0\",
                0 AS \"h__model_level\"
            FROM \"public\".\"KibanaSampleDataEcommerce\" \"table\"
            GROUP BY
                1,
                3,
                4
            ORDER BY
                4 DESC
            LIMIT 100000"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let cube_scan = logical_plan.find_cube_scan();

        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.minPrice".to_string()
                ]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }
        );

        let logical_plan = convert_select_to_query_plan(
            "SELECT
                TO_CHAR((CAST ( (DATE_TRUNC ( 'month', (CAST ( \"table\".\"order_date\" AS timestamptz )) AT TIME ZONE 'Etc/UTC' )) AT TIME ZONE 'Etc/UTC' AS timestamptz )) AT TIME ZONE 'Etc/UTC', 'YYYY-MM-DD HH24:MI:SS.US') AS \"dm_pu_ca_754b1e__0\",
                MAX(\"table\".\"maxPrice\") AS \"m_pu_mn_0844e5__1\",
                CAST ( NULL AS text ) AS \"h__placeholder_marker_0\",
                0 AS \"h__model_level\"
            FROM \"public\".\"KibanaSampleDataEcommerce\" \"table\"
            GROUP BY
                1,
                3,
                4
            ORDER BY
                4 DESC
            LIMIT 100000".to_string(),
            DatabaseProtocol::PostgreSQL
        ).await.as_logical_plan();

        let cube_scan = logical_plan.find_cube_scan();

        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.maxPrice".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("month".to_owned()),
                    date_range: None
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_holistics_str_not_contains_filter() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            "SELECT COUNT(\"table\".\"count\") AS \"c_pu_c_d4696e\"
            FROM \"public\".\"KibanaSampleDataEcommerce\" \"table\"
            WHERE NOT(\"table\".\"customer_gender\" ILIKE ('%' || CAST ( 'test' AS text ) || '%'))
            ORDER BY 1 DESC
            LIMIT 100000"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let cube_scan = logical_plan.find_cube_scan();

        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "desc".to_string()
                ]]),
                limit: Some(100000),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("notContains".to_string()),
                    values: Some(vec!["test".to_string()]),
                    or: None,
                    and: None,
                }]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_holistics_aggr_fun_with_null() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            "SELECT \"table\".\"count\" AS \"pu_c_3dcebf__0\",
                \"table\".\"maxPrice\" AS \"pu_mn_287b51__1\",
                MIN(\"table\".\"minPrice\") AS \"m_pu_mn_ad42df__2\",
                CAST ( NULL AS text ) AS \"h__placeholder_marker_0\",
                CAST ( NULL AS text ) AS \"h__placeholder_marker_1\",
                0 AS \"h__model_level\"
            FROM \"public\".\"KibanaSampleDataEcommerce\" \"table\"
            GROUP BY
                1,
                2,
                4,
                5,
                6
            UNION ALL
            (
                SELECT
                    CAST ( NULL AS numeric ) AS \"pu_c_3dcebf__0\",
                    \"table\".\"maxPrice\" AS \"pu_mn_287b51__1\",
                    MIN(CAST ( NULL AS numeric )) AS \"m_pu_mn_ad42df__2\",
                    'total' AS \"h__placeholder_marker_0\",
                    CAST ( NULL AS text ) AS \"h__placeholder_marker_1\",
                    2 AS \"h__model_level\"
                FROM \"public\".\"KibanaSampleDataEcommerce\" \"table\"
                GROUP BY
                1,
                2,
                4,
                5,
                6
            )
            ORDER BY
                6 DESC
            LIMIT 100000"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let cube_scans = logical_plan
            .find_cube_scans()
            .iter()
            .map(|cube| cube.request.clone())
            .collect::<Vec<V1LoadRequestQuery>>();

        assert_eq!(
            cube_scans.contains(&V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.maxPrice".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }),
            true
        );

        assert_eq!(
            cube_scans.contains(&V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.minPrice".to_string()
                ]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }),
            true
        );
    }

    #[tokio::test]
    async fn test_holistics_split_with_nulls() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            "SELECT TO_CHAR((CAST ( (DATE_TRUNC ( 'quarter', (CAST ( \"table\".\"order_date\" AS timestamptz )) AT TIME ZONE 'Etc/UTC' )) AT TIME ZONE 'Etc/UTC' AS timestamptz )) AT TIME ZONE 'Etc/UTC', 'YYYY-MM-DD HH24:MI:SS.US') AS \"dq_pu_ca_6b9696__0\",
                \"table\".\"maxPrice\" AS \"pu_mn_287b51__1\",
                MIN(\"table\".\"minPrice\") AS \"m_pu_mn_ad42df__2\",
                CAST ( NULL AS text ) AS \"h__placeholder_marker_0\",
                CAST ( NULL AS text ) AS \"h__placeholder_marker_1\",
                0 AS \"h__model_level\"
            FROM \"public\".\"KibanaSampleDataEcommerce\" \"table\"
            GROUP BY
                1,
                2,
                4,
                5,
                6
            UNION ALL
            (
                SELECT TO_CHAR((CAST ( (DATE_TRUNC ( 'quarter', (CAST ( CAST ( NULL AS timestamptz ) AS timestamptz )) AT TIME ZONE 'Etc/UTC' )) AT TIME ZONE 'Etc/UTC' AS timestamptz )) AT TIME ZONE 'Etc/UTC', 'YYYY-MM-DD HH24:MI:SS.US') AS \"dq_pu_ca_6b9696__0\",
                    \"table\".\"maxPrice\" AS \"pu_mn_287b51__1\",
                    MIN(CAST ( NULL AS numeric )) AS \"m_pu_mn_ad42df__2\",
                    'total' AS \"h__placeholder_marker_0\",
                    CAST ( NULL AS text ) AS \"h__placeholder_marker_1\",
                    2 AS \"h__model_level\"
                FROM \"public\".\"KibanaSampleDataEcommerce\" \"table\"
                GROUP BY
                1,
                2,
                4,
                5,
                6
            )
            ORDER BY 6 DESC
            LIMIT 100000".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let cube_scans = logical_plan
            .find_cube_scans()
            .iter()
            .map(|cube| cube.request.clone())
            .collect::<Vec<V1LoadRequestQuery>>();

        assert_eq!(
            cube_scans.contains(&V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.maxPrice".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }),
            true
        );

        assert_eq!(
            cube_scans.contains(&V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.minPrice".to_string()
                ]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("quarter".to_string()),
                    date_range: None,
                }]),
                order: Some(vec![]),
                ..Default::default()
            }),
            true
        );
    }

    #[tokio::test]
    async fn test_holistics_in_dates_list_filter() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            "SELECT COUNT(\"table\".\"count\") AS \"c_pu_c_d4696e\"
            FROM \"public\".\"KibanaSampleDataEcommerce\" \"table\"
            WHERE \"table\".\"order_date\" IN (CAST ( '2022-06-06 13:30:46' AS timestamptz ), CAST ( '2022-06-06 13:30:47' AS timestamptz ))
            ORDER BY 1 DESC
            LIMIT 100000".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let cube_scan = logical_plan.find_cube_scan();

        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "desc".to_string()
                ]]),
                limit: Some(100000),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec![
                        "2022-06-06T13:30:46.000Z".to_string(),
                        "2022-06-06T13:30:47.000Z".to_string()
                    ]),
                    or: None,
                    and: None,
                }]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_select_column_with_same_name_as_table() -> Result<(), CubeError> {
        init_testing_logger();

        insta::assert_snapshot!(
            "test_select_column_with_same_name_as_table",
            execute_query(
                "select table.column as column from (select 1 column, 2 table union all select 3 column, 4 table) table;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_quicksight_interval_mul_query() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT date_trunc('day', "order_date") AS "uuid.order_date_tg", COUNT(*) AS "count"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE
                "order_date" >= date_trunc('year', LOCALTIMESTAMP + -5 * interval '1 YEAR') AND
                "order_date" < date_trunc('year', LOCALTIMESTAMP)
            GROUP BY date_trunc('day', "order_date")
            ORDER BY date_trunc('day', "order_date") DESC NULLS LAST
            LIMIT 2500;
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let now = chrono::Utc::now();
        let current_year = now.naive_utc().date().year();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: Some(json!(vec![
                        format!("{}-01-01T00:00:00.000Z", current_year - 5),
                        format!("{}-12-31T23:59:59.999Z", current_year - 1),
                    ])),
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "desc".to_string()
                ]]),
                limit: Some(2500),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_quicksight_date_trunc_equals() {
        init_testing_logger();

        let base_date = "2022-08-27 19:43:09";
        let granularities = vec![
            (
                "second",
                "2022-08-27T19:43:09.000Z",
                "2022-08-27T19:43:09.999Z",
            ),
            (
                "minute",
                "2022-08-27T19:43:00.000Z",
                "2022-08-27T19:43:59.999Z",
            ),
            (
                "hour",
                "2022-08-27T19:00:00.000Z",
                "2022-08-27T19:59:59.999Z",
            ),
            (
                "day",
                "2022-08-27T00:00:00.000Z",
                "2022-08-27T23:59:59.999Z",
            ),
            (
                "week",
                "2022-08-22T00:00:00.000Z",
                "2022-08-28T23:59:59.999Z",
            ),
            (
                "month",
                "2022-08-01T00:00:00.000Z",
                "2022-08-31T23:59:59.999Z",
            ),
            (
                "quarter",
                "2022-07-01T00:00:00.000Z",
                "2022-09-30T23:59:59.999Z",
            ),
            (
                "year",
                "2022-01-01T00:00:00.000Z",
                "2022-12-31T23:59:59.999Z",
            ),
        ];

        for (granularity, date_min, date_max) in granularities {
            let sql = format!(
                r#"
                SELECT date_trunc('{}', "order_date") AS "uuid.order_date_tg", COUNT(*) AS "count"
                FROM "public"."KibanaSampleDataEcommerce"
                WHERE date_trunc('{}', "order_date") = date_trunc('{}', TO_TIMESTAMP('{}', 'yyyy-MM-dd HH24:mi:ss'))
                GROUP BY date_trunc('{}', "order_date")
                ORDER BY date_trunc('{}', "order_date") DESC NULLS LAST
                LIMIT 2500;
                "#,
                granularity, granularity, granularity, base_date, granularity, granularity,
            );
            let logical_plan = convert_select_to_query_plan(sql, DatabaseProtocol::PostgreSQL)
                .await
                .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some(granularity.to_string()),
                        date_range: Some(json!(vec![date_min.to_string(), date_max.to_string()]))
                    }]),
                    order: Some(vec![vec![
                        "KibanaSampleDataEcommerce.order_date".to_string(),
                        "desc".to_string()
                    ]]),
                    limit: Some(2500),
                    ..Default::default()
                }
            )
        }
    }

    #[tokio::test]
    async fn test_quicksight_str_starts_with_query() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "customer_gender" AS "uuid.customer_gender",
                COUNT(*) AS "count",
                DENSE_RANK() OVER (ORDER BY "customer_gender" DESC NULLS LAST) AS "$RANK_1"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE LEFT("customer_gender", 1) = 'f'
            GROUP BY "customer_gender";
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("startsWith".to_string()),
                    values: Some(vec!["f".to_string()]),
                    or: None,
                    and: None,
                }]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_quicksight_str_ends_with_query() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "customer_gender" AS "uuid.customer_gender",
                COUNT(*) AS "count",
                DENSE_RANK() OVER (ORDER BY "customer_gender" DESC NULLS LAST) AS "$RANK_1"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE RIGHT("customer_gender", 2) = 'le'
            GROUP BY "customer_gender";
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("endsWith".to_string()),
                    values: Some(vec!["le".to_string()]),
                    or: None,
                    and: None,
                }]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_quicksight_str_contains_query() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "customer_gender" AS "uuid.customer_gender",
                COUNT(*) AS "count",
                DENSE_RANK() OVER (ORDER BY "customer_gender" DESC NULLS LAST) AS "$RANK_1"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE case
                when strpos(substring("customer_gender", 1), 'al') > 0
                    then strpos(substring("customer_gender", 1), 'al') + 1 - 1
                else 0
            end > 0
            GROUP BY "customer_gender";
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("contains".to_string()),
                    values: Some(vec!["al".to_string()]),
                    or: None,
                    and: None,
                }]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_quicksight_str_does_not_contain_query() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "customer_gender" AS "uuid.customer_gender",
                COUNT(*) AS "count",
                DENSE_RANK() OVER (ORDER BY "customer_gender" DESC NULLS LAST) AS "$RANK_1"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE
                case
                    when strpos(substring("customer_gender", 1), 'al') > 0
                        then strpos(substring("customer_gender", 1), 'al') + 1 - 1 else 0
                    end = 0 AND
                "customer_gender" IS NOT NULL
            GROUP BY "customer_gender";
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("notContains".to_string()),
                        values: Some(vec!["al".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("set".to_string()),
                        values: None,
                        or: None,
                        and: None,
                    },
                ]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_quicksight_num_starts_with_query() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "maxPrice" AS "uuid.maxPrice",
                COUNT(*) AS "count",
                DENSE_RANK() OVER (ORDER BY "maxPrice" DESC NULLS LAST) AS "$RANK_1"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE LEFT(CAST("maxPrice" AS VARCHAR), 1) = '1'
            GROUP BY "maxPrice";
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
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.count".to_string(),
                ]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                    operator: Some("startsWith".to_string()),
                    values: Some(vec!["1".to_string()]),
                    or: None,
                    and: None,
                }]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_quicksight_num_ends_with_query() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "maxPrice" AS "uuid.maxPrice",
                COUNT(*) AS "count",
                DENSE_RANK() OVER (ORDER BY "maxPrice" DESC NULLS LAST) AS "$RANK_1"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE RIGHT(CAST("maxPrice" AS VARCHAR), 2) = '23'
            GROUP BY "maxPrice";
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
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.count".to_string(),
                ]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                    operator: Some("endsWith".to_string()),
                    values: Some(vec!["23".to_string()]),
                    or: None,
                    and: None,
                }]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_quicksight_num_contains_query() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "maxPrice" AS "uuid.maxPrice",
                COUNT(*) AS "count",
                DENSE_RANK() OVER (ORDER BY "maxPrice" DESC NULLS LAST) AS "$RANK_1"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE case
                when strpos(substring(CAST("maxPrice" AS VARCHAR), 1), '45') > 0
                    then strpos(substring(CAST("maxPrice" AS VARCHAR), 1), '45') + 1 - 1
                else 0
            end > 0
            GROUP BY "maxPrice";
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
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.count".to_string(),
                ]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                    operator: Some("contains".to_string()),
                    values: Some(vec!["45".to_string()]),
                    or: None,
                    and: None,
                }]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_quicksight_num_does_not_contain_query() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "maxPrice" AS "uuid.maxPrice",
                COUNT(*) AS "count",
                DENSE_RANK() OVER (ORDER BY "maxPrice" DESC NULLS LAST) AS "$RANK_1"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE
                case
                    when strpos(substring(CAST("maxPrice" AS VARCHAR), 1), '67') > 0
                        then strpos(substring(CAST("maxPrice" AS VARCHAR), 1), '67') + 1 - 1 else 0
                    end = 0 AND
                "maxPrice" IS NOT NULL
            GROUP BY "maxPrice";
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
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.count".to_string(),
                ]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                        operator: Some("notContains".to_string()),
                        values: Some(vec!["67".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                        operator: Some("set".to_string()),
                        values: None,
                        or: None,
                        and: None,
                    },
                ]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_filter_date_part_by_year() {
        init_testing_logger();

        fn assert_expected_result(query_plan: QueryPlan) {
            assert_eq!(
                query_plan.as_logical_plan().find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some("year".to_string()),
                        date_range: Some(json!(vec![
                            "2019-01-01".to_string(),
                            "2019-12-31".to_string(),
                        ])),
                    },]),
                    order: Some(vec![]),
                    ..Default::default()
                }
            )
        }

        assert_expected_result(
            convert_select_to_query_plan(
                r#"
            SELECT
                COUNT(*) AS "count",
                date_part('YEAR', "KibanaSampleDataEcommerce"."order_date") AS "yr:completedAt:ok"
            FROM "public"."KibanaSampleDataEcommerce" "KibanaSampleDataEcommerce"
            WHERE date_part('YEAR', "KibanaSampleDataEcommerce"."order_date") = 2019
            GROUP BY 2
            ;"#
                .to_string(),
                DatabaseProtocol::PostgreSQL,
            )
            .await,
        );

        // Same as above, but with string literal.
        assert_expected_result(
            convert_select_to_query_plan(
                r#"
            SELECT
                COUNT(*) AS "count",
                date_part('YEAR', "KibanaSampleDataEcommerce"."order_date") AS "yr:completedAt:ok"
            FROM "public"."KibanaSampleDataEcommerce" "KibanaSampleDataEcommerce"
            WHERE date_part('YEAR', "KibanaSampleDataEcommerce"."order_date") = '2019'
            GROUP BY 2
            ;"#
                .to_string(),
                DatabaseProtocol::PostgreSQL,
            )
            .await,
        )
    }

    #[tokio::test]
    async fn test_filter_extract_by_year() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                COUNT(*) AS "count",
                EXTRACT(YEAR FROM "KibanaSampleDataEcommerce"."order_date") AS "yr:completedAt:ok"
            FROM "public"."KibanaSampleDataEcommerce" "KibanaSampleDataEcommerce"
            WHERE EXTRACT(YEAR FROM "KibanaSampleDataEcommerce"."order_date") = 2019
            GROUP BY 2
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("year".to_string()),
                    date_range: Some(json!(vec![
                        "2019-01-01".to_string(),
                        "2019-12-31".to_string(),
                    ])),
                },]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_tableau_filter_extract_by_year() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                COUNT(*) AS "count",
                CAST(TRUNC(EXTRACT(YEAR FROM "KibanaSampleDataEcommerce"."order_date")) AS INTEGER) AS "yr:completedAt:ok"
            FROM "public"."KibanaSampleDataEcommerce" "KibanaSampleDataEcommerce"
            WHERE (CAST(TRUNC(EXTRACT(YEAR FROM "KibanaSampleDataEcommerce"."order_date")) AS INTEGER) = 2019)
            GROUP BY 2
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("year".to_string()),
                    date_range: Some(json!(vec![
                        "2019-01-01".to_string(),
                        "2019-12-31".to_string(),
                    ])),
                },]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_quicksight_date_trunc_column_less_or_eq() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT date_trunc('day', "order_date") AS "uuid.order_date_tg", COUNT(*) AS "count"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE
                "order_date" >= date_trunc('day', TO_TIMESTAMP('2020-01-01 00:00:00', 'yyyy-MM-dd HH24:mi:ss')) AND
                date_trunc('day', "order_date") <= date_trunc('day', LOCALTIMESTAMP + -5 * interval '1 DAY')
            GROUP BY date_trunc('day', "order_date")
            ORDER BY date_trunc('day', "order_date") DESC NULLS LAST
            LIMIT 2500;
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let end_date = chrono::Utc::now().date_naive() - chrono::Duration::days(5);
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: Some(json!(vec![
                        "2020-01-01T00:00:00.000Z".to_string(),
                        format!("{}T23:59:59.999Z", end_date),
                    ]))
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "desc".to_string()
                ]]),
                limit: Some(2500),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_quicksight_excluding_n_weeks() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT date_trunc('day', "order_date") AS "uuid.order_date_tg", COUNT(*) AS "count"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE
                "order_date" >= date_trunc('day', TO_TIMESTAMP('2020-01-01 00:00:00', 'yyyy-MM-dd HH24:mi:ss')) AND
                DATE_TRUNC(
                    'week',
                    "order_date"  + INTERVAL '1 day'
                ) - INTERVAL '1 day' <= DATE_TRUNC(
                    'week',
                    LOCALTIMESTAMP + 7 * -5 * interval '1 DAY' + INTERVAL '1 day'
                ) - INTERVAL '1 day'
            GROUP BY date_trunc('day', "order_date")
            ORDER BY date_trunc('day', "order_date") DESC NULLS LAST
            LIMIT 2500;
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let now = chrono::Utc::now();
        let duration_sub_weeks = chrono::Duration::weeks(4);
        let duration_sub_days =
            chrono::Duration::days(now.weekday().num_days_from_sunday() as i64 + 1);
        let end_date = now.date_naive() - duration_sub_weeks - duration_sub_days;
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: Some(json!(vec![
                        "2020-01-01T00:00:00.000Z".to_string(),
                        format!("{}T23:59:59.999Z", end_date),
                    ]))
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "desc".to_string()
                ]]),
                limit: Some(2500),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_char_length() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT char_length("ta_1"."customer_gender") "cl"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1";
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_sum_measure_binary_expr_unwrap() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT SUM("ta_1"."sumPrice" / NULLIF(CAST(10.0 AS FLOAT8), 0.0)) "cl"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1";
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.sumPrice".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }
    #[tokio::test]
    async fn test_in_filter() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce WHERE customer_gender IN ('female', 'male')"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await;
        let cube_scan = query_plan.as_logical_plan().find_cube_scan();

        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["female".to_string(), "male".to_string()]),
                    or: None,
                    and: None,
                }]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_thoughtspot_casts() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT CAST("ta_4"."ca_3" AS FLOAT8), CAST("ta_4"."ca_3" AS INT2), CAST("ta_4"."ca_3" AS BOOL)
            FROM (
                SELECT sum("ta_1"."count") AS "ca_3"
                FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
            ) AS "ta_4"
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
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_derived_dot_column() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            select
                "_"."t1.agentCountApprox" as "agentCountApprox",
                "_"."a0" as "a0"
            from (
                select
                    sum(cast("rows"."t0.taxful_total_price" as decimal)) as "a0",
                    "rows"."t1.agentCountApprox" as "t1.agentCountApprox"
                from (
                    select
                        "$Outer"."t1.agentCountApprox",
                        "$Inner"."t0.taxful_total_price"
                    from (
                        select
                            "_"."agentCount" as "t1.agentCount",
                            "_"."agentCountApprox" as "t1.agentCountApprox",
                            "_"."__cubeJoinField" as "t1.__cubeJoinField"
                        from "public"."Logs" "_"
                    ) "$Outer"
                    left outer join (
                        select
                            "_"."taxful_total_price" as "t0.taxful_total_price",
                            "_"."count" as "t0.count",
                            "_"."__cubeJoinField" as "t0.__cubeJoinField"
                        from "public"."KibanaSampleDataEcommerce" "_"
                    ) "$Inner" on ("$Outer"."t1.__cubeJoinField" = "$Inner"."t0.__cubeJoinField")
                ) "rows"
                group by "t1.agentCountApprox"
            ) "_"
            where not "_"."a0" is null
            limit 1000001
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        assert_eq!(
            query_plan.as_logical_plan().find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "Logs.agentCount".to_string(),
                    "Logs.agentCountApprox".to_string(),
                    "KibanaSampleDataEcommerce.count".to_string()
                ]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                join_hints: Some(vec![vec![
                    "Logs".to_string(),
                    "KibanaSampleDataEcommerce".to_string(),
                ],]),
                ..Default::default()
            },
        );
    }

    #[tokio::test]
    async fn test_thoughtspot_count_distinct_with_year_and_month() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                EXTRACT(MONTH FROM "ta_1"."order_date") "ca_1",
                CAST(CAST(((((EXTRACT(YEAR FROM "ta_1"."order_date") * 100) + 1) * 100) + 1) AS varchar) AS date) "ca_2",
                count(DISTINCT "ta_1"."countDistinct") "ca_3"
            FROM "database"."public"."KibanaSampleDataEcommerce" "ta_1"
            GROUP BY
                "ca_1",
                "ca_2"
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                EXTRACT(MONTH FROM "ta_1"."order_date") "ca_1",
                CAST(CAST(((((EXTRACT(YEAR FROM "ta_1"."order_date") * 100) + 1) * 100) + 1) AS varchar) AS date) "ca_2",
                ((((EXTRACT(DAY FROM "ta_1"."order_date") * 100) + 1) * 100) + 1) "ca_3",
                count(DISTINCT "ta_1"."countDistinct") "ca_4",
                count("ta_1"."count") "ca_5"
            FROM "database"."public"."KibanaSampleDataEcommerce" "ta_1"
            GROUP BY
                "ca_1",
                "ca_2",
                "ca_3"
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );

        convert_select_to_query_plan(
            r#"
            SELECT
                EXTRACT(MONTH FROM "ta_1"."order_date") "ca_1",
                count(DISTINCT "ta_1"."countDistinct") "ca_2"
            FROM "database"."public"."KibanaSampleDataEcommerce" "ta_1"
            GROUP BY
                "ca_1"
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;
    }

    #[tokio::test]
    async fn test_cast_to_timestamp_timezone_utc() -> Result<(), CubeError> {
        init_testing_logger();

        insta::assert_snapshot!(
            "test_cast_to_timestamp_timezone_utc_1",
            execute_query(
                "select CAST ('2020-12-25 22:48:48.000' AS timestamptz)".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "test_cast_to_timestamp_timezone_utc_2",
            execute_query(
                "select CAST ('2020-12-25 22:48:48.000' AS timestamp)".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_join_with_distinct() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "test_join_with_distinct",
            execute_query(
                "WITH \"holistics__explore_60963d\" AS (
                    SELECT
                        1 AS \"dm_pu_ca_754b1e\",
                        2 AS \"pu_n_fddcd1\"
                    ), \"holistics__explore_edd38b\" AS (
                    SELECT DISTINCT
                        2 AS \"dm_pu_ca_754b1e\",
                        1 AS \"pu_n_fddcd1\"
                    )
                    SELECT
                        \"holistics__explore_60963d\".\"pu_n_fddcd1\" AS \"pu_n_fddcd1\",
                        \"holistics__explore_edd38b\".\"dm_pu_ca_754b1e\" AS \"dm_pu_ca_754b1e\"
                  FROM
                    \"holistics__explore_60963d\"
                    INNER JOIN \"holistics__explore_edd38b\" ON (\"holistics__explore_60963d\".\"dm_pu_ca_754b1e\" = \"holistics__explore_edd38b\".\"pu_n_fddcd1\");".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_extract_string_field() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "test_extract_string_field",
            execute_query(
                "SELECT EXTRACT('YEAR' FROM CAST ('2020-12-25 22:48:48.000' AS timestamptz))"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_select_is_null_is_not_null() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                (count IS NOT NULL) c,
                (customer_gender IS NULL) g
            FROM KibanaSampleDataEcommerce
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
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_cast_split_aliasing() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            select
                q1.datetrunc_8 datetrunc_8,
                q1.cast_timestamp_to_datetime_10 cast_timestamp_to_datetime_10,
                q1.v_11 v_11
            from (
                select
                    date_trunc('second', "order_date"::timestamptz) datetrunc_8,
                    "order_date"::timestamptz cast_timestamp_to_datetime_10,
                    1 v_11
                from "public"."KibanaSampleDataEcommerce" "KibanaSampleDataEcommerce"
            ) q1
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("second".to_owned()),
                    date_range: None
                }]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_sigma_str_contains() -> Result<(), CubeError> {
        if !Rewriter::sql_push_down_enabled() {
            return Ok(());
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT customer_gender
            FROM KibanaSampleDataEcommerce
            WHERE
                ((position(lower('el') in lower(customer_gender)) > 0) or
                (lower(customer_gender) is null))
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("contains".to_string()),
                            values: Some(vec!["el".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notSet".to_string()),
                            values: None,
                            or: None,
                            and: None,
                        }),
                    ]),
                    and: None,
                }]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sigma_str_not_contains() -> Result<(), CubeError> {
        if !Rewriter::sql_push_down_enabled() {
            return Ok(());
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT customer_gender
            FROM KibanaSampleDataEcommerce
            WHERE
                ((position(lower('ale') in lower(customer_gender)) <= 0) or
                (lower(customer_gender) is null))
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notContains".to_string()),
                            values: Some(vec!["ale".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notSet".to_string()),
                            values: None,
                            or: None,
                            and: None,
                        }),
                    ]),
                    and: None,
                }]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sigma_str_starts_with() -> Result<(), CubeError> {
        if !Rewriter::sql_push_down_enabled() {
            return Ok(());
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT customer_gender
            FROM KibanaSampleDataEcommerce
            WHERE
                ((position(lower('fe') in lower(customer_gender)) = 1) or
                (lower(customer_gender) is null))
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("startsWith".to_string()),
                            values: Some(vec!["fe".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notSet".to_string()),
                            values: None,
                            or: None,
                            and: None,
                        }),
                    ]),
                    and: None,
                }]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sigma_str_not_starts_with() -> Result<(), CubeError> {
        if !Rewriter::sql_push_down_enabled() {
            return Ok(());
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT customer_gender
            FROM KibanaSampleDataEcommerce
            WHERE
                ((position(lower('fe') in lower(customer_gender)) <> 1)
                or (lower(customer_gender) is null))
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notStartsWith".to_string()),
                            values: Some(vec!["fe".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notSet".to_string()),
                            values: None,
                            or: None,
                            and: None,
                        }),
                    ]),
                    and: None,
                }]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sigma_str_ends_with() -> Result<(), CubeError> {
        if !Rewriter::sql_push_down_enabled() {
            return Ok(());
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT customer_gender
            FROM KibanaSampleDataEcommerce
            WHERE
                ((position(reverse(lower('ale')) in reverse(lower(customer_gender))) = 1)
                or (lower(customer_gender) is null))
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("endsWith".to_string()),
                            values: Some(vec!["ale".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notSet".to_string()),
                            values: None,
                            or: None,
                            and: None,
                        }),
                    ]),
                    and: None,
                }]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sigma_str_not_ends_with() -> Result<(), CubeError> {
        if !Rewriter::sql_push_down_enabled() {
            return Ok(());
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT customer_gender
            FROM KibanaSampleDataEcommerce
            WHERE
                ((position(reverse(lower('ale')) in reverse(lower(customer_gender))) <> 1)
                or (lower(customer_gender) is null))
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notEndsWith".to_string()),
                            values: Some(vec!["ale".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notSet".to_string()),
                            values: None,
                            or: None,
                            and: None,
                        }),
                    ]),
                    and: None,
                }]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_union_with_cast_count_to_decimal() -> Result<(), CubeError> {
        init_testing_logger();

        insta::assert_snapshot!(
            "test_union_with_cast_count_to_decimal",
            execute_query(
                "select count(1) from (select 1 a) x union all select cast(null as decimal) order by 1;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sigma_num_range() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT taxful_total_price
            FROM KibanaSampleDataEcommerce
            WHERE (
                (
                    (500 <= taxful_total_price) AND
                    (10000 >= taxful_total_price)
                ) OR
                (taxful_total_price IS NULL)
            )
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: None,
                            operator: None,
                            values: None,
                            or: None,
                            and: Some(vec![
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some(
                                        "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                                    ),
                                    operator: Some("gte".to_string()),
                                    values: Some(vec!["500".to_string()]),
                                    or: None,
                                    and: None,
                                }),
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some(
                                        "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                                    ),
                                    operator: Some("lte".to_string()),
                                    values: Some(vec!["10000".to_string()]),
                                    or: None,
                                    and: None,
                                }),
                            ]),
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some(
                                "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                            ),
                            operator: Some("notSet".to_string()),
                            values: None,
                            or: None,
                            and: None,
                        }),
                    ]),
                    and: None,
                }]),
                ungrouped: Some(true),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_sigma_num_not_in() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT taxful_total_price
            FROM KibanaSampleDataEcommerce
            WHERE (
                NOT (taxful_total_price IN (1, 1.1)) OR
                (taxful_total_price IS NULL)
            )
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some(
                                "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                            ),
                            operator: Some("notEquals".to_string()),
                            values: Some(vec!["1".to_string(), "1.1".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some(
                                "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                            ),
                            operator: Some("notSet".to_string()),
                            values: None,
                            or: None,
                            and: None,
                        }),
                    ]),
                    and: None,
                }]),
                ungrouped: Some(true),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_date_granularity_skyvia() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();
        let supported_granularities = vec![
            // Day
            ("CAST(DATE_TRUNC('day', t.\"order_date\")::date AS varchar)", vec!["day"]),
            // Day of Month
            ("EXTRACT(DAY FROM t.\"order_date\")", vec!["day"]),
            // Month
            ("EXTRACT(YEAR FROM t.\"order_date\")::varchar || ',' || LPAD(EXTRACT(MONTH FROM t.\"order_date\")::varchar, 2, '0')", vec!["year", "month"]),
            // Month of Year
            ("EXTRACT(MONTH FROM t.\"order_date\")", vec!["month"]),
            // Quarter
            ("EXTRACT(YEAR FROM t.\"order_date\")::varchar || ',' || EXTRACT(QUARTER FROM t.\"order_date\")::varchar", vec!["year", "quarter"]),
            // Quarter of Year
            ("EXTRACT(QUARTER FROM t.\"order_date\")", vec!["quarter"]),
            // Year
            (
                "CAST(EXTRACT(YEAR FROM t.\"order_date\") AS varchar)",
                vec!["year"],
            ),
        ];

        for (expr, expected_granularity) in &supported_granularities {
            let logical_plan = convert_select_to_query_plan(
                format!(
                    "SELECT {} AS expr1 FROM public.\"KibanaSampleDataEcommerce\" AS t",
                    expr
                ),
                DatabaseProtocol::PostgreSQL,
            )
            .await
            .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: Some(
                        expected_granularity
                            .iter()
                            .map(|granularity| V1LoadRequestQueryTimeDimension {
                                dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                                granularity: Some(granularity.to_string()),
                                date_range: None,
                            })
                            .collect::<Vec<_>>()
                    ),
                    order: Some(vec![]),
                    ungrouped: Some(true),
                    ..Default::default()
                }
            )
        }

        for (expr, expected_granularity) in supported_granularities {
            let logical_plan = convert_select_to_query_plan(
                format!(
                    "
                    SELECT
                        {} AS expr1,
                        SUM(t.\"count\") AS expr2
                    FROM public.\"KibanaSampleDataEcommerce\" AS t
                    GROUP BY {}
                    ",
                    expr, expr
                ),
                DatabaseProtocol::PostgreSQL,
            )
            .await
            .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: Some(
                        expected_granularity
                            .iter()
                            .map(|granularity| V1LoadRequestQueryTimeDimension {
                                dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                                granularity: Some(granularity.to_string()),
                                date_range: None,
                            })
                            .collect::<Vec<_>>()
                    ),
                    order: Some(vec![]),
                    ..Default::default()
                }
            )
        }
    }

    #[tokio::test]
    async fn test_sigma_literal_relation() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT l1.*
            FROM (
                SELECT
                    "customer_gender",
                    1 as error
                FROM "KibanaSampleDataEcommerce"
            ) as l1
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_limit_push_down_recursion() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            select cast_timestamp_to_datetime_6 "Order Date"
            from (
                select "order_date"::timestamptz cast_timestamp_to_datetime_6
                from (
                    select *
                    from "public"."KibanaSampleDataEcommerce" "KibanaSampleDataEcommerce"
                    limit 10001
                ) q1
                limit 10001
            ) q3
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                limit: Some(10001),
                ungrouped: Some(true),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_push_down_projection_literal() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT cg2 cg3, l2 l3
            FROM (
                SELECT cg1 cg2, l1 l2
                FROM (
                    SELECT cg cg1, l l1
                    FROM (
                        SELECT customer_gender cg, lit l
                        FROM (
                            SELECT customer_gender, 1 lit
                            FROM KibanaSampleDataEcommerce
                        ) k
                    ) k1
                ) k2
            ) k3
            ORDER BY cg3 ASC;
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                ]),
                segments: Some(vec![]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "asc".to_string(),
                ]]),
                ungrouped: Some(true),
                ..Default::default()
            }
        )
    }

    // TODO: unignore once filter push down to projection is implemented
    #[tokio::test]
    #[ignore]
    async fn test_sigma_date_range() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            select count_23 "__Row Count"
            from (
                select count(1) count_23
                from (
                    select *
                    from (
                        select "order_date"::timestamptz cast_timestamp_to_datetime_11
                        from "public"."KibanaSampleDataEcommerce" "KibanaSampleDataEcommerce"
                    ) q1
                    where (
                        (
                            ('2022-11-01T00:00:00+00:00'::timestamptz <= cast_timestamp_to_datetime_11) and
                            ('2022-11-15T23:59:59.999+00:00'::timestamptz >= cast_timestamp_to_datetime_11)
                        ) or
                        (cast_timestamp_to_datetime_11 is null)
                    )
                ) q2
                limit 1001
            ) q4
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2022-11-01T00:00:00.000Z".to_string(),
                        "2022-11-15T23:59:59.999Z".to_string(),
                    ]))
                }]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                    operator: Some("set".to_string()),
                    values: None,
                    or: None,
                    and: None
                },]),
                ..Default::default()
            }
        )
    }

    // TODO: unignore once filter push down to projection is implemented
    #[tokio::test]
    #[ignore]
    async fn test_sigma_date_top_n() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            select cast_timestamp_to_datetime_10 "Order Date"
            from (
                select cast_timestamp_to_datetime_10, isnotnull_11, Rank() over ( order by if_12 desc) "Rank_13" from (
                    select
                        cast_timestamp_to_datetime_10,
                        (cast_timestamp_to_datetime_10 is not null) isnotnull_11,
                        case
                            when (cast_timestamp_to_datetime_10 is not null) then cast_timestamp_to_datetime_10
                        end if_12
                    from (
                        select "order_date"::timestamptz cast_timestamp_to_datetime_10
                        from "public"."KibanaSampleDataEcommerce" "KibanaSampleDataEcommerce"
                    ) q1
                    where (cast_timestamp_to_datetime_10 is not null)
                ) q2
            ) q3
            where
                case
                    when isnotnull_11 then ("Rank_13" <= 3)
                end
            limit 10001
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                    operator: Some("set".to_string()),
                    values: None,
                    or: None,
                    and: None
                },]),
                ..Default::default()
            }
        )
    }

    // TODO: unignore once filter push down to projection is implemented
    #[tokio::test]
    #[ignore]
    async fn test_sigma_date_in_list() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            select cast_timestamp_to_datetime_10 "Order Date"
            from (
                select "order_date"::timestamptz cast_timestamp_to_datetime_10
                from "public"."KibanaSampleDataEcommerce" "KibanaSampleDataEcommerce"
            ) q1
            where cast_timestamp_to_datetime_10 in (
                '2019-01-17T15:25:48+00:00'::timestamptz,
                '2019-09-09T00:00:00+00:00'::timestamptz
            )
            limit 10001
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                limit: Some(10001),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec![
                        "2019-01-17T15:25:48.000Z".to_string(),
                        "2019-09-09T00:00:00.000Z".to_string(),
                    ]),
                    or: None,
                    and: None
                },]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_approximate_count_distinct() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT approximate count(distinct "ta_1"."customer_gender") "ca_1"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_count_distinct_text() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT count(distinct "ta_1"."customer_gender") "ca_1"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_like_with_escape() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT "ta_1"."customer_gender" "ca_1"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
            WHERE LOWER("ta_1"."customer_gender") LIKE ('%' || replace(
                replace(
                    replace(
                        'male',
                        '!',
                        '!!'
                    ),
                    '%',
                    '!%'
                ),
                '_',
                '!_'
            ) || '%') ESCAPE '!'
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC
            LIMIT 1000
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "asc".to_string(),
                ]]),
                limit: Some(1000),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("contains".to_string()),
                    values: Some(vec!["male".to_string()]),
                    or: None,
                    and: None
                }]),
                ..Default::default()
            }
        );

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT "ta_1"."customer_gender" "ca_1"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
            WHERE NOT(LOWER("ta_1"."customer_gender") LIKE (replace(
                replace(
                    replace(
                        'test',
                        '!',
                        '!!'
                    ),
                    '%',
                    '!%'
                ),
                '_',
                '!_'
            ) || '%') ESCAPE '!')
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC
            LIMIT 1000
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "asc".to_string(),
                ]]),
                limit: Some(1000),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("notStartsWith".to_string()),
                    values: Some(vec!["test".to_string()]),
                    or: None,
                    and: None
                }]),
                ..Default::default()
            }
        );

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT "ta_1"."customer_gender" "ca_1"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
            WHERE NOT(LOWER("ta_1"."customer_gender") LIKE ('%' || replace(
                replace(
                    replace(
                        'known',
                        '!',
                        '!!'
                    ),
                    '%',
                    '!%'
                ),
                '_',
                '!_'
            )) ESCAPE '!')
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC
            LIMIT 1000
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "asc".to_string(),
                ]]),
                limit: Some(1000),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("notEndsWith".to_string()),
                    values: Some(vec!["known".to_string()]),
                    or: None,
                    and: None
                }]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_exclude_single_filter() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT "ta_1"."customer_gender" "ca_1"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
            WHERE (
                LOWER("ta_1"."customer_gender") <> 'male'
                OR "ta_1"."customer_gender" IS NULL
            )
            GROUP BY "ca_1"
            LIMIT 1000
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;

        // check wrapping for `LOWER(..) <> .. OR .. IS NULL`
        let re = Regex::new(r"LOWER ?\(.+\) != .+ OR .+ IS NULL").unwrap();
        assert!(re.is_match(&sql));
    }

    #[tokio::test]
    async fn test_thoughtspot_exclude_multiple_filter() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT "ta_1"."customer_gender" "ca_1"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
            WHERE (
            NOT(LOWER("ta_1"."customer_gender") IN (
                'male', 'female'
            ))
            OR NOT("ta_1"."customer_gender" IS NOT NULL)
            )
            GROUP BY "ca_1"
            LIMIT 1000
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;

        // check wrapping for `NOT(LOWER(..) IN (..))`
        let re = Regex::new(r"NOT.+LOWER ?\(.+\).* IN ").unwrap();
        assert!(re.is_match(&sql));
    }

    #[tokio::test]
    async fn test_segment_post_aggr() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT is_male is_male, SUBSTRING(customer_gender FROM 1 FOR 1234) gender
            FROM KibanaSampleDataEcommerce
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_select_from_cube_case() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                CASE
                    WHEN notes IS NULL THEN customer_gender
                    ELSE notes
                END customer_info
            FROM KibanaSampleDataEcommerce
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.notes".to_string(),
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_select_from_cube_case_with_group_by() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                CASE
                    WHEN notes IS NULL THEN customer_gender
                    ELSE notes
                END customer_info,
                COUNT(*) count
            FROM KibanaSampleDataEcommerce
            GROUP BY 1
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
                    "KibanaSampleDataEcommerce.notes".to_string(),
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_select_from_cube_case_with_expr() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                CASE customer_gender
                    WHEN 'f' THEN 'Female'
                    WHEN 'm' THEN 'Male'
                    ELSE CASE
                        WHEN notes IS NULL THEN 'Other'
                        ELSE notes
                    END
                END customer_gender
            FROM KibanaSampleDataEcommerce
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

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
                ungrouped: Some(true),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_select_from_cube_case_with_expr_and_group_by() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                CASE customer_gender
                    WHEN 'f' THEN 'Female'
                    WHEN 'm' THEN 'Male'
                    ELSE CASE
                        WHEN notes IS NULL THEN 'Other'
                        ELSE notes
                    END
                END customer_gender,
                COUNT(*) count
            FROM KibanaSampleDataEcommerce
            GROUP BY 1
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
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "KibanaSampleDataEcommerce.notes".to_string(),
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_select_case_is_null() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                CASE
                    WHEN "ta_1"."customer_gender" IS NULL
                    THEN "ta_1"."notes"
                    ELSE "ta_1"."customer_gender"
                END "ca_1"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
            GROUP BY "ca_1"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

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
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_select_case_when_true() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT CASE
                WHEN TRUE THEN "ta_1"."customer_gender"
                ELSE CASE
                    WHEN "ta_1"."customer_gender" IS NOT NULL THEN "ta_1"."customer_gender"
                    ELSE "ta_1"."notes"
                END
            END "ca_1"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
            GROUP BY "ca_1"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

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
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_lower() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            WITH "qt_0" AS (
                SELECT "ta_1"."notes" "ca_1"
                FROM KibanaSampleDataEcommerce "ta_1"
                WHERE (
                    NOT(LOWER("ta_1"."customer_gender") IN (
                        'f', 'm'
                    ))
                    OR NOT("ta_1"."customer_gender" IS NOT NULL)
                )
                GROUP BY "ca_1"
            )
            SELECT count(DISTINCT "ta_2"."ca_1") "ca_2"
            FROM "qt_0" "ta_2"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;

        // check wrapping for `NOT(LOWER(..) IN (..)) OR NOT(.. IS NOT NULL)`
        let re = Regex::new(r"NOT.+LOWER ?\(.+\) IN .+\) OR NOT.+ IS NOT NULL").unwrap();
        assert!(re.is_match(&sql));
    }

    #[tokio::test]
    async fn test_thoughtspot_having_cast_float8() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            WITH "qt_0" AS (
                SELECT "ta_1"."customer_gender" "ca_1"
                FROM "KibanaSampleDataEcommerce" "ta_1"
                GROUP BY "ca_1"
                HAVING CAST(COUNT("ta_1"."count") AS FLOAT8) < 10.0
            )
            SELECT count(DISTINCT "ta_2"."ca_1") "ca_2"
            FROM "qt_0" "ta_2"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.count".to_string()),
                    operator: Some("lt".to_string()),
                    values: Some(vec!["10".to_string()]),
                    or: None,
                    and: None
                }]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_avg_cast_arg() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "ta_1"."customer_gender" "ca_1",
                avg(CAST("ta_1"."avgPrice" AS FLOAT8)) "ca_2"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            ORDER BY
                "ca_2" DESC,
                "ca_1" ASC
            LIMIT 2
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_concat() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            WITH "qt_0" AS (
                SELECT
                    ("ta_1"."customer_gender" || 'aa') "ca_1"
                FROM KibanaSampleDataEcommerce "ta_1"
                GROUP BY "ca_1"
            )
            SELECT "ca_1" "ca_2"
            FROM "qt_0" "ta_2"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_extract_equals() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT (EXTRACT(DAY FROM "ta_1"."order_date") = 15.0) "ca_1"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: None
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_extract_month_of_quarter() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                (((EXTRACT(MONTH FROM "ta_1"."order_date") - 1) % 3) + 1) "ca_1",
                CASE
                    WHEN sum("ta_1"."count") IS NOT NULL THEN sum("ta_1"."count")
                    ELSE 0
                END "ca_2"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            ORDER BY
                "ca_1" ASC,
                "ca_2" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: None
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_extract_lt_extract() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT (EXTRACT(MONTH FROM "ta_1"."order_date") < (EXTRACT(MONTH FROM "ta_1"."last_mod") + 1.0)) "ca_1"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![
                    V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some("month".to_string()),
                        date_range: None
                    },
                    V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.last_mod".to_string(),
                        granularity: Some("month".to_string()),
                        date_range: None
                    },
                ]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_select_eq_or() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT ((
                LOWER("ta_1"."customer_gender") = 'male'
                OR LOWER("ta_1"."customer_gender") = 'female'
            )) "ca_1"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }
        );

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT ((
                LOWER("ta_1"."customer_gender") = 'female'
                OR LOWER("ta_1"."notes") = 'test'
            )) "ca_1"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

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
                ..Default::default()
            }
        );

        if !Rewriter::sql_push_down_enabled() {
            return;
        }

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT ((
                "ta_1"."order_date" = DATE '1994-05-01'
                OR "ta_1"."order_date" = DATE '1996-05-03'
            )) "ca_1"
            FROM KibanaSampleDataEcommerce "ta_1"
                JOIN Logs "ta_2"
                    ON "ta_1"."__cubeJoinField" = "ta_2"."__cubeJoinField"
            WHERE LOWER("ta_2"."content") = 'test'
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan
                .find_cube_scan_wrapped_sql()
                .request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    json!({
                        "cubeName": "KibanaSampleDataEcommerce",
                        "alias": "ta_1_order_date_",
                        "expr": {
                            "type": "SqlFunction",
                            "cubeParams": ["KibanaSampleDataEcommerce"],
                            "sql": "((${KibanaSampleDataEcommerce.order_date} = DATE('1994-05-01')) OR (${KibanaSampleDataEcommerce.order_date} = DATE('1996-05-03')))",
                        },
                        "groupingSet": null,
                    }).to_string(),
                ]),
                segments: Some(vec![
                    json!({
                        "cubeName": "Logs",
                        "alias": "lower_ta_2_conte",
                        "expr": {
                            "type": "SqlFunction",
                            "cubeParams": ["Logs"],
                            "sql": "(LOWER(${Logs.content}) = $0$)",
                        },
                        "groupingSet": null,
                    }).to_string(),
                ]),
                order: Some(vec![]),
                join_hints: Some(vec![
                    vec![
                        "KibanaSampleDataEcommerce".to_string(),
                        "Logs".to_string(),
                    ],
                ]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_thoughtspot_column_comparison() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT ("ta_1"."taxful_total_price" > 10.0) "ca_1"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_date_trunc_month_year() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT DATE_TRUNC('month', DATE_TRUNC('month', "ta_1"."order_date")) "ca_1"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: None
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "asc".to_string()
                ]]),
                ..Default::default()
            }
        );

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT DATE_TRUNC('month', CAST(CAST(((((EXTRACT(YEAR FROM "ta_1"."order_date") * 100) + 1) * 100) + 1) AS CHARACTER VARYING) AS timestamp)) "ca_1"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("year".to_string()),
                    date_range: None
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "asc".to_string()
                ]]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_day_in_quarter() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                (DATEDIFF(day, DATEADD(month, CAST((((((EXTRACT(MONTH FROM "ta_1"."order_date") - 1) % 3) + 1) - 1) * -1) AS int), CAST(CAST(((((EXTRACT(YEAR FROM "ta_1"."order_date") * 100) + EXTRACT(MONTH FROM "ta_1"."order_date")) * 100) + 1) AS varchar) AS date)), "ta_1"."order_date") + 1) "ca_1",
                CASE
                    WHEN sum("ta_1"."count") IS NOT NULL THEN sum("ta_1"."count")
                    ELSE 0
                END "ca_2"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            ORDER BY
                "ca_1" ASC,
                "ca_2" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: None
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_date_trunc_offset() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                DATE_TRUNC('qtr', DATEADD(day, CAST(2 AS int), "ta_1"."order_date")) "ca_1",
                CASE
                    WHEN sum("ta_1"."count") IS NOT NULL THEN sum("ta_1"."count")
                    ELSE 0
                END "ca_2"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            ORDER BY
                "ca_1" ASC,
                "ca_2" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                DATE_TRUNC('qtr', DATEADD(week, CAST(5 AS int), "ta_1"."order_date")) "ca_1",
                CASE
                    WHEN sum("ta_1"."count") IS NOT NULL THEN sum("ta_1"."count")
                    ELSE 0
                END "ca_2"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            ORDER BY
                "ca_1" ASC,
                "ca_2" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_date_offset_with_filter() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            WITH "qt_0" AS (
                SELECT
                    "ta_1"."customer_gender" "ca_1",
                    CAST(DATEADD(day, CAST(2 AS int), "ta_1"."order_date") AS date) "ca_2",
                    DATEADD(second, CAST(2000 AS int), "ta_1"."order_date") "ca_3"
                FROM KibanaSampleDataEcommerce "ta_1"
                WHERE DATEADD(day, CAST(2 AS int), "ta_1"."order_date") < DATE '2014-06-02'
                GROUP BY
                    "ca_1",
                    "ca_2",
                    "ca_3"
            )
            SELECT
                min("ta_2"."ca_2") "ca_3",
                max("ta_2"."ca_2") "ca_4"
            FROM "qt_0" "ta_2"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        assert_eq!(
            query_plan
                .as_logical_plan()
                .find_cube_scan_wrapped_sql()
                .request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    json!({
                        "cubeName": "KibanaSampleDataEcommerce",
                        "alias": "customer_gender",
                        "expr": {
                            "type": "SqlFunction",
                            "cubeParams": ["KibanaSampleDataEcommerce"],
                            "sql": "${KibanaSampleDataEcommerce.customer_gender}",
                        },
                        "groupingSet": null,
                    }).to_string(),
                    json!({
                        "cubeName": "KibanaSampleDataEcommerce",
                        "alias": "cast_dateadd_utf",
                        "expr": {
                            "type": "SqlFunction",
                            "cubeParams": ["KibanaSampleDataEcommerce"],
                            "sql": "CAST(DATE_ADD(${KibanaSampleDataEcommerce.order_date}, INTERVAL '2 DAY') AS DATE)",
                        },
                        "groupingSet": null,
                    }).to_string(),
                    json!({
                        "cubeName": "KibanaSampleDataEcommerce",
                        "alias": "dateadd_utf8__se",
                        "expr": {
                            "type": "SqlFunction",
                            "cubeParams": ["KibanaSampleDataEcommerce"],
                            "sql": "DATE_ADD(${KibanaSampleDataEcommerce.order_date}, INTERVAL '2000000 MILLISECOND')",
                        },
                        "groupingSet": null,
                    }).to_string(),
                ]),
                segments: Some(vec![
                    json!({
                        "cubeName": "KibanaSampleDataEcommerce",
                        "alias": "dateadd_utf8__da",
                        "expr": {
                            "type": "SqlFunction",
                            "cubeParams": ["KibanaSampleDataEcommerce"],
                            "sql": "(DATE_ADD(${KibanaSampleDataEcommerce.order_date}, INTERVAL '2 DAY') < DATE('2014-06-02'))",
                        },
                        "groupingSet": null,
                    }).to_string(),
                ]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_min_max_date_offset() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                min(DATEADD(day, CAST(2 AS int), "ta_1"."order_date")) "ca_1",
                max(DATEADD(day, CAST(2 AS int), "ta_1"."order_date")) "ca_2"
            FROM KibanaSampleDataEcommerce "ta_1"
            ORDER BY
                "ca_1" ASC,
                "ca_2" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_week_num_in_month() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                FLOOR(((EXTRACT(DAY FROM DATEADD(day, CAST((4 - (((DATEDIFF(day, DATE '1970-01-01', "ta_1"."order_date") + 3) % 7) + 1)) AS int), "ta_1"."order_date")) + 6) / NULLIF(CAST(7 AS FLOAT8),0.0))) "ca_1",
                CASE
                    WHEN sum("ta_1"."count") IS NOT NULL THEN sum("ta_1"."count")
                    ELSE 0
                END "ca_2"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            ORDER BY
                "ca_1" ASC,
                "ca_2" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("week".to_string()),
                    date_range: None
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_binary_sum_columns() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            WITH "qt_0" AS (
                SELECT
                    ("ta_2"."taxful_total_price" + "ta_1"."id") "ca_1",
                    CASE
                        WHEN sum("ta_2"."count") IS NOT NULL THEN sum("ta_2"."count")
                        ELSE 0
                    END "ca_2"
                FROM KibanaSampleDataEcommerce "ta_2"
                JOIN Logs "ta_1"
                    ON "ta_2"."__cubeJoinField" = "ta_1"."__cubeJoinField"
                GROUP BY "ca_1"
            )
            SELECT count(DISTINCT "ta_3"."ca_1") "ca_3"
            FROM "qt_0" "ta_3"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    "Logs.id".to_string(),
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                join_hints: Some(vec![vec![
                    "KibanaSampleDataEcommerce".to_string(),
                    "Logs".to_string(),
                ],]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_date_trunc_qtr_with_post_processing() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                DATE_TRUNC('qtr', DATEADD(minute, CAST(2 AS int), "ta_1"."order_date")) "ca_1",
                DATE_TRUNC('qtr', "ta_1"."order_date") "ca_2"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY
                "ca_1",
                "ca_2"
            ORDER BY
                "ca_1" ASC,
                "ca_2" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_split_date_trunc_qtr() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
             SELECT
                 TO_CHAR("ta_1"."order_date", 'Mon') "ca_1",
                 DATE_TRUNC('qtr', "ta_1"."order_date") "ca_2"
             FROM KibanaSampleDataEcommerce "ta_1"
             GROUP BY
                 "ca_1",
                 "ca_2"
             "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("quarter".to_owned()),
                    date_range: None
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_extract_quarter() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            WITH "qt_0" AS (
                SELECT
                    CEIL((EXTRACT(MONTH FROM "ta_1"."order_date") / NULLIF(3.0,0.0))) "ca_1",
                    CASE
                        WHEN sum("ta_1"."count") IS NOT NULL THEN sum("ta_1"."count")
                        ELSE 0
                    END "ca_2"
                FROM KibanaSampleDataEcommerce "ta_1"
                GROUP BY "ca_1"
            )
            SELECT count(DISTINCT "ta_2"."ca_1") "ca_3"
            FROM "qt_0" "ta_2"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: None
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_where_not_or() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            WITH "qt_0" AS (
                SELECT "ta_1"."customer_gender" "ca_1"
                FROM KibanaSampleDataEcommerce "ta_1"
                WHERE NOT((
                    "ta_1"."customer_gender" IS NULL
                    OR LOWER("ta_1"."customer_gender") IN ('unknown')
                ))
                GROUP BY "ca_1"
            )
            SELECT count(DISTINCT "ta_2"."ca_1") "ca_2"
            FROM "qt_0" "ta_2"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();

        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;

        // check wrapping for `NOT((.. IS NULL) OR LOWER(..) IN)`
        let re = Regex::new(r"NOT \(\(.+ IS NULL\) OR .*LOWER\(.+ IN ").unwrap();
        assert!(re.is_match(&sql));
    }

    #[tokio::test]
    async fn test_thoughtspot_where_binary_in_true_false() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                ((
                    LOWER("ta_1"."customer_gender") = 'female'
                    OR LOWER("ta_1"."customer_gender") = 'male'
                )) "ca_1",
                CASE
                    WHEN sum("ta_1"."count") IS NOT NULL THEN sum("ta_1"."count")
                    ELSE 0
                END "ca_2"
            FROM KibanaSampleDataEcommerce "ta_1"
            WHERE ((
                LOWER("ta_1"."customer_gender") = 'female'
                OR LOWER("ta_1"."customer_gender") = 'male'
            )) IN (
                TRUE, FALSE
            )
            GROUP BY "ca_1"
            ORDER BY
                "ca_1" ASC,
                "ca_2" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        // check if contains `(LOWER(..) = .. OR ..LOWER(..) = ..) IN (TRUE, FALSE)`
        let re = Regex::new(r"\(LOWER ?\(.+\) = .+ OR .+LOWER ?\(.+\) = .+\) IN \(TRUE, FALSE\)")
            .unwrap();

        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;

        assert!(re.is_match(&sql));
    }

    #[tokio::test]
    async fn test_thoughtspot_left_right() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "ta_1"."customer_gender" "ca_1",
                LEFT("ta_1"."customer_gender", 2) "ca_2",
                RIGHT("ta_1"."customer_gender", 2) "ca_3"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY
                "ca_1",
                "ca_2",
                "ca_3"
            ORDER BY
                "ca_1" ASC,
                "ca_2" ASC,
                "ca_3" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_nullif_measure_dimension() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                NULLIF(CAST("ta_1"."taxful_total_price" AS FLOAT8), 0.0) "ca_1",
                NULLIF(CAST("ta_1"."count" AS FLOAT8), 0.0) "ca_2"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY
                "ca_1",
                "ca_2"
            ORDER BY
                "ca_1" ASC,
                "ca_2" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_datediff_to_date() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            WITH "qt_0" AS (
                SELECT
                    DATEDIFF(day, min("ta_1"."order_date"), TO_DATE('2020-02-20','YYYY-MM-DD')) "ca_1",
                    min("ta_1"."order_date") "ca_2"
                FROM KibanaSampleDataEcommerce "ta_1"
                HAVING DATEDIFF(day, min("ta_1"."order_date"), TO_DATE('2020-02-20','YYYY-MM-DD')) > 4
            )
            SELECT DATEDIFF(day, min("ta_2"."ca_2"), TO_DATE('2020-02-20','YYYY-MM-DD')) "ca_3"
            FROM "qt_0" "ta_2"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        assert_eq!(
            query_plan.as_logical_plan().find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_thoughtspot_filter_date_trunc_column_with_literal() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let test_data = vec![
            // (operator, literal date, filter operator, filter value)
            (
                ">=",
                "2020-03-25",
                "afterOrOnDate",
                "2020-04-01T00:00:00.000Z",
            ),
            (
                ">=",
                "2020-04-01",
                "afterOrOnDate",
                "2020-04-01T00:00:00.000Z",
            ),
            (
                ">=",
                "2020-04-10",
                "afterOrOnDate",
                "2020-05-01T00:00:00.000Z",
            ),
            ("<=", "2020-03-25", "beforeDate", "2020-04-01T00:00:00.000Z"),
            ("<=", "2020-04-01", "beforeDate", "2020-05-01T00:00:00.000Z"),
            ("<=", "2020-04-10", "beforeDate", "2020-05-01T00:00:00.000Z"),
            (
                ">",
                "2020-03-25",
                "afterOrOnDate",
                "2020-04-01T00:00:00.000Z",
            ),
            (
                ">",
                "2020-04-01",
                "afterOrOnDate",
                "2020-05-01T00:00:00.000Z",
            ),
            (
                ">",
                "2020-04-10",
                "afterOrOnDate",
                "2020-05-01T00:00:00.000Z",
            ),
            ("<", "2020-03-25", "beforeDate", "2020-04-01T00:00:00.000Z"),
            ("<", "2020-04-01", "beforeDate", "2020-04-01T00:00:00.000Z"),
            ("<", "2020-04-10", "beforeDate", "2020-05-01T00:00:00.000Z"),
        ];

        for (operator, literal_date, filter_operator, filter_value) in test_data {
            let logical_plan = convert_select_to_query_plan(
                format!(
                    "
                    SELECT
                        \"ta_1\".\"order_date\" \"ca_1\"
                    FROM KibanaSampleDataEcommerce \"ta_1\"
                    WHERE DATE_TRUNC('MONTH', CAST(\"ta_1\".\"order_date\" as TIMESTAMP)) {} to_date('{}', 'yyyy-MM-dd')
                    ",
                    operator, literal_date,
                ),
                DatabaseProtocol::PostgreSQL,
            )
            .await
            .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                    segments: Some(vec![]),
                    order: Some(vec![]),
                    filters: Some(vec![V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                        operator: Some(filter_operator.to_string()),
                        values: Some(vec![filter_value.to_string()]),
                        or: None,
                        and: None
                    }]),
                    ungrouped: Some(true),
                    ..Default::default()
                }
            );
        }
    }

    #[tokio::test]
    async fn test_thoughtspot_double_date_trunc_with_cast() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                DATE_TRUNC('MONTH', CAST(DATE_TRUNC('MONTH', CAST("ta_1"."order_date" as TIMESTAMP)) as TIMESTAMP)) AS "ca_1"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: None
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_metabase_substring_postaggr() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                avgPrice avgPrice,
                countDistinct countDistinct,
                customer_gender customer_gender,
                SUBSTRING(customer_gender FROM 1 FOR 1234) substring_400
            FROM KibanaSampleDataEcommerce
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
                    "KibanaSampleDataEcommerce.avgPrice".to_string(),
                    "KibanaSampleDataEcommerce.countDistinct".to_string(),
                ]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_reaggregate_without_aliases() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                EXTRACT(YEAR FROM order_date),
                CHAR_LENGTH(customer_gender),
                count
            FROM KibanaSampleDataEcommerce
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("year".to_string()),
                    date_range: None,
                }]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_extract_year_to_date_trunc() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                CAST(CAST(((((EXTRACT(YEAR FROM "ta_1"."order_date") * 100) + 1) * 100) + 1) AS varchar) AS date) "ca_1",
                count(DISTINCT "ta_1"."countDistinct") "ca_2"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
            GROUP BY "ca_1"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.countDistinct".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("year".to_string()),
                    date_range: None,
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    // TODO: __cubeJoinField for WrappedSelect
    #[ignore]
    #[tokio::test]
    async fn test_sigma_row_count_cross_join() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                count_25 "__Row Count",
                datetrunc_8 "Second of Order Date",
                cast_timestamp_to_datetime_10 "Order Date",
                v_11 "Target Const"
            FROM (
                SELECT
                    q1.datetrunc_8 datetrunc_8,
                    q1.cast_timestamp_to_datetime_10 cast_timestamp_to_datetime_10,
                    q1.v_11 v_11,
                    q2.count_25 count_25
                FROM (
                    SELECT
                        date_trunc('second', "order_date"::timestamptz) datetrunc_8,
                        "order_date"::timestamptz cast_timestamp_to_datetime_10,
                        1 v_11
                    FROM "public"."KibanaSampleDataEcommerce" "KibanaSampleDataEcommerce"
                ) q1
                CROSS JOIN (
                    SELECT count(1) count_25
                    FROM "public"."KibanaSampleDataEcommerce" "KibanaSampleDataEcommerce"
                ) q2
                ORDER BY q1.datetrunc_8 ASC
                LIMIT 10001
            ) q5;
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let cube_scans = logical_plan
            .find_cube_scans()
            .iter()
            .map(|cube| cube.request.clone())
            .collect::<Vec<V1LoadRequestQuery>>();

        assert!(cube_scans.contains(&V1LoadRequestQuery {
            measures: Some(vec![]),
            dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
            segments: Some(vec![]),
            time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                granularity: Some("second".to_string()),
                date_range: None,
            }]),
            // Order and Limit and nearly pushed to CubeScan but the Projection
            // before TableScan is a post-processing projection.
            // Splitting such projections into two may be a good idea.
            order: Some(vec![]),
            ungrouped: Some(true),
            ..Default::default()
        }))
    }

    #[tokio::test]
    async fn test_metabase_cast_column_to_date() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT
                CAST("public"."KibanaSampleDataEcommerce"."order_date" AS DATE) AS "order_date",
                avg("public"."KibanaSampleDataEcommerce"."avgPrice") AS "avgPrice"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE (
                "public"."KibanaSampleDataEcommerce"."order_date" >= CAST((now() + (INTERVAL '-30 day')) AS DATE)
                AND "public"."KibanaSampleDataEcommerce"."order_date" < CAST(now() AS DATE)
                AND (
                    "public"."KibanaSampleDataEcommerce"."notes" = 'note1'
                    OR "public"."KibanaSampleDataEcommerce"."notes" = 'note2'
                    OR "public"."KibanaSampleDataEcommerce"."notes" = 'note3'
                )
            )
            GROUP BY CAST("public"."KibanaSampleDataEcommerce"."order_date" AS DATE)
            ORDER BY CAST("public"."KibanaSampleDataEcommerce"."order_date" AS DATE) ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan_wrapped_sql().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    json!({
                        "cubeName": "KibanaSampleDataEcommerce",
                        "alias": "avg_kibanasample",
                        "expr": {
                            "type": "SqlFunction",
                            "cubeParams": ["KibanaSampleDataEcommerce"],
                            "sql": "${KibanaSampleDataEcommerce.avgPrice}",
                        },
                        "groupingSet": null,
                    }).to_string(),
                ]),
                dimensions: Some(vec![
                    json!({
                        "cubeName": "KibanaSampleDataEcommerce",
                        "alias": "cast_kibanasampl",
                        "expr": {
                            "type": "SqlFunction",
                            "cubeParams": ["KibanaSampleDataEcommerce"],
                            "sql": "CAST(${KibanaSampleDataEcommerce.order_date} AS DATE)",
                        },
                        "groupingSet": null,
                    }).to_string(),
                ]),
                segments: Some(vec![
                    json!({
                        "cubeName": "KibanaSampleDataEcommerce",
                        "alias": "kibanasampledata",
                        "expr": {
                            "type": "SqlFunction",
                            "cubeParams": ["KibanaSampleDataEcommerce"],
                            "sql": format!("(((${{KibanaSampleDataEcommerce.order_date}} >= CAST((NOW() + INTERVAL '-30 DAY') AS DATE)) AND (${{KibanaSampleDataEcommerce.order_date}} < CAST(NOW() AS DATE))) AND (((${{KibanaSampleDataEcommerce.notes}} = $0$) OR (${{KibanaSampleDataEcommerce.notes}} = $1$)) OR (${{KibanaSampleDataEcommerce.notes}} = $2$)))"),
                        },
                        "groupingSet": null,
                    }).to_string(),
                ]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_date_trunc_column_equals_literal() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                avg("avgPrice") AS "avgPrice"
            FROM public."KibanaSampleDataEcommerce"
            WHERE
                DATE_TRUNC('week', "order_date") = str_to_date('2022-11-14 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.avgPrice".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2022-11-14T00:00:00.000Z".to_string(),
                        "2022-11-20T23:59:59.999Z".to_string(),
                    ]))
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_date_trunc_column_not_equals_literal() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                avg("avgPrice") AS "avgPrice"
            FROM public."KibanaSampleDataEcommerce"
            WHERE
                DATE_TRUNC('week', "order_date") != str_to_date('2022-11-14 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.avgPrice".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    or: Some(vec![
                        json!({
                            "member": "KibanaSampleDataEcommerce.order_date",
                            "operator": "beforeDate",
                            "values": ["2022-11-14T00:00:00.000Z"],
                        }),
                        json!({
                            "member": "KibanaSampleDataEcommerce.order_date",
                            "operator": "afterOrOnDate",
                            "values": ["2022-11-21T00:00:00.000Z"],
                        }),
                    ]),
                    ..Default::default()
                }]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_psqlodbc_null() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "psqlodbc_null",
            execute_query(
                "select NULL, NULL, NULL".to_string(),
                DatabaseProtocol::PostgreSQL,
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_where_push_down() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "customer_gender" AS "uuid.customer_gender",
                COUNT(*) AS "count"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE CAST(LEFT(RIGHT("customer_gender", 2), 1) AS TEXT) = 'le'
            GROUP BY "customer_gender";
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        assert!(logical_plan
            .find_cube_scan_wrapped_sql()
            .wrapped_sql
            .sql
            .contains("LEFT"));
    }

    #[tokio::test]
    async fn test_extract_epoch_from_dimension() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let context = TestContext::new(DatabaseProtocol::PostgreSQL).await;

        let expected_cube_scan = V1LoadRequestQuery {
            measures: Some(vec![]),
            segments: Some(vec![]),
            dimensions: Some(vec!["MultiTypeCube.dim_date0".to_string()]),
            order: Some(vec![]),
            ..Default::default()
        };

        context
            .add_cube_load_mock(
                expected_cube_scan.clone(),
                simple_load_response(vec![
                    json!({"MultiTypeCube.dim_date0": "2024-12-31T01:02:03.500"}),
                ]),
            )
            .await;

        // "extract(EPOCH FROM dim_date0)" expression gets typed Int32 in schema by DF, but executed as Float64
        // https://github.com/apache/datafusion/blob/e088945c38b74bb1d86dcbb88a69dfc21d59e375/datafusion/functions/src/datetime/date_part.rs#L131-L133
        // https://github.com/cube-js/arrow-datafusion/blob/a78e52154e63bed2b7546bb250959239b020036f/datafusion/expr/src/function.rs#L126-L133
        // Without + 0.0 execution will fail with "column types must match schema types, expected Int32 but found Float64 at column index 0"
        // TODO Remove + 0.0 on fresh DF

        // language=PostgreSQL
        let query = r#"
            SELECT EXTRACT(EPOCH FROM dim_date0) + 0.0 AS result
            FROM MultiTypeCube
            GROUP BY 1
        "#;

        assert_eq!(
            context
                .convert_sql_to_cube_query(&query)
                .await
                .unwrap()
                .as_logical_plan()
                .find_cube_scan()
                .request,
            expected_cube_scan
        );

        // Expect proper epoch in floating point
        insta::assert_snapshot!(context.execute_query(query).await.unwrap());
    }

    #[tokio::test]
    async fn test_extract_granularity_from_dimension() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let context = TestContext::new(DatabaseProtocol::PostgreSQL).await;

        // This date should be idempotent for every expected granularity, so mocked response would stay correct
        // At the same time, it should generate different extractions for different tokens
        let base_date = "2024-10-01T00:00:00.000Z";

        // TODO qtr is not supported in EXTRACT for now, probably in sqlparser
        let tokens = [
            ("day", "day"),
            ("dow", "day"),
            ("doy", "day"),
            ("quarter", "quarter"),
            // ("qtr", "quarter"),
        ];

        for (token, expected_granularity) in tokens {
            // language=PostgreSQL
            let query = format!(
                r#"
                SELECT EXTRACT({token} FROM dim_date0) AS result
                FROM MultiTypeCube
                GROUP BY 1
            "#
            );

            let expected_cube_scan = V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "MultiTypeCube.dim_date0".to_string(),
                    granularity: Some(expected_granularity.to_string()),
                    date_range: None,
                }]),
                order: Some(vec![]),
                ..Default::default()
            };

            context
                .add_cube_load_mock(
                    expected_cube_scan.clone(),
                    simple_load_response(vec![
                        json!({format!("MultiTypeCube.dim_date0.{expected_granularity}"): base_date}),
                    ]),
                )
                .await;

            assert_eq!(
                context
                    .convert_sql_to_cube_query(&query)
                    .await
                    .unwrap()
                    .as_logical_plan()
                    .find_cube_scan()
                    .request,
                expected_cube_scan
            );

            // Expect different values for different tokens
            insta::assert_snapshot!(
                format!("extract_{token}_from_dimension"),
                context.execute_query(query).await.unwrap()
            );
        }
    }

    #[tokio::test]
    async fn test_date_part_epoch_from_dimension() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let context = TestContext::new(DatabaseProtocol::PostgreSQL).await;

        let expected_cube_scan = V1LoadRequestQuery {
            measures: Some(vec![]),
            segments: Some(vec![]),
            dimensions: Some(vec!["MultiTypeCube.dim_date0".to_string()]),
            time_dimensions: None,
            order: Some(vec![]),
            ..Default::default()
        };

        context
            .add_cube_load_mock(
                expected_cube_scan.clone(),
                simple_load_response(vec![
                    json!({"MultiTypeCube.dim_date0": "2024-12-31T01:02:03.500"}),
                ]),
            )
            .await;

        // "extract(EPOCH FROM dim_date0)" expression gets typed Int32 in schema by DF, but executed as Float64
        // https://github.com/apache/datafusion/blob/e088945c38b74bb1d86dcbb88a69dfc21d59e375/datafusion/functions/src/datetime/date_part.rs#L131-L133
        // https://github.com/cube-js/arrow-datafusion/blob/a78e52154e63bed2b7546bb250959239b020036f/datafusion/expr/src/function.rs#L126-L133
        // Without + 0.0 execution will fail with "column types must match schema types, expected Int32 but found Float64 at column index 0"
        // TODO Remove + 0.0 on fresh DF

        // language=PostgreSQL
        let query = r#"
            SELECT date_part('epoch', dim_date0) + 0.0 AS result
            FROM MultiTypeCube
            GROUP BY 1
        "#;

        assert_eq!(
            context
                .convert_sql_to_cube_query(&query)
                .await
                .unwrap()
                .as_logical_plan()
                .find_cube_scan()
                .request,
            expected_cube_scan
        );

        // Expect proper epoch in floating point
        insta::assert_snapshot!(context.execute_query(query).await.unwrap());
    }

    #[tokio::test]
    async fn test_date_part_granularity_from_dimension() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let context = TestContext::new(DatabaseProtocol::PostgreSQL).await;

        // This date should be idempotent for every expected granularity, so mocked response would stay correct
        // At the same time, it should generate different extractions for different tokens
        let base_date = "2024-10-01T00:00:00.000Z";

        let tokens = [
            ("day", "day"),
            ("dow", "day"),
            ("doy", "day"),
            ("quarter", "quarter"),
            ("qtr", "quarter"),
        ];

        for (token, expected_granularity) in tokens {
            // language=PostgreSQL
            let query = format!(
                r#"
                SELECT date_part('{token}', dim_date0) AS result
                FROM MultiTypeCube
                GROUP BY 1
            "#
            );

            let expected_cube_scan = V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "MultiTypeCube.dim_date0".to_string(),
                    granularity: Some(expected_granularity.to_string()),
                    date_range: None,
                }]),
                order: Some(vec![]),
                ..Default::default()
            };

            context
                .add_cube_load_mock(
                    expected_cube_scan.clone(),
                    simple_load_response(vec![
                        json!({format!("MultiTypeCube.dim_date0.{expected_granularity}"): base_date}),
                    ]),
                )
                .await;

            assert_eq!(
                context
                    .convert_sql_to_cube_query(&query)
                    .await
                    .unwrap()
                    .as_logical_plan()
                    .find_cube_scan()
                    .request,
                expected_cube_scan
            );

            // Expect different values for different tokens
            insta::assert_snapshot!(
                format!("date_part_{token}_from_dimension"),
                context.execute_query(query).await.unwrap()
            );
        }
    }

    #[tokio::test]
    async fn test_noninjective_call_dimension() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let context = TestContext::new(DatabaseProtocol::PostgreSQL).await;

        // Expected scan is same for every query
        let expected_cube_scan = V1LoadRequestQuery {
            measures: Some(vec![]),
            segments: Some(vec![]),
            dimensions: Some(vec!["MultiTypeCube.dim_str0".to_string()]),
            order: Some(vec![]),
            ..Default::default()
        };

        context
            .add_cube_load_mock(
                expected_cube_scan.clone(),
                simple_load_response(vec![
                    json!({"MultiTypeCube.dim_str0": "foo"}),
                    json!({"MultiTypeCube.dim_str0": null}),
                    json!({"MultiTypeCube.dim_str0": "(none)"}),
                    json!({"MultiTypeCube.dim_str0": "abcd"}),
                    json!({"MultiTypeCube.dim_str0": "ab__cd"}),
                ]),
            )
            .await;

        let exprs = [
            ("coalesce", "COALESCE(dim_str0, '(none)')"),
            ("nullif", "NULLIF(dim_str0, '(none)')"),
            ("left", "LEFT(dim_str0, 2)"),
            ("right", "RIGHT(dim_str0, 2)"),
        ];

        for (name, expr) in exprs {
            // language=PostgreSQL
            let query = format!(
                r#"
                SELECT {expr} AS result
                FROM MultiTypeCube
                GROUP BY 1
                ORDER BY result
            "#
            );

            assert_eq!(
                context
                    .convert_sql_to_cube_query(&query)
                    .await
                    .unwrap()
                    .as_logical_plan()
                    .find_cube_scan()
                    .request,
                expected_cube_scan
            );

            // Expect no dublicates in result set
            insta::assert_snapshot!(
                format!("noninjective_{name}_from_dimension"),
                context.execute_query(query).await.unwrap()
            );
        }
    }

    #[tokio::test]
    async fn test_wrapper_tableau_sunday_week() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT (CAST(DATE_TRUNC('day', CAST(order_date AS TIMESTAMP)) AS DATE) - (((7 + CAST(EXTRACT(DOW FROM order_date) AS BIGINT) - 1) % 7) * INTERVAL '1 DAY')) AS \"twk:date:ok\", AVG(avgPrice) mp FROM KibanaSampleDataEcommerce a GROUP BY 1 ORDER BY 1 DESC"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        assert!(logical_plan
            .find_cube_scan_wrapped_sql()
            .wrapped_sql
            .sql
            .contains("EXTRACT"));
    }

    // TODO using this is not correct, but works for now
    fn empty_annotation() -> V1LoadResultAnnotation {
        V1LoadResultAnnotation::new(json!([]), json!([]), json!([]), json!([]))
    }

    pub(crate) fn simple_load_response(data: Vec<serde_json::Value>) -> V1LoadResponse {
        V1LoadResponse::new(vec![V1LoadResult::new(empty_annotation(), data)])
    }

    #[tokio::test]
    async fn test_cube_scan_exec() {
        init_testing_logger();

        let context = TestContext::new(DatabaseProtocol::PostgreSQL).await;

        // language=PostgreSQL
        let query = r#"
            SELECT dim_str0
            FROM MultiTypeCube
            GROUP BY 1
        "#;

        let expected_cube_scan = V1LoadRequestQuery {
            measures: Some(vec![]),
            segments: Some(vec![]),
            dimensions: Some(vec!["MultiTypeCube.dim_str0".to_string()]),
            order: Some(vec![]),
            ..Default::default()
        };

        assert_eq!(
            context
                .convert_sql_to_cube_query(query)
                .await
                .unwrap()
                .as_logical_plan()
                .find_cube_scan()
                .request,
            expected_cube_scan,
        );

        context
            .add_cube_load_mock(
                expected_cube_scan,
                simple_load_response(vec![json!({"MultiTypeCube.dim_str0": "foo"})]),
            )
            .await;

        insta::assert_snapshot!(context.execute_query(query).await.unwrap());
    }

    #[tokio::test]
    async fn test_wrapper_tableau_week_number() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT CAST(FLOOR((7 + EXTRACT(DOY FROM order_date) - 1 + EXTRACT(DOW FROM DATE_TRUNC('YEAR', order_date))) / 7) AS INT) AS \"wk:created_at:ok\", AVG(avgPrice) mp FROM KibanaSampleDataEcommerce a GROUP BY 1 ORDER BY 1 DESC"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        assert!(logical_plan
            .find_cube_scan_wrapped_sql()
            .wrapped_sql
            .sql
            .contains("EXTRACT"));
    }

    #[tokio::test]
    async fn test_wrapper_tableau_week_mmmm_yyyy() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT ((CAST(TRUNC(EXTRACT(YEAR FROM order_date)) AS INT) * 100) + CAST(TRUNC(EXTRACT(MONTH FROM order_date)) AS INT)) AS \"my:created_at:ok\", AVG(avgPrice) mp FROM KibanaSampleDataEcommerce a GROUP BY 1 ORDER BY 1 DESC"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        assert!(logical_plan
            .find_cube_scan_wrapped_sql()
            .wrapped_sql
            .sql
            .contains("EXTRACT"));
    }

    #[tokio::test]
    async fn test_wrapper_tableau_iso_quarter() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT (LEAST(CAST((EXTRACT(WEEK FROM order_date) - 1) AS BIGINT) / 13, 3) + 1) AS \"iqr:created_at:ok\", AVG(avgPrice) mp FROM KibanaSampleDataEcommerce a GROUP BY 1 ORDER BY 1 DESC"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        assert!(logical_plan
            .find_cube_scan_wrapped_sql()
            .wrapped_sql
            .sql
            .contains("EXTRACT"));
    }

    #[tokio::test]
    async fn test_wrapper_window_function() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT customer_gender, AVG(avgPrice) mp, SUM(COUNT(count)) OVER() FROM KibanaSampleDataEcommerce a GROUP BY 1 LIMIT 100"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await;

        let logical_plan = query_plan.as_logical_plan();
        assert!(
            logical_plan
                .find_cube_scan_wrapped_sql()
                .wrapped_sql
                .sql
                .contains("OVER"),
            "SQL should contain 'OVER': {}",
            logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql
        );

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );
    }

    #[tokio::test]
    async fn test_wrapper_long_alias_names() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT customer_gender AS long_long_long_long_long_long_long_long_a, AVG(avgPrice) AS long_long_long_long_long_long_long_long_b, SUM(COUNT(count)) OVER() AS long_long_long_long_long_long_long_long_c FROM KibanaSampleDataEcommerce a GROUP BY 1 LIMIT 100"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await;

        let logical_plan = query_plan.as_logical_plan();
        assert!(
            logical_plan
                .find_cube_scan_wrapped_sql()
                .wrapped_sql
                .sql
                .contains("long_l_1"),
            "SQL should contain long_l_1: {}",
            logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql
        );

        assert!(
            logical_plan
                .find_cube_scan_wrapped_sql()
                .wrapped_sql
                .sql
                .contains("long_l_1"),
            "SQL should contain long_l_2: {}",
            logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql
        );

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );
    }

    #[tokio::test]
    async fn test_tableau_custom_date_diff() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT SUM(CAST(FLOOR(EXTRACT(EPOCH FROM CAST(CURRENT_DATE() AS TIMESTAMP)) / 86400) - FLOOR(EXTRACT(EPOCH FROM CAST(order_date AS TIMESTAMP)) / 86400) AS BIGINT)) FROM KibanaSampleDataEcommerce a"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await;

        let logical_plan = query_plan.as_logical_plan();
        assert!(logical_plan
            .find_cube_scan_wrapped_sql()
            .wrapped_sql
            .sql
            .contains("CURRENT_DATE()"));

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );
    }

    #[tokio::test]
    async fn test_tableau_extract_epoch() -> Result<(), CubeError> {
        init_testing_logger();

        insta::assert_snapshot!(
            "tableau_extract_epoch",
            execute_query(
                "SELECT EXTRACT(EPOCH FROM (TIMESTAMP '2050-01-01T23:01:01.22')) as t".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_thoughtspot_pg_date_trunc_year() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "ta_1"."customer_gender" AS "ca_1",
                CAST(EXTRACT(YEAR FROM "ta_1"."order_date") || '-' || 1 || '-01' AS DATE) AS "ca_2",
                COALESCE(sum("ta_1"."count"), 0) AS "ca_3"
            FROM "db"."public"."KibanaSampleDataEcommerce" AS "ta_1"
            WHERE (
                LOWER("ta_1"."customer_gender") = 'none'
                AND LOWER("ta_1"."notes") = ''
            )
            GROUP BY
                "ca_1",
                "ca_2"
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;

        // check if contains `CAST(EXTRACT(year FROM ..) || .. || .. || ..)`
        let re = Regex::new(r"CAST.+EXTRACT.+year FROM(.+ \|\|){3}").unwrap();
        assert!(re.is_match(&sql));
        // check if contains `LOWER(..) = .. AND LOWER(..) = ..`
        let re = Regex::new(r"LOWER ?\(.+\) = .+ AND .+LOWER ?\(.+\) = .+").unwrap();
        assert!(re.is_match(&sql));
    }

    #[tokio::test]
    async fn test_thoughtspot_pg_date_trunc_quarter() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "ta_1"."customer_gender" AS "ca_1",
                CAST(
                    EXTRACT(YEAR FROM "ta_1"."order_date")
                    || '-'
                    || ((FLOOR(((EXTRACT(MONTH FROM "ta_1"."order_date") - 1) / NULLIF(3,0))) * 3) + 1)
                    || '-01'
                    AS DATE
                ) AS "ca_2"
            FROM "db"."public"."KibanaSampleDataEcommerce" AS "ta_1"
            GROUP BY
                "ca_1",
                "ca_2"
            LIMIT 1000
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("quarter".to_string()),
                    date_range: None,
                }]),
                order: Some(vec![]),
                limit: Some(1000),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_pg_date_trunc_month() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                DATE_TRUNC('MONTH', CAST("ta_1"."order_date" AS date)) AS "ca_1",
                count(DISTINCT "ta_1"."countDistinct") AS "ca_2"
            FROM "db"."public"."KibanaSampleDataEcommerce" AS "ta_1"
            GROUP BY "ca_1"
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.countDistinct".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: None,
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_pg_extract_month_of_quarter() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "ta_1"."customer_gender" AS "ca_1",
                (MOD(CAST((EXTRACT(MONTH FROM "ta_1"."order_date") - 1) AS numeric), 3) + 1) AS "ca_2"
            FROM "db"."public"."KibanaSampleDataEcommerce" AS "ta_1"
            GROUP BY
                "ca_1",
                "ca_2"
            LIMIT 1000
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: None,
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_pg_extract_day_of_year() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT
                (CAST("ta_1"."order_date" AS date) - CAST((CAST(EXTRACT(YEAR FROM "ta_1"."order_date") || '-' || EXTRACT(MONTH FROM "ta_1"."order_date") || '-01' AS DATE) + ((EXTRACT(MONTH FROM "ta_1"."order_date") - 1) * -1) * INTERVAL '1 month') AS date) + 1) AS "ca_1",
                "ta_1"."customer_gender" AS "ca_2"
            FROM "db"."public"."KibanaSampleDataEcommerce" AS "ta_1"
            GROUP BY
                "ca_1",
                "ca_2"
            LIMIT 1000
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;
        let logical_plan = query_plan.as_logical_plan();

        if Rewriter::sql_push_down_enabled() {
            let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
            assert!(sql.contains("EXTRACT(year"));
            assert!(sql.contains("EXTRACT(month"));

            let physical_plan = query_plan.as_physical_plan().await.unwrap();
            println!(
                "Physical plan: {}",
                displayable(physical_plan.as_ref()).indent()
            );
            return;
        }

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "KibanaSampleDataEcommerce.customer_gender".to_string()
                ]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![
                    V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some("year".to_string()),
                        date_range: None,
                    },
                    V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some("month".to_string()),
                        date_range: None,
                    }
                ]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_pg_extract_day_of_quarter() {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT
                (CAST("ta_1"."order_date" AS date) - CAST((CAST(EXTRACT(YEAR FROM "ta_1"."order_date") || '-' || EXTRACT(MONTH FROM "ta_1"."order_date") || '-01' AS DATE) + (((MOD(CAST((EXTRACT(MONTH FROM "ta_1"."order_date") - 1) AS numeric), 3) + 1) - 1) * -1) * INTERVAL '1 month') AS date) + 1) AS "ca_1",
                "ta_1"."customer_gender" AS "ca_2"
            FROM "db"."public"."KibanaSampleDataEcommerce" AS "ta_1"
            GROUP BY
                "ca_1",
                "ca_2"
            LIMIT 1000
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;
        let logical_plan = query_plan.as_logical_plan();

        if Rewriter::sql_push_down_enabled() {
            let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
            assert!(sql.contains("DATEDIFF(day,"));
            assert!(sql.contains("EXTRACT(year"));
            assert!(sql.contains("EXTRACT(month"));

            let physical_plan = query_plan.as_physical_plan().await.unwrap();
            println!(
                "Physical plan: {}",
                displayable(physical_plan.as_ref()).indent()
            );
            return;
        }

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: None,
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_pg_extract_day_of_week() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT
                (MOD(CAST((CAST("ta_1"."order_date" AS date) - CAST(DATE '1970-01-01' AS date) + 3) AS numeric), 7) + 1) AS "ca_1",
                "ta_1"."customer_gender" AS "ca_2"
            FROM "db"."public"."KibanaSampleDataEcommerce" AS "ta_1"
            GROUP BY
                "ca_1",
                "ca_2"
            LIMIT 1000
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;
        let logical_plan = query_plan.as_logical_plan();

        // TODO: split on complex expressions?
        // CAST(CAST(ta_1.order_date AS Date32) - CAST(CAST(Utf8("1970-01-01") AS Date32) AS Date32) + Int64(3) AS Decimal(38, 10))
        if Rewriter::sql_push_down_enabled() {
            let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
            assert!(sql.contains("LIMIT 1000"));
            assert!(sql.contains("% 7"));

            let physical_plan = query_plan.as_physical_plan().await.unwrap();
            println!(
                "Physical plan: {}",
                displayable(physical_plan.as_ref()).indent()
            );
            return;
        }

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "KibanaSampleDataEcommerce.customer_gender".to_string()
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_domo_filter_date_gt() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "T1"."order_date",
                COUNT("T1"."count") AS "count",
                "T1"."customer_gender"
            FROM "db"."public"."KibanaSampleDataEcommerce" AS "T1"
            WHERE (DATE("T1"."order_date") > '2020-01-01')
            GROUP BY
                "T1"."customer_gender",
                "T1"."order_date"
            LIMIT 25000
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                limit: Some(25000),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                    operator: Some("afterOrOnDate".to_string()),
                    values: Some(vec!["2020-01-02T00:00:00.000Z".to_string()]),
                    or: None,
                    and: None,
                }]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_domo_filter_date_between() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "T1"."order_date",
                COUNT("T1"."count") AS "count",
                "T1"."customer_gender"
            FROM "db"."public"."KibanaSampleDataEcommerce" AS "T1"
            WHERE DATE("T1"."order_date") BETWEEN '2019-01-01' AND '2020-01-01'
            GROUP BY
                "T1"."customer_gender",
                "T1"."order_date"
            LIMIT 25000
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                ]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2019-01-01".to_string(),
                        "2020-01-01".to_string(),
                    ])),
                }]),
                order: Some(vec![]),
                limit: Some(25000),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_domo_filter_not_date() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "T1"."order_date",
                COUNT("T1"."count") AS "count",
                "T1"."customer_gender"
            FROM "db"."public"."KibanaSampleDataEcommerce" AS "T1"
            WHERE (NOT (DATE("T1"."order_date") = '2019-01-01'))
            GROUP BY
                "T1"."customer_gender",
                "T1"."order_date"
            LIMIT 25000
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                limit: Some(25000),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                            operator: Some("beforeDate".to_string()),
                            values: Some(vec!["2019-01-01T00:00:00.000Z".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                            operator: Some("afterOrOnDate".to_string()),
                            values: Some(vec!["2019-01-02T00:00:00.000Z".to_string()]),
                            or: None,
                            and: None,
                        }),
                    ]),
                    and: None,
                }]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_sigma_visitor_group_by() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                datetrunc_12 AS "Day",
                sum_14 AS "Sum"
            FROM
            (
                SELECT
                    DATE_TRUNC('day', CAST(order_date AS TIMESTAMP)) AS datetrunc_12,
                    SUM(count) AS sum_14
                FROM
                    "public".KibanaSampleDataEcommerce AS KibanaSampleDataEcommerce
                GROUP BY
                    DATE_TRUNC('day', CAST(order_date AS timestamptz))
            ) AS q1
            ORDER BY
                datetrunc_12 ASC
            LIMIT
                25000
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let cube_scan = logical_plan.find_cube_scan();

        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: None
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "asc".to_string(),
                ]]),
                limit: Some(25000),
                ..Default::default()
            }
        );
    }

    // TODO: Can't generate SQL for literal: IntervalMonthDayNano
    #[ignore]
    #[tokio::test]
    async fn test_sigma_sunday_week_push_down() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            select
                datetrunc_12 "Week of Event Date",
                sum_15 "Active_Events_SUM_Metric"
            from (
                select
                    (
                        date_trunc(
                            'week',
                            (
                                order_date :: timestamptz + cast(1 || ' day' as interval)
                            )
                        ) + cast(-1 || ' day' as interval)
                    ) datetrunc_12,
                    sum(count) sum_15
                from
                    "public"."KibanaSampleDataEcommerce" "KibanaSampleDataEcommerce"
                group by
                    (
                        date_trunc(
                            'week',
                            (
                                order_date :: timestamptz + cast(1 || ' day' as interval)
                            )
                        ) + cast(-1 || ' day' as interval)
                    )
            ) q1
            order by
                datetrunc_12 asc
            limit
                25000
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        assert_eq!(
            query_plan.as_logical_plan().find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: None,
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_case_timestamp_nanosecond() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            select
                "_"."order_date" as "c10"
            from
                (
                    select
                        "order_date",
                        "_"."t0_0" as "t0_0",
                        "_"."t1_0" as "t1_0"
                    from
                        (
                            select
                                "_"."order_date",
                                "_"."o0",
                                "_"."t0_0",
                                "_"."t1_0"
                            from
                                (
                                    select
                                        "_"."order_date" as "order_date",
                                        "_"."o0" as "o0",
                                        case
                                            when "_"."o0" is not null then "_"."o0"
                                            else timestamp '1899-12-28 00:00:00'
                                        end as "t0_0",
                                        case
                                            when "_"."o0" is null then 0
                                            else 1
                                        end as "t1_0"
                                    from
                                        (
                                            select
                                                "rows"."order_date" as "order_date",
                                                "rows"."o0" as "o0"
                                            from
                                                (
                                                    select
                                                        "order_date" as "order_date",
                                                        "order_date" as "o0"
                                                    from
                                                        "public"."KibanaSampleDataEcommerce" "$Table"
                                                ) "rows"
                                            group by
                                                "order_date",
                                                "o0"
                                        ) "_"
                                ) "_"
                        ) "_"
                ) "_"
            order by
                "_"."t0_0",
                "_"."t1_0"
            limit
                101
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;
        let logical_plan = query_plan.as_logical_plan();

        if Rewriter::sql_push_down_enabled() {
            let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
            assert!(sql.contains("LIMIT 101"));
            assert!(sql.contains("ORDER BY"));

            let physical_plan = query_plan.as_physical_plan().await.unwrap();
            println!(
                "Physical plan: {}",
                displayable(physical_plan.as_ref()).indent()
            );
            return;
        }

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_langchain_pgcatalog_schema() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "langchain_pgcatalog_schema",
            execute_query(
                "
                SELECT pg_catalog.pg_class.relname
                FROM pg_catalog.pg_class
                JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace
                WHERE
                    pg_catalog.pg_class.relkind = ANY (ARRAY['r', 'p'])
                    AND pg_catalog.pg_class.relpersistence != 't'
                    AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid)
                    AND pg_catalog.pg_namespace.nspname != 'pg_catalog'
                ;".to_string(),
                DatabaseProtocol::PostgreSQL,
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_langchain_array_agg_order_by() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "langchain_array_agg_order_by",
            execute_query(
                "
                SELECT
                    pg_catalog.pg_type.typname AS name,
                    pg_catalog.pg_type_is_visible(pg_catalog.pg_type.oid) AS visible,
                    pg_catalog.pg_namespace.nspname AS schema, lbl_agg.labels AS labels
                FROM pg_catalog.pg_type
                JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_type.typnamespace
                LEFT OUTER JOIN (
                    SELECT
                        pg_catalog.pg_enum.enumtypid AS enumtypid,
                        array_agg(pg_catalog.pg_enum.enumlabel ORDER BY pg_catalog.pg_enum.enumsortorder) AS labels
                    FROM pg_catalog.pg_enum
                    GROUP BY pg_catalog.pg_enum.enumtypid
                ) AS lbl_agg ON pg_catalog.pg_type.oid = lbl_agg.enumtypid
                WHERE pg_catalog.pg_type.typtype = 'e'
                ORDER BY
                    pg_catalog.pg_namespace.nspname,
                    pg_catalog.pg_type.typname
                ;".to_string(),
                DatabaseProtocol::PostgreSQL,
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_inlist_expr() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "
            SELECT
                CASE
                    WHEN (customer_gender NOT IN ('1', '2', '3')) THEN customer_gender
                    ELSE '0'
                END AS customer_gender
            FROM KibanaSampleDataEcommerce AS k
            GROUP BY 1
            ORDER BY 1 DESC
            "
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        assert!(logical_plan
            .find_cube_scan_wrapped_sql()
            .wrapped_sql
            .sql
            .contains("NOT IN ("));
    }

    #[tokio::test]
    async fn test_negative_expr() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "
            SELECT -taxful_total_price AS neg_taxful_total_price
            FROM KibanaSampleDataEcommerce AS k
            GROUP BY 1
            ORDER BY 1 DESC
            "
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        assert_eq!(
            query_plan.as_logical_plan().find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_not_expr() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "
            SELECT NOT has_subscription AS has_no_subscription
            FROM KibanaSampleDataEcommerce AS k
            GROUP BY 1
            ORDER BY 1 DESC
            "
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        assert!(logical_plan
            .find_cube_scan_wrapped_sql()
            .wrapped_sql
            .sql
            .contains("NOT ("));
    }

    #[tokio::test]
    async fn test_datetrunc_push_down() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        // BigQuery
        let query_plan = convert_select_to_query_plan_customized(
            "
            SELECT DATE_TRUNC('week', k.order_date) AS d
            FROM KibanaSampleDataEcommerce AS k
            WHERE LOWER(k.customer_gender) = LOWER('unknown')
            GROUP BY 1
            ORDER BY 1 DESC
            "
            .to_string(),
            DatabaseProtocol::PostgreSQL,
            vec![
                ("functions/DATETRUNC".to_string(), "DATETIME_TRUNC(CAST({{ args[1] }} AS DATETIME), {% if date_part|upper == \'WEEK\' %}{{ \'WEEK(MONDAY)\' }}{% else %}{{ date_part }}{% endif %})".to_string()),
            ]
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(sql.contains(".week"));
    }

    #[tokio::test]
    async fn test_datediff_push_down() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "
            SELECT DATEDIFF(DAY, order_date, last_mod) AS d
            FROM KibanaSampleDataEcommerce AS k
            GROUP BY 1
            ORDER BY 1 DESC
            "
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        assert!(logical_plan
            .find_cube_scan_wrapped_sql()
            .wrapped_sql
            .sql
            .contains("DATEDIFF(day,"));

        // BigQuery
        let query_plan = convert_select_to_query_plan_customized(
            "
            SELECT DATEDIFF(DAY, order_date, last_mod) AS d
            FROM KibanaSampleDataEcommerce AS k
            GROUP BY 1
            ORDER BY 1 DESC
            "
            .to_string(),
            DatabaseProtocol::PostgreSQL,
            vec![
                ("functions/DATEDIFF".to_string(), "DATETIME_DIFF(CAST({{ args[2] }} AS DATETIME), CAST({{ args[1] }} AS DATETIME), {{ date_part }})".to_string()),
            ]
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(sql.contains("DATETIME_DIFF(CAST("));
        assert!(sql.contains("day)"));

        // Databricks
        let query_plan = convert_select_to_query_plan_customized(
            "
            SELECT DATEDIFF(DAY, order_date, last_mod) AS d
            FROM KibanaSampleDataEcommerce AS k
            GROUP BY 1
            ORDER BY 1 DESC
            "
            .to_string(),
            DatabaseProtocol::PostgreSQL,
            vec![
                ("functions/DATEDIFF".to_string(), "DATEDIFF({{ date_part }}, DATE_TRUNC(\'{{ date_part }}\', {{ args[1] }}), DATE_TRUNC(\'{{ date_part }}\', {{ args[2] }}))".to_string()),
            ]
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(sql.contains("DATEDIFF(day,"));
        assert!(sql.contains("DATE_TRUNC('day',"));

        // PostgreSQL
        let query_plan = convert_select_to_query_plan_customized(
            "
            SELECT DATEDIFF(DAY, order_date, last_mod) AS d
            FROM KibanaSampleDataEcommerce AS k
            GROUP BY 1
            ORDER BY 1 DESC
            "
            .to_string(),
            DatabaseProtocol::PostgreSQL,
            vec![
                ("functions/DATEDIFF".to_string(), "CASE WHEN LOWER(\'{{ date_part }}\') IN (\'year\', \'quarter\', \'month\') THEN (EXTRACT(YEAR FROM AGE(DATE_TRUNC(\'{{ date_part }}\', {{ args[2] }}), DATE_TRUNC(\'{{ date_part }}\', {{ args[1] }}))) * 12 + EXTRACT(MONTH FROM AGE(DATE_TRUNC(\'{{ date_part }}\', {{ args[2] }}), DATE_TRUNC(\'{{ date_part }}\', {{ args[1] }})))) / CASE LOWER(\'{{ date_part }}\') WHEN \'year\' THEN 12 WHEN \'quarter\' THEN 3 WHEN \'month\' THEN 1 END ELSE EXTRACT(EPOCH FROM DATE_TRUNC(\'{{ date_part }}\', {{ args[2] }}) - DATE_TRUNC(\'{{ date_part }}\', {{ args[1] }})) / EXTRACT(EPOCH FROM \'1 {{ date_part }}\'::interval) END::bigint".to_string()),
            ]
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(sql.contains("CASE WHEN LOWER('day')"));
        assert!(sql.contains("WHEN 'year' THEN 12 WHEN 'quarter' THEN 3 WHEN 'month' THEN 1 END"));
        assert!(sql.contains("EXTRACT(EPOCH FROM"));
    }

    #[tokio::test]
    async fn test_dateadd_push_down() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        // Redshift function DATEADD
        let query_plan = convert_select_to_query_plan(
            "
            SELECT DATEADD(DAY, 7, order_date) AS d
            FROM KibanaSampleDataEcommerce AS k
            WHERE LOWER(customer_gender) = 'test'
            GROUP BY 1
            ORDER BY 1 DESC
            "
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        // redshift-dateadd-[literal-date32-]to-interval rewrites DATEADD to DATE_ADD
        assert!(sql.contains("DATE_ADD("));
        assert!(sql.contains("INTERVAL '7 DAY')"));

        // BigQuery + Postgres DATE_ADD + DAYS
        let bq_templates = vec![("functions/DATE_ADD".to_string(), "{% if date_part|upper in ['YEAR', 'MONTH', 'QUARTER'] %}TIMESTAMP(DATETIME_ADD(DATETIME({{ args[0] }}), INTERVAL {{ interval }} {{ date_part }})){% else %}TIMESTAMP_ADD({{ args[0] }}, INTERVAL {{ interval }} {{ date_part }}){% endif %}".to_string())];
        let query_plan = convert_select_to_query_plan_customized(
            "
            SELECT DATE_ADD(order_date, INTERVAL '7 DAYS') AS d
            FROM KibanaSampleDataEcommerce AS k
            WHERE LOWER(customer_gender) = 'test'
            GROUP BY 1
            ORDER BY 1 DESC
            "
            .to_string(),
            DatabaseProtocol::PostgreSQL,
            bq_templates.clone(),
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(sql.contains("TIMESTAMP_ADD("));
        assert!(sql.contains("INTERVAL 7 DAY)"));

        // BigQuery + Redshift DATEADD + DAYS
        let bq_templates = vec![("functions/DATE_ADD".to_string(), "{% if date_part|upper in ['YEAR', 'MONTH', 'QUARTER'] %}TIMESTAMP(DATETIME_ADD(DATETIME({{ args[0] }}), INTERVAL {{ interval }} {{ date_part }})){% else %}TIMESTAMP_ADD({{ args[0] }}, INTERVAL {{ interval }} {{ date_part }}){% endif %}".to_string())];
        let query_plan = convert_select_to_query_plan_customized(
            "
            SELECT DATEADD(DAY, 7, order_date) AS d
            FROM KibanaSampleDataEcommerce AS k
            WHERE LOWER(customer_gender) = 'test'
            GROUP BY 1
            ORDER BY 1 DESC
            "
            .to_string(),
            DatabaseProtocol::PostgreSQL,
            bq_templates.clone(),
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(sql.contains("TIMESTAMP_ADD("));
        assert!(sql.contains("INTERVAL 7 DAY)"));

        // BigQuery + Postgres DATE_ADD + MONTHS
        let query_plan = convert_select_to_query_plan_customized(
            "
            SELECT DATE_ADD(order_date, INTERVAL '7 MONTHS') AS d
            FROM KibanaSampleDataEcommerce AS k
            WHERE LOWER(customer_gender) = 'test'
            GROUP BY 1
            ORDER BY 1 DESC
            "
            .to_string(),
            DatabaseProtocol::PostgreSQL,
            bq_templates,
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(sql.contains("TIMESTAMP(DATETIME_ADD(DATETIME("));
        assert!(sql.contains("INTERVAL 7 MONTH)"));

        // BigQuery + Redshift DATEADD + MONTHS
        let bq_templates = vec![("functions/DATE_ADD".to_string(), "{% if date_part|upper in ['YEAR', 'MONTH', 'QUARTER'] %}TIMESTAMP(DATETIME_ADD(DATETIME({{ args[0] }}), INTERVAL {{ interval }} {{ date_part }})){% else %}TIMESTAMP_ADD({{ args[0] }}, INTERVAL {{ interval }} {{ date_part }}){% endif %}".to_string())];
        let query_plan = convert_select_to_query_plan_customized(
            "
            SELECT DATEADD(MONTH, 7, order_date) AS d
            FROM KibanaSampleDataEcommerce AS k
            WHERE LOWER(customer_gender) = 'test'
            GROUP BY 1
            ORDER BY 1 DESC
            "
            .to_string(),
            DatabaseProtocol::PostgreSQL,
            bq_templates.clone(),
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(sql.contains("TIMESTAMP(DATETIME_ADD(DATETIME("));
        assert!(sql.contains("INTERVAL 7 MONTH)"));

        // Postgres DATE_ADD
        let query_plan = convert_select_to_query_plan_customized(
            "
            SELECT DATE_ADD(order_date, INTERVAL '7 DAYS') AS d
            FROM KibanaSampleDataEcommerce AS k
            WHERE LOWER(customer_gender) = 'test'
            GROUP BY 1
            ORDER BY 1 DESC
            "
            .to_string(),
            DatabaseProtocol::PostgreSQL,
            vec![],
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(sql.contains("DATE_ADD("));
        assert!(sql.contains("INTERVAL '7 DAY'"));
    }

    #[tokio::test]
    async fn test_string_multiply_interval() -> Result<(), CubeError> {
        init_testing_logger();

        insta::assert_snapshot!(
            "test_string_multiply_interval",
            execute_query(
                "SELECT NULL * INTERVAL '1 day' n, '5' * INTERVAL '1 day' d5".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_domo_group_date_by_month() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "domo_group_date_by_month",
            execute_query(
                "SELECT TO_CHAR(DATE('2023-10-02 13:47:01')::TIMESTAMP, 'YYYY-Mon') AS \"CalendarMonth\"".to_string(),
                DatabaseProtocol::PostgreSQL,
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cast_string_to_interval() -> Result<(), CubeError> {
        init_testing_logger();

        insta::assert_snapshot!(
            "test_cast_string_to_interval",
            execute_query(
                "SELECT CAST('3 DAY' AS INTERVAL) AS d3".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_push_down_date_fn() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            "
            SELECT CONCAT(DATE(order_date), '-') AS d
            FROM KibanaSampleDataEcommerce AS k
            GROUP BY 1
            "
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        assert!(logical_plan
            .find_cube_scan_wrapped_sql()
            .wrapped_sql
            .sql
            .contains("DATE("));
    }

    #[tokio::test]
    async fn test_thoughtspot_kpi_monthly() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT
                DATE_TRUNC('MONTH', CAST("ta_1"."order_date" AS date)) AS "ca_1",
                COALESCE(sum("ta_1"."count"), 0) AS "ca_2",
                min((MOD(CAST((EXTRACT(MONTH FROM "ta_1"."order_date") - 1) AS numeric), 3) + 1)) AS "ca_3",
                min(CEIL((EXTRACT(MONTH FROM "ta_1"."order_date") / NULLIF(3.0,0.0)))) AS "ca_4"
            FROM "db"."public"."KibanaSampleDataEcommerce" AS "ta_1"
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC
            LIMIT 5000
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        assert!(logical_plan
            .find_cube_scan_wrapped_sql()
            .wrapped_sql
            .sql
            .contains("EXTRACT(month FROM "));
    }

    #[tokio::test]
    async fn test_thoughtspot_kpi_daily() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT
                CAST("ta_1"."order_date" AS date) AS "ca_1",
                COALESCE(sum("ta_1"."count"), 0) AS "ca_2",
                min((MOD(CAST((CAST("ta_1"."order_date" AS date) - CAST(DATE '1970-01-01' AS date) + 3) AS numeric), 7) + 1)) AS "ca_3",
                min(EXTRACT(DAY FROM "ta_1"."order_date")) AS "ca_4",
                min((CAST("ta_1"."order_date" AS date) - CAST((CAST(EXTRACT(YEAR FROM "ta_1"."order_date") || '-' || EXTRACT(MONTH FROM "ta_1"."order_date") || '-01' AS DATE) + ((EXTRACT(MONTH FROM "ta_1"."order_date") - 1) * -1) * INTERVAL '1 month') AS date) + 1)) AS "ca_5",
                min(FLOOR(((EXTRACT(DAY FROM ("ta_1"."order_date" + (4 - (MOD(CAST((CAST("ta_1"."order_date" AS date) - CAST(DATE '1970-01-01' AS date) + 3) AS numeric), 7) + 1)) * INTERVAL '1 day')) + 6) / NULLIF(7,0)))) AS "ca_6",
                min(FLOOR(((CAST(("ta_1"."order_date" + (4 - (MOD(CAST((CAST("ta_1"."order_date" AS date) - CAST(DATE '1970-01-01' AS date) + 3) AS numeric), 7) + 1)) * INTERVAL '1 day') AS date) - CAST((CAST(EXTRACT(YEAR FROM ("ta_1"."order_date" + (4 - (MOD(CAST((CAST("ta_1"."order_date" AS date) - CAST(DATE '1970-01-01' AS date) + 3) AS numeric), 7) + 1)) * INTERVAL '1 day')) || '-' || EXTRACT(MONTH FROM ("ta_1"."order_date" + (4 - (MOD(CAST((CAST("ta_1"."order_date" AS date) - CAST(DATE '1970-01-01' AS date) + 3) AS numeric), 7) + 1)) * INTERVAL '1 day')) || '-01' AS DATE) + ((EXTRACT(MONTH FROM ("ta_1"."order_date" + (4 - (MOD(CAST((CAST("ta_1"."order_date" AS date) - CAST(DATE '1970-01-01' AS date) + 3) AS numeric), 7) + 1)) * INTERVAL '1 day')) - 1) * -1) * INTERVAL '1 month') AS date) + 1 + 6) / NULLIF(7,0)))) AS "ca_7",
                min((MOD(CAST((EXTRACT(MONTH FROM "ta_1"."order_date") - 1) AS numeric), 3) + 1)) AS "ca_8",
                min(CEIL((EXTRACT(MONTH FROM "ta_1"."order_date") / NULLIF(3.0,0.0)))) AS "ca_9"
            FROM "db"."public"."KibanaSampleDataEcommerce" AS "ta_1"
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC
            LIMIT 5000
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(sql.contains("order_date"));
        assert!(sql.contains("EXTRACT(day FROM"))
    }

    #[tokio::test]
    async fn test_unary_minus_constant_folding() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT order_date + (-EXTRACT(YEAR FROM order_date) * INTERVAL '1 day') AS t
            FROM KibanaSampleDataEcommerce
            GROUP BY 1
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        assert_eq!(
            query_plan.as_logical_plan().find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("year".to_string()),
                    date_range: None,
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_case_mixed_values_with_null() -> Result<(), CubeError> {
        init_testing_logger();

        insta::assert_snapshot!(
            "test_case_mixed_values_with_null",
            execute_query(
                "
                SELECT LEFT(ACOS(
                    CASE i
                        WHEN 0 THEN NULL
                        ELSE (i::float / 10.0)
                    END
                )::text, 10) AS acos
                FROM (
                    SELECT generate_series(0, 5) AS i
                ) AS t
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_athena_offset_limit_push_down() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan_customized(
            "
            SELECT
                SUM(s) AS s,
                p AS p
            FROM (
                SELECT
                    taxful_total_price AS p,
                    CASE
                        WHEN taxful_total_price = 1 THEN 0
                        ELSE SUM(taxful_total_price)
                    END AS s
                FROM KibanaSampleDataEcommerce AS k
                GROUP BY 1
            ) AS t
            GROUP BY 2
            LIMIT 2
            OFFSET 1
            "
            .to_string(),
            DatabaseProtocol::PostgreSQL,
            vec![
                (
                    "statements/select".to_string(),
                    r#"SELECT {{ select_concat | map(attribute='aliased') | join(', ') }}
FROM ({{ from }}) AS {{ from_alias }}
{% if group_by %} GROUP BY {{ group_by | map(attribute='index') | join(', ') }}{% endif %}
{% if order_by %} ORDER BY {{ order_by | map(attribute='expr') | join(', ') }}{% endif %}{% if offset is not none %}
OFFSET {{ offset }}{% endif %}{% if limit is not none %}
LIMIT {{ limit }}{% endif %}"#.to_string(),
                ),
            ]
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(sql.contains("OFFSET 1\nLIMIT 2"));
    }

    #[tokio::test]
    async fn test_metabase_table_privilege_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "metabase_table_privilege_query",
            execute_query(
                r#"
                with table_privileges as (
	 select
	   NULL as role,
	   t.schemaname as schema,
	   t.objectname as table,
	   pg_catalog.has_table_privilege(current_user, '"' || t.schemaname || '"' || '.' || '"' || t.objectname || '"',  'UPDATE') as update,
	   pg_catalog.has_table_privilege(current_user, '"' || t.schemaname || '"' || '.' || '"' || t.objectname || '"',  'SELECT') as select,
	   pg_catalog.has_table_privilege(current_user, '"' || t.schemaname || '"' || '.' || '"' || t.objectname || '"',  'INSERT') as insert,
	   pg_catalog.has_table_privilege(current_user, '"' || t.schemaname || '"' || '.' || '"' || t.objectname || '"',  'DELETE') as delete
	 from (
	   select schemaname, tablename as objectname from pg_catalog.pg_tables
	   union
	   select schemaname, viewname as objectname from pg_catalog.pg_views
	   union
	   select schemaname, matviewname as objectname from pg_catalog.pg_matviews
	 ) t
	 where t.schemaname !~ '^pg_'
	   and t.schemaname <> 'information_schema'
	   and pg_catalog.has_schema_privilege(current_user, t.schemaname, 'USAGE')
	)
	select t.*
	from table_privileges t
    order by t.schema, t.table
                "#.to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_table_privilege_query_v2() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "metabase_table_privilege_query_v2",
            execute_query(
                r#"
                with table_privileges as (
	 select
	   NULL as role,
	   t.schemaname as schema,
	   t.objectname as table,
	   pg_catalog.has_any_column_privilege(current_user, '"' || t.schemaname || '"' || '.' || '"' || t.objectname || '"',  'update') as update,
	   pg_catalog.has_any_column_privilege(current_user, '"' || t.schemaname || '"' || '.' || '"' || t.objectname || '"',  'select') as select,
	   pg_catalog.has_any_column_privilege(current_user, '"' || t.schemaname || '"' || '.' || '"' || t.objectname || '"',  'insert') as insert,
	   pg_catalog.has_table_privilege(current_user, '"' || t.schemaname || '"' || '.' || '"' || t.objectname || '"',  'delete') as delete
	 from (
	   select schemaname, tablename as objectname from pg_catalog.pg_tables
	   union
	   select schemaname, viewname as objectname from pg_catalog.pg_views
	   union
	   select schemaname, matviewname as objectname from pg_catalog.pg_matviews
	 ) t
	 where t.schemaname !~ '^pg_'
	   and t.schemaname <> 'information_schema'
	   and pg_catalog.has_schema_privilege(current_user, t.schemaname, 'usage')
	)
	select t.*
	from table_privileges t
    order by t.schema, t.table
                "#.to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_string_unicode_escapes() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT rows.order_date, rows.customer_gender, sum(rows.sumPrice)
            FROM (
                select order_date, customer_gender, sumPrice
                from KibanaSampleDataEcommerce
                where notes = U&'HHHH-444JJJ\\Admin'

                ) rows
            GROUP BY 1,2
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.sumPrice".to_string(),]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.notes".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["HHHH-444JJJ\\Admin".to_string()]),
                    or: None,
                    and: None
                }]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_cast_float_to_text() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "cast_float_to_text",
            execute_query(
                "SELECT (11.0::double precision)::text AS eleven;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_next_rows_only() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "fetch_next_rows_only",
            execute_query(
                "
                SELECT i
                FROM (
                    SELECT 1 i
                    UNION ALL
                    SELECT 2 i
                    UNION ALL
                    SELECT 3 i
                ) t
                ORDER BY i ASC
                FETCH NEXT 2 ROWS ONLY
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_time_dimensions_filter_twice() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT customer_gender
            FROM KibanaSampleDataEcommerce
            WHERE
                order_date BETWEEN '2024-01-01T00:00:00' AND '2025-01-01T00:00:00'
                AND order_date BETWEEN '2024-03-01T00:00:00' AND '2024-06-01T00:00:00'
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![
                    V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: None,
                        date_range: Some(json!(vec![
                            "2024-01-01T00:00:00".to_string(),
                            "2025-01-01T00:00:00".to_string(),
                        ]))
                    },
                    V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: None,
                        date_range: Some(json!(vec![
                            "2024-03-01T00:00:00".to_string(),
                            "2024-06-01T00:00:00".to_string(),
                        ]))
                    },
                ]),
                order: Some(vec![]),
                ungrouped: Some(true),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_exponentiate_op() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "exponentiate_op",
            execute_query("SELECT 3^5 AS e".to_string(), DatabaseProtocol::PostgreSQL).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_select_distinct_wrapper() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT DISTINCT
                COALESCE(customer_gender, 'N/A', 'NN'),
                AVG(avgPrice) mp
            FROM KibanaSampleDataEcommerce a
            GROUP BY 1
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert!(logical_plan
            .find_cube_scan_wrapped_sql()
            .wrapped_sql
            .sql
            .contains("SELECT DISTINCT "));

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );
    }

    #[tokio::test]
    async fn test_no_cycle_applier() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT "customer_gender" AS "customer_gender"
            FROM (
                SELECT "customer_gender" AS "customer_gender"
                FROM "KibanaSampleDataEcommerce"
                GROUP BY "customer_gender"
                ORDER BY "customer_gender" ASC
            ) AS "KibanaSampleDataEcommerce"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "asc".to_string(),
                ]]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_push_down_window_frame() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT
                customer_gender,
                taxful_total_price,
                CASE
                    WHEN customer_gender IS NOT NULL THEN avg(taxful_total_price) OVER (
                        PARTITION BY customer_gender
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )
                    ELSE 0
                END AS "avg"
            FROM KibanaSampleDataEcommerce k
            GROUP BY 1, 2
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert!(logical_plan
            .find_cube_scan_wrapped_sql()
            .wrapped_sql
            .sql
            .contains("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"));

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );
    }

    #[tokio::test]
    async fn test_long_in_expr() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }

        const N: usize = 50;
        let set = (1..=N).join(", ");

        let query = format!("SELECT * FROM NumberCube WHERE someNumber IN ({set})");
        let query_plan = convert_select_to_query_plan(query, DatabaseProtocol::PostgreSQL).await;
        let logical_plan = query_plan.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["NumberCube.someNumber".into()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("NumberCube.someNumber".into()),
                    operator: Some("equals".into()),
                    values: Some((1..=N).map(|x| x.to_string()).collect()),
                    or: None,
                    and: None
                }]),
                ungrouped: Some(true),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn test_daterange_filter_literals() -> Result<(), CubeError> {
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            // language=PostgreSQL
            r#"SELECT
                    DATE_TRUNC('month', order_date) AS order_date,
                    COUNT(*) AS month_count
            FROM "KibanaSampleDataEcommerce" ecom
            WHERE ecom.order_date >= '2025-01-01' and ecom.order_date < '2025-02-01'
            GROUP BY 1"#
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("month".to_string()),
                    date_range: Some(json!(vec![
                        // WHY NOT "2025-01-01T00:00:00.000Z".to_string(), ?
                        "2025-01-01".to_string(),
                        "2025-01-31T23:59:59.999Z".to_string()
                    ])),
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_time_dimension_range_filter_chain_or() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "customer_gender",
                date_trunc('day', "order_date") AS "order_date"
            FROM "KibanaSampleDataEcommerce"
            WHERE
                ("order_date" >= '2019-01-01 00:00:00.0' AND "order_date" < '2020-01-01 00:00:00.0')
                OR ("order_date" >= '2021-01-01 00:00:00.0' AND "order_date" < '2022-01-01 00:00:00.0')
            GROUP BY 1, 2
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("day".to_owned()),
                    date_range: None
                }]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                            operator: Some("inDateRange".to_string()),
                            values: Some(vec![
                                "2019-01-01 00:00:00.0".to_string(),
                                "2019-12-31T23:59:59.999Z".to_string(),
                            ]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                            operator: Some("inDateRange".to_string()),
                            values: Some(vec![
                                "2021-01-01 00:00:00.0".to_string(),
                                "2021-12-31T23:59:59.999Z".to_string(),
                            ]),
                            or: None,
                            and: None,
                        }),
                    ]),
                    and: None
                }]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_filter_datetrunc_in_date_range_merged() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            select
                DATE_TRUNC('year', order_date) as "c0",
                sum("KibanaSampleDataEcommerce"."sumPrice") as "m0"
            from
                "KibanaSampleDataEcommerce" as "KibanaSampleDataEcommerce"
            where
                DATE_TRUNC('year', order_date) in (
                    '2019-01-01 00:00:00.0',
                    '2020-01-01 00:00:00.0',
                    '2021-01-01 00:00:00.0',
                    '2022-01-01 00:00:00.0',
                    '2023-01-01 00:00:00.0'
                )
            group by
                DATE_TRUNC('year', order_date)
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.sumPrice".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("year".to_string()),
                    date_range: Some(json!(vec![
                        "2019-01-01 00:00:00.000".to_string(),
                        "2023-12-31 23:59:59.999".to_string()
                    ])),
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_filter_datetrunc_in_date_range_separate() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            select
                DATE_TRUNC('quarter', order_date) as "c0",
                sum("KibanaSampleDataEcommerce"."sumPrice") as "m0"
            from
                "KibanaSampleDataEcommerce" as "KibanaSampleDataEcommerce"
            where
                DATE_TRUNC('quarter', order_date) in (
                    '2019-01-01 00:00:00.0',
                    '2020-01-01 00:00:00.0',
                    '2021-01-01 00:00:00.0',
                    '2022-01-01 00:00:00.0',
                    '2023-01-01 00:00:00.0'
                )
            group by
                DATE_TRUNC('quarter', order_date)
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.sumPrice".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("quarter".to_string()),
                    date_range: None
                }]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                            operator: Some("inDateRange".to_string()),
                            values: Some(vec![
                                "2019-01-01 00:00:00.000".to_string(),
                                "2019-03-31 23:59:59.999".to_string(),
                            ]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                            operator: Some("inDateRange".to_string()),
                            values: Some(vec![
                                "2020-01-01 00:00:00.000".to_string(),
                                "2020-03-31 23:59:59.999".to_string(),
                            ]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                            operator: Some("inDateRange".to_string()),
                            values: Some(vec![
                                "2021-01-01 00:00:00.000".to_string(),
                                "2021-03-31 23:59:59.999".to_string(),
                            ]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                            operator: Some("inDateRange".to_string()),
                            values: Some(vec![
                                "2022-01-01 00:00:00.000".to_string(),
                                "2022-03-31 23:59:59.999".to_string(),
                            ]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                            operator: Some("inDateRange".to_string()),
                            values: Some(vec![
                                "2023-01-01 00:00:00.000".to_string(),
                                "2023-03-31 23:59:59.999".to_string(),
                            ]),
                            or: None,
                            and: None,
                        }),
                    ]),
                    and: None
                }]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_date_trunc_date_part_multiple_granularities() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            select
                DATE_TRUNC('year', order_date) as "c0",
                DATE_PART('year', order_date) as "c1",
                DATE_TRUNC('quarter', order_date) as "c2",
                DATE_PART('quarter', order_date) as "c3",
                DATE_TRUNC('month', order_date) as "c4",
                DATE_PART('month', order_date) as "c5",
                DATE_TRUNC('week', order_date) as "c6",
                DATE_PART('week', order_date) as "c7",
                DATE_TRUNC('day', order_date) as "c8",
                DATE_PART('day', order_date) as "c9",
                DATE_TRUNC('hour', order_date) as "c10",
                DATE_PART('hour', order_date) as "c11",
                DATE_TRUNC('minute', order_date) as "c12",
                DATE_PART('minute', order_date) as "c13",
                DATE_TRUNC('second', order_date) as "c14",
                DATE_PART('second', order_date) as "c15"
            from
                "KibanaSampleDataEcommerce" as "KibanaSampleDataEcommerce"
            group by
                DATE_TRUNC('year', order_date),
                DATE_PART('year', order_date),
                DATE_TRUNC('quarter', order_date),
                DATE_PART('quarter', order_date),
                DATE_TRUNC('month', order_date),
                DATE_PART('month', order_date),
                DATE_TRUNC('week', order_date),
                DATE_PART('week', order_date),
                DATE_TRUNC('day', order_date),
                DATE_PART('day', order_date),
                DATE_TRUNC('hour', order_date),
                DATE_PART('hour', order_date),
                DATE_TRUNC('minute', order_date),
                DATE_PART('minute', order_date),
                DATE_TRUNC('second', order_date),
                DATE_PART('second', order_date)
            order by
                CASE
                    WHEN DATE_TRUNC('year', order_date) IS NULL THEN 1
                    ELSE 0
                END,
                DATE_TRUNC('year', order_date) ASC,
                CASE
                    WHEN DATE_TRUNC('quarter', order_date) IS NULL THEN 1
                    ELSE 0
                END,
                DATE_TRUNC('quarter', order_date) ASC,
                CASE
                    WHEN DATE_TRUNC('month', order_date) IS NULL THEN 1
                    ELSE 0
                END,
                DATE_TRUNC('month', order_date) ASC,
                CASE
                    WHEN DATE_TRUNC('week', order_date) IS NULL THEN 1
                    ELSE 0
                END,
                DATE_TRUNC('week', order_date) ASC,
                CASE
                    WHEN DATE_TRUNC('day', order_date) IS NULL THEN 1
                    ELSE 0
                END,
                DATE_TRUNC('day', order_date) ASC,
                CASE
                    WHEN DATE_TRUNC('hour', order_date) IS NULL THEN 1
                    ELSE 0
                END,
                DATE_TRUNC('hour', order_date) ASC,
                CASE
                    WHEN DATE_TRUNC('minute', order_date) IS NULL THEN 1
                    ELSE 0
                END,
                DATE_TRUNC('minute', order_date) ASC,
                CASE
                    WHEN DATE_TRUNC('second', order_date) IS NULL THEN 1
                    ELSE 0
                END,
                DATE_TRUNC('second', order_date) ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![
                    V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some("year".to_string()),
                        date_range: None
                    },
                    V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some("quarter".to_string()),
                        date_range: None
                    },
                    V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some("month".to_string()),
                        date_range: None
                    },
                    V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some("week".to_string()),
                        date_range: None
                    },
                    V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some("day".to_string()),
                        date_range: None
                    },
                    V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some("hour".to_string()),
                        date_range: None
                    },
                    V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some("minute".to_string()),
                        date_range: None
                    },
                    V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some("second".to_string()),
                        date_range: None
                    },
                ]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_date_trunc_eq_literal() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                DATE_TRUNC('quarter', order_date) as "c0",
                DATE_PART('quarter', order_date) as "c1"
            FROM
                "KibanaSampleDataEcommerce" as "KibanaSampleDataEcommerce"
            WHERE
                (
                    DATE_TRUNC('year', order_date) = '2024-01-01 00:00:00.0'
                )
            GROUP BY
                DATE_TRUNC('quarter', order_date),
                DATE_PART('quarter', order_date)
            ORDER BY
                CASE
                    WHEN DATE_TRUNC('quarter', order_date) IS NULL THEN 1
                    ELSE 0
                END,
                DATE_TRUNC('quarter', order_date) ASC
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("quarter".to_string()),
                    date_range: Some(json!(vec![
                        "2024-01-01T00:00:00.000Z".to_string(),
                        "2024-12-31T23:59:59.999Z".to_string(),
                    ])),
                },]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_quicksight_cast_with_limit_no_sort() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "customer_gender" AS "customer_gender",
                CAST("has_subscription" AS INTEGER) AS "has_subscription"
            FROM "public"."KibanaSampleDataEcommerce"
            GROUP BY
                "customer_gender",
                CAST("has_subscription" AS INTEGER)
            LIMIT 500
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "KibanaSampleDataEcommerce.has_subscription".to_string(),
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                limit: Some(500),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_quicksight_cast_with_limit_and_sort() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "customer_gender" AS "customer_gender",
                CAST("has_subscription" AS INTEGER) AS "has_subscription"
            FROM "public"."KibanaSampleDataEcommerce"
            GROUP BY
                "customer_gender",
                CAST("has_subscription" AS INTEGER)
            ORDER BY
                1 NULLS LAST,
                2 NULLS LAST
            LIMIT 250
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert!(logical_plan
            .find_cube_scan_wrapped_sql()
            .wrapped_sql
            .sql
            .contains("LIMIT 250"));

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );
    }

    #[tokio::test]
    async fn test_flatten_projection_aliasing() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT DISTINCT customer_gender
            FROM (
                SELECT
                    order_date,
                    last_mod,
                    customer_gender,
                    notes,
                    taxful_total_price,
                    has_subscription,
                    count,
                    maxPrice,
                    sumPrice,
                    minPrice,
                    avgPrice,
                    countDistinct
                FROM KibanaSampleDataEcommerce
            ) AS anon_1
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
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                ]),
                segments: Some(vec![]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "asc".to_string()
                ],]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_split_projection_aggregate_function_column() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                CAST("KibanaSampleDataEcommerce"."customer_gender" AS TEXT) AS "CUSTOMER_GENDER",
                SUM("KibanaSampleDataEcommerce"."sumPrice") AS "sum:SUM_PRICE:ok"
            FROM
                "public"."KibanaSampleDataEcommerce" "KibanaSampleDataEcommerce"
            GROUP BY 1
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let logical_plan_str = format!("{:?}", logical_plan);
        assert!(!logical_plan_str.contains("SUM(#SUM"));

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.sumPrice".to_string()]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                ]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_filter_in_outer_query_over_date_trunced_column() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            WITH pre_aggregation AS
            (
                SELECT
                    customer_gender,
                    date_trunc('month', "order_date") AS order_date__month,
                    measure(count) AS value
                FROM "KibanaSampleDataEcommerce"
                WHERE customer_gender IN ('male', 'female', 'other')
                GROUP BY 1, 2
            )
            SELECT *
            FROM pre_aggregation
            WHERE (customer_gender = 'female') AND (order_date__month = CAST('2019-01-01 00:00:00' AS TIMESTAMP));
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                ]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: Some(json!(vec![
                        "2019-01-01T00:00:00.000Z".to_string(),
                        "2019-01-31T23:59:59.999Z".to_string()
                    ]))
                }]),
                order: Some(vec![]),
                filters: Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("equals".to_string()),
                        values: Some(vec![
                            "male".to_string(),
                            "female".to_string(),
                            "other".to_string()
                        ]),
                        or: None,
                        and: None
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("equals".to_string()),
                        values: Some(vec!["female".to_string()]),
                        or: None,
                        and: None
                    },
                ]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_sum_avg_null_type() -> Result<(), CubeError> {
        init_testing_logger();

        insta::assert_snapshot!(
            "sum_null_type",
            execute_query(
                "SELECT SUM(x) FROM (SELECT NULL AS x) AS t".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "avg_null_type",
            execute_query(
                "SELECT AVG(x) FROM (SELECT NULL AS x) AS t".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_time_dimension_equals_as_date_range() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                measure(count) AS cnt,
                date_trunc('month', order_date) AS dt
            FROM KibanaSampleDataEcommerce
            WHERE date_trunc('month', order_date) IN (to_timestamp('2019-01-01 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US'))
            GROUP BY 2
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: Some(json!(vec![
                        "2019-01-01T00:00:00.000Z".to_string(),
                        "2019-01-31T23:59:59.999Z".to_string()
                    ]))
                }]),
                order: Some(vec![]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_cast_as_type_pushdown() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query = "
        SELECT
            CAST(CASE customer_gender
                WHEN '1' THEN 2
                WHEN '3' THEN 4
                ELSE 5
            END AS TEXT) AS text,
            CAST(taxful_total_price AS REAL) AS real,
            CAST(taxful_total_price AS DOUBLE PRECISION) AS double,
            CAST(taxful_total_price AS DECIMAL) AS decimal
        FROM KibanaSampleDataEcommerce AS k
        GROUP BY 1, 2, 3, 4
        ORDER BY
            CAST(CASE customer_gender
                WHEN '1' THEN 2
                WHEN '3' THEN 4
                ELSE 5
            END AS TEXT) DESC,
            CAST(taxful_total_price AS REAL),
            CAST(taxful_total_price AS DOUBLE PRECISION),
            CAST(taxful_total_price AS DECIMAL)
        LIMIT 5
        ";

        // Generic
        let query_plan =
            convert_select_to_query_plan(query.to_string(), DatabaseProtocol::PostgreSQL).await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(sql.contains(" AS STRING)"));
        assert!(sql.contains(" AS FLOAT)"));
        assert!(sql.contains(" AS DOUBLE)"));
        assert!(sql.contains(" AS DECIMAL(38,10))"));

        // BigQuery
        let query_plan = convert_select_to_query_plan_customized(
            query.to_string(),
            DatabaseProtocol::PostgreSQL,
            vec![
                ("types/float".to_string(), "FLOAT64".to_string()),
                ("types/double".to_string(), "FLOAT64".to_string()),
                (
                    "types/decimal".to_string(),
                    "BIGDECIMAL({{ precision }},{{ scale }})".to_string(),
                ),
            ],
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(sql.contains(" AS STRING)"));
        assert!(sql.contains(" AS FLOAT64)"));
        assert!(sql.contains(" AS BIGDECIMAL(38,10))"));

        // PostgreSQL
        let query_plan = convert_select_to_query_plan_customized(
            query.to_string(),
            DatabaseProtocol::PostgreSQL,
            vec![
                ("types/string".to_string(), "TEXT".to_string()),
                ("types/float".to_string(), "REAL".to_string()),
                ("types/double".to_string(), "DOUBLE PRECISION".to_string()),
            ],
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(sql.contains(" AS TEXT)"));
        assert!(sql.contains(" AS REAL)"));
        assert!(sql.contains(" AS DOUBLE PRECISION)"));
        assert!(sql.contains(" AS DECIMAL(38,10))"));
    }

    #[tokio::test]
    async fn test_extract_epoch_pushdown() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query = "
            SELECT LOWER(customer_gender),
                   MAX(CAST(FLOOR(EXTRACT(EPOCH FROM order_date) / 31536000) AS bigint)) AS max_years
            FROM KibanaSampleDataEcommerce
            GROUP BY 1
        ";

        // Generic
        let query_plan =
            convert_select_to_query_plan(query.to_string(), DatabaseProtocol::PostgreSQL).await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(sql.contains("EXTRACT(epoch"));

        // Databricks
        let query_plan = convert_select_to_query_plan_customized(
            query.to_string(),
            DatabaseProtocol::PostgreSQL,
            vec![
                ("expressions/timestamp_literal".to_string(), "from_utc_timestamp('{{ value }}', 'UTC')".to_string()),
                ("expressions/extract".to_string(), "{% if date_part|lower == \"epoch\" %}unix_timestamp({{ expr }}){% else %}EXTRACT({{ date_part }} FROM {{ expr }}){% endif %}".to_string()),
            ],
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(!sql.contains("EXTRACT(EPOCH"));
        assert!(sql.contains("unix_timestamp"));
    }

    #[tokio::test]
    async fn test_push_down_to_grouped_query_with_filters() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            select
                sum(t1.sum)
            from (
            select
                customer_gender,
                measure(sumPrice) as sum
            from KibanaSampleDataEcommerce
            where order_date >= '2024-01-01'
                and order_date <= '2024-02-29'
            group by 1
            having measure(sumPrice) >= 5
            ) t1
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.sumPrice".to_string()]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2024-01-01".to_string(),
                        "2024-02-29".to_string()
                    ]))
                }]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.sumPrice".to_string()),
                    operator: Some("gte".to_string()),
                    values: Some(vec!["5".to_string()]),
                    or: None,
                    and: None
                }]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_quicksight_sql_implementation_info() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "quicksight_sql_implementation_info",
            execute_query(
                r#"
                SELECT character_value, version()
                FROM INFORMATION_SCHEMA.SQL_IMPLEMENTATION_INFO
                WHERE implementation_info_id IN ('17','18')
                "#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_thoughtspot_like_escape_push_down() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT CAST("customer_gender" AS TEXT) AS "customer_gender"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE
                "customer_gender" LIKE (
                    '%' || replace(
                        replace(
                            replace(
                                'ale',
                                '!',
                                '!!'
                            ),
                            '%',
                            '!%'
                        ),
                        '_',
                        '!_'
                    ) || '%'
                ) ESCAPE '!'
            GROUP BY 1
            ORDER BY 1
            LIMIT 100
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(sql.contains("LIKE "));
        assert!(sql.contains("ESCAPE "));

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );
    }

    #[tokio::test]
    async fn test_quicksight_sql_sizing() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "quicksight_sql_sizing",
            execute_query(
                r#"
                SELECT supported_value
                FROM INFORMATION_SCHEMA.SQL_SIZING
                WHERE
                    sizing_id = 34
                    or sizing_id = 30
                    or sizing_id = 31
                    or sizing_id = 10005
                    or sizing_id = 32
                    or sizing_id = 35
                    or sizing_id = 107
                    or sizing_id = 97
                    or sizing_id = 99
                    or sizing_id = 100
                    or sizing_id = 101
                "#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_quicksight_stv_slices() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "quicksight_stv_slices",
            execute_query(
                r#"
                with nodes as (
                    select count(distinct node) as node_count
                    from STV_SLICES
                )
                select
                    case
                        when diststyle = 'ALL' then size/cast(nodes.node_count as float)
                        else size
                    end as sizeMBs
                from SVV_TABLE_INFO
                join nodes on 1=1
                where
                    "table" = 'KibanaSampleDataEcommerce'
                    and "schema" = 'public';
                "#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_quicksight_pg_external_schema() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "quicksight_pg_external_schema",
            execute_query(
                r#"
                select nspname
                from pg_external_schema pe
                join pg_namespace pn on pe.esoid = pn.oid
                where
                    nspowner != 1
                    and nspname = 'public'
                "#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_quicksight_regexp_instr() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "quicksight_regexp_instr",
            execute_query(
                r#"SELECT regexp_instr('abcdefg', 'd.f', 3)"#.to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_mysql_nulls_last() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan_customized(
            "
            SELECT customer_gender AS s
            FROM KibanaSampleDataEcommerce AS k
            WHERE LOWER(customer_gender) = 'test'
            GROUP BY 1
            ORDER BY 1 DESC
            "
            .to_string(),
            DatabaseProtocol::PostgreSQL,
            vec![
                ("expressions/sort".to_string(), "{{ expr }} IS NULL {% if nulls_first %}DESC{% else %}ASC{% endif %}, {{ expr }} {% if asc %}ASC{% else %}DESC{% endif %}".to_string()),
            ]
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(sql.contains(" IS NULL DESC, "));
    }

    #[tokio::test]
    async fn test_values_literal_table() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "values_literal_table",
            execute_query(
                r#"SELECT a AS a, b AS b FROM (VALUES (1, 2), (3, 4), (5, 6)) AS t(a, b)"#
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_format_function() -> Result<(), CubeError> {
        // Test: Basic usage with a single string
        let result = execute_query(
            "SELECT format('%s', 'foo') AS formatted_string".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await?;
        insta::assert_snapshot!("formatted_string", result);

        // Test: Basic usage with a single null string
        let result = execute_query(
            "SELECT format('%s', NULL) = '' AS formatted_null_string_is_empty".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await?;
        insta::assert_snapshot!("formatted_null_string_is_empty", result);

        // Test: Basic usage with a multiple strings
        let result = execute_query(
            "SELECT format('%s.%s', 'foo', 'bar') AS formatted_strings".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await?;
        insta::assert_snapshot!("formatted_strings", result);

        // Test: Basic usage with a single identifier
        let result = execute_query(
            "SELECT format('%I', 'column_name') AS formatted_identifier".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await?;
        insta::assert_snapshot!("formatted_identifier", result);

        // Test: Using multiple identifiers
        let result = execute_query(
            "SELECT format('%I, %I', 'table_name', 'column_name') AS formatted_identifiers"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await?;
        insta::assert_snapshot!("formatted_identifiers", result);

        // Test: Unsupported format specifier
        let result = execute_query(
            "SELECT format('%X', 'value') AS unsupported_specifier".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;
        assert!(result.is_err());

        // Test: Format string ending with %
        let result = execute_query(
            "SELECT format('%', 'value') AS invalid_format".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;
        assert!(result.is_err());

        // Test: Quoting necessary for special characters
        let result = execute_query(
            "SELECT format('%I', 'column-name') AS quoted_identifier".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await?;
        insta::assert_snapshot!("quoted_identifier", result);

        // Test: Quoting necessary for reserved keywords
        let result = execute_query(
            "SELECT format('%I', 'select') AS quoted_keyword".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await?;
        insta::assert_snapshot!("quoted_keyword", result);

        Ok(())
    }

    #[tokio::test]
    async fn test_double_window_aggr_sql_push_down() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT
                customer_gender AS customer_gender,
                notes AS notes,
                SUM(SUM(taxful_total_price)) OVER (PARTITION BY customer_gender ORDER BY customer_gender) AS sum,
                AVG(SUM(taxful_total_price)) OVER (PARTITION BY notes ORDER BY notes) AS avg
            FROM KibanaSampleDataEcommerce
            GROUP BY 1, 2
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(sql.contains("OVER (PARTITION BY"));

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );
    }

    #[tokio::test]
    async fn test_date_filter_with_or_and() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                DATE_TRUNC('year', "order_date") AS "y",
                SUM("KibanaSampleDataEcommerce"."sumPrice") AS "m1"
            FROM "KibanaSampleDataEcommerce" AS "KibanaSampleDataEcommerce"
            WHERE
                DATE_TRUNC('year', "order_date") = '2024-01-01T00:00:00Z'::timestamptz
                OR (
                    DATE_TRUNC('year', "order_date") = '2025-01-01T00:00:00Z'::timestamptz
                    AND DATE_TRUNC('month', "order_date") = '2025-01-01T00:00:00Z'::timestamptz
                )
            GROUP BY 1
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.sumPrice".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("year".to_string()),
                    date_range: None
                }]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                            operator: Some("inDateRange".to_string()),
                            values: Some(vec![
                                "2024-01-01T00:00:00.000Z".to_string(),
                                "2024-12-31T23:59:59.999Z".to_string(),
                            ]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: None,
                            operator: None,
                            values: None,
                            or: None,
                            and: Some(vec![
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some(
                                        "KibanaSampleDataEcommerce.order_date".to_string()
                                    ),
                                    operator: Some("inDateRange".to_string()),
                                    values: Some(vec![
                                        "2025-01-01T00:00:00.000Z".to_string(),
                                        "2025-12-31T23:59:59.999Z".to_string(),
                                    ]),
                                    or: None,
                                    and: None,
                                }),
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some(
                                        "KibanaSampleDataEcommerce.order_date".to_string()
                                    ),
                                    operator: Some("inDateRange".to_string()),
                                    values: Some(vec![
                                        "2025-01-01T00:00:00.000Z".to_string(),
                                        "2025-01-31T23:59:59.999Z".to_string(),
                                    ]),
                                    or: None,
                                    and: None,
                                })
                            ]),
                        }),
                    ]),
                    and: None
                }]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_tableau_relative_dates() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                CAST("KibanaSampleDataEcommerce"."customer_gender" AS TEXT) AS "customer_gendder",
                SUM("KibanaSampleDataEcommerce"."sumPrice") AS "sum:sumPrice:ok"
            FROM
                "public"."KibanaSampleDataEcommerce" "KibanaSampleDataEcommerce"
            WHERE
                (
                    CASE
                        WHEN (
                            NOT (
                                CAST(
                                    CAST(
                                        TO_TIMESTAMP(
                                            CAST(
                                                CAST("KibanaSampleDataEcommerce"."order_date" AS TEXT) AS TEXT
                                            ),
                                            'YYYY-MM-DD"T"HH24:MI:SS.MS'
                                        ) AS TIMESTAMP
                                    ) AS DATE
                                ) IS NULL
                            )
                        ) THEN CAST(
                            CAST(
                                TO_TIMESTAMP(
                                    CAST(
                                        CAST("KibanaSampleDataEcommerce"."order_date" AS TEXT) AS TEXT
                                    ),
                                    'YYYY-MM-DD"T"HH24:MI:SS.MS'
                                ) AS TIMESTAMP
                            ) AS DATE
                        )
                        ELSE NULL
                    END
                ) < (TIMESTAMP '2025-01-01 00:00:00.000')
            GROUP BY
                1
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.sumPrice".to_string()]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                order: Some(vec![]),
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                    operator: Some("beforeDate".to_string()),
                    values: Some(vec!["2025-01-01T00:00:00.000Z".to_string()]),
                    ..Default::default()
                }]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_within_group_push_down() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY taxful_total_price) AS pc
            FROM KibanaSampleDataEcommerce
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(sql.contains("WITHIN GROUP (ORDER BY"));

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );
    }

    #[tokio::test]
    async fn test_sort_normalize() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                q2.id,
                q3.id
            FROM KibanaSampleDataEcommerce q1
            LEFT JOIN Logs q2 ON q1.__cubeJoinField = q2.__cubeJoinField
            LEFT JOIN Logs q3 ON q1.__cubeJoinField = q3.__cubeJoinField
            ORDER BY
                q2.id,
                q3.id
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["Logs.id".to_string(),]),
                segments: Some(vec![]),
                order: Some(vec![]),
                ungrouped: Some(true),
                join_hints: Some(vec![
                    vec!["KibanaSampleDataEcommerce".to_string(), "Logs".to_string()],
                    vec!["KibanaSampleDataEcommerce".to_string(), "Logs".to_string()],
                ]),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_push_down_limit_sort_projection() {
        init_testing_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "ta_1"."customer_gender" AS "ca_1",
                DATE_TRUNC('MONTH', CAST("ta_1"."order_date" AS date)) AS "ca_2",
                COALESCE(sum("ta_1"."sumPrice"), 0) AS "ca_3"
            FROM
                "db"."public"."KibanaSampleDataEcommerce" AS "ta_1"
            WHERE
                (
                    "ta_1"."order_date" >= TIMESTAMP '2024-01-01 00:00:00.0'
                    AND "ta_1"."order_date" < TIMESTAMP '2025-01-01 00:00:00.0'
                )
            GROUP BY
                "ca_1",
                "ca_2"
            ORDER BY
                "ca_2" ASC NULLS LAST
            LIMIT
                5000
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.sumPrice".to_string()]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: Some(json!(vec![
                        "2024-01-01T00:00:00.000Z".to_string(),
                        "2024-12-31T23:59:59.999Z".to_string()
                    ])),
                },]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "asc".to_string(),
                ]]),
                limit: Some(5000),
                ..Default::default()
            }
        )
    }

    #[tokio::test]
    async fn test_athena_concat_numbers() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan_customized(
            r#"
            SELECT
                CAST(EXTRACT(YEAR FROM "ta_1"."order_date") || '-' || 1 || '-01' AS DATE) AS "ca_1",
                COALESCE(sum("ta_1"."sumPrice"), 0) AS "ca_2"
                FROM "ovr"."public"."KibanaSampleDataEcommerce" AS "ta_1"
                WHERE ((
                    EXTRACT(DAY FROM "ta_1"."order_date") <= EXTRACT(DAY FROM CURRENT_DATE)
                    AND EXTRACT(MONTH FROM "ta_1"."order_date") = EXTRACT(MONTH FROM CURRENT_DATE)
                ))
                GROUP BY "ca_1"
                ORDER BY "ca_1" ASC NULLS LAST
                LIMIT 5000
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
            vec![(
                "expressions/binary".to_string(),
                "{% if op == \'||\' %}(CAST({{ left }} AS VARCHAR) || \
                    CAST({{ right }} AS VARCHAR))\
                    {% else %}({{ left }} {{ op }} {{ right }}){% endif %}"
                    .to_string(),
            )],
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(sql.contains("CAST(1 AS VARCHAR)"));
    }

    #[tokio::test]
    async fn test_trino_datediff() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan_customized(
            r#"
            SELECT
                KibanaSampleDataEcommerce.id,
                KibanaSampleDataEcommerce.order_date,
                KibanaSampleDataEcommerce.last_mod,
                DATEDIFF(
                    day,
                    KibanaSampleDataEcommerce.order_date,
                    KibanaSampleDataEcommerce.last_mod
                ) as conv_date_diff,
                COUNT(*)
            FROM KibanaSampleDataEcommerce
            WHERE (
                KibanaSampleDataEcommerce.order_date > cast('2025-01-01T00:00:00.000' as timestamp)
                    AND KibanaSampleDataEcommerce.order_date < cast('2025-01-01T23:59:59.999' as timestamp)
                    AND KibanaSampleDataEcommerce.customer_gender = 'test'
            )
            GROUP BY 1, 2, 3, 4
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
            vec![(
                "functions/DATEDIFF".to_string(),
                "DATE_DIFF('{{ date_part }}', {{ args[1] }}, {{ args[2] }})".to_string(),
            )],
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(sql.contains("DATE_DIFF('day', "));
    }

    #[tokio::test]
    async fn test_athena_binary_expr_brackets() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan_customized(
            r#"
            SELECT
                CAST(
                    EXTRACT(YEAR FROM "ta_1"."order_date") || '-' ||
                    ((FLOOR(((EXTRACT(MONTH FROM "ta_1"."order_date") - 1) / NULLIF(3, 0))) * 3) + 1)
                    || '-01' AS DATE
                ) AS "ca_1",
                COALESCE(sum("ta_1"."sumPrice"), 0) AS "ca_2"
            FROM "ovr"."public"."KibanaSampleDataEcommerce" AS "ta_1"
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC NULLS LAST
            LIMIT 10000
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
            vec![(
                "expressions/binary".to_string(),
                "{% if op == \'||\' %}(CAST({{ left }} AS VARCHAR) || \
                    CAST({{ right }} AS VARCHAR))\
                    {% else %}({{ left }} {{ op }} {{ right }}){% endif %}"
                    .to_string(),
            )],
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(sql.contains(" - 1) / 3)"));
    }

    #[tokio::test]
    async fn test_athena_date_part_over_age() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan_customized(
            r#"
            SELECT
                DATE_TRUNC('MONTH', CAST("ta_1"."order_date" AS date)) AS "ca_1",
                COALESCE(sum("ta_1"."sumPrice"), 0) AS "ca_2",
                min(CAST(
                    DATE_PART('year', AGE("ta_1"."order_date", DATE '1970-01-01')) * 12
                    + DATE_PART('month', AGE("ta_1"."order_date", DATE '1970-01-01'))
                    AS int
                )) AS "ca_3",
                min(
                    (MOD(CAST((EXTRACT(MONTH FROM "ta_1"."order_date") - 1) AS numeric), 3) + 1)
                ) AS "ca_4",
                min(CEIL((EXTRACT(MONTH FROM "ta_1"."order_date") / NULLIF(3.0, 0.0)))) AS "ca_5"
            FROM "ovr"."public"."KibanaSampleDataEcommerce" AS "ta_1"
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC NULLS LAST
            LIMIT 5000
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
            vec![(
                "functions/DATEDIFF".to_string(),
                "DATE_DIFF('{{ date_part }}', {{ args[1] }}, {{ args[2] }})".to_string(),
            )],
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(sql.contains("DATE_DIFF('month', "));
    }

    #[tokio::test]
    async fn test_athena_date_minus_date() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan_customized(
            r#"
            SELECT
                DATE_TRUNC('week', "ta_1"."order_date") AS "ca_1",
                COALESCE(sum("ta_1"."sumPrice"), 0) AS "ca_2",
                min((CEIL((((
                    CAST("ta_1"."order_date" AS date) - CAST(DATE '1970-01-01' AS date) + 1 + 7) - 4
                ) / NULLIF(7.0, 0.0))) - 1)) AS "ca_3",
                min(FLOOR(((
                    EXTRACT(DAY FROM (
                        ("ta_1"."order_date") + ((4 - (MOD(CAST((
                            CAST("ta_1"."order_date" AS date) - CAST(DATE '1970-01-01' AS date) + 3
                        ) AS numeric), 7) + 1))) * INTERVAL '1 day')) + 6
                ) / NULLIF(7, 0)))) AS "ca_4",
                min(
                    (MOD(CAST((EXTRACT(MONTH FROM "ta_1"."order_date") - 1) AS numeric), 3) + 1)
                ) AS "ca_6",
                min(CEIL((EXTRACT(MONTH FROM "ta_1"."order_date") / NULLIF(3.0, 0.0)))) AS "ca_7"
            FROM "ovr"."public"."KibanaSampleDataEcommerce" AS "ta_1"
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC NULLS LAST
            LIMIT 5000
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
            vec![(
                "functions/DATEDIFF".to_string(),
                "DATE_DIFF('{{ date_part }}', {{ args[1] }}, {{ args[2] }})".to_string(),
            )],
        )
        .await;

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(sql.contains("DATE_DIFF('day', "));
    }

    #[tokio::test]
    async fn test_count_over_joined_cubes() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT COUNT(*)
            FROM (
                SELECT
                    t1.id AS id,
                    t2.read AS read
                FROM KibanaSampleDataEcommerce t1
                LEFT JOIN Logs t2 ON t1.__cubeJoinField = t2.__cubeJoinField
            ) t
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(sql.contains("COUNT(*)"));
        assert!(sql.contains("KibanaSampleDataEcommerce"));
        assert!(sql.contains("Logs"));

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );
    }

    #[tokio::test]
    async fn test_where_subquery_sql_push_down_measure_fn() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            WITH top_customers AS (
                SELECT
                    KibanaSampleDataEcommerce.id,
                    MEASURE(KibanaSampleDataEcommerce.sumPrice) AS sum_value
                FROM KibanaSampleDataEcommerce
                GROUP BY 1
                ORDER BY 2 DESC
                LIMIT 3
            )
            SELECT
                KibanaSampleDataEcommerce.id,
                MEASURE(KibanaSampleDataEcommerce.sumPrice) AS sum_value
            FROM KibanaSampleDataEcommerce
            WHERE KibanaSampleDataEcommerce.id IN (
                SELECT id FROM top_customers
            )
            GROUP BY 1
            ORDER BY 1
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        assert!(sql.contains("IN (SELECT"));
        assert!(sql.contains(r#"\\\"limit\\\": 3\\n"#));

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );
    }

    #[tokio::test]
    async fn test_subquery_inner_context() {
        if !Rewriter::sql_push_down_enabled() {
            return;
        }
        init_testing_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT customer_gender
            FROM KibanaSampleDataEcommerce
            WHERE customer_gender IN (
                SELECT customer_gender
                FROM KibanaSampleDataEcommerce
                WHERE KibanaSampleDataEcommerce.order_date > '2025-01-01'
                GROUP BY 1
            )
            GROUP BY 1
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
        println!("Generated SQL: {}", sql);
        assert!(sql.contains("2025-01-01"));
        assert!(sql.contains("customer_gender} IN (SELECT"));

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!(
            "Physical plan: {}",
            displayable(physical_plan.as_ref()).indent()
        );
    }
}
