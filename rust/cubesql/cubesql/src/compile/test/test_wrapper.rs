use cubeclient::models::{V1LoadRequestQuery, V1LoadRequestQueryTimeDimension};
use datafusion::{physical_plan::displayable, scalar::ScalarValue};
use pretty_assertions::assert_eq;
use regex::Regex;
use serde_json::json;
use std::sync::Arc;

use crate::{
    compile::{
        engine::df::scan::MemberField,
        rewrite::rewriter::Rewriter,
        test::{
            convert_select_to_query_plan, convert_select_to_query_plan_customized,
            convert_select_to_query_plan_with_config, init_testing_logger, LogicalPlanTestUtils,
        },
        DatabaseProtocol,
    },
    config::ConfigObjImpl,
    transport::TransportLoadRequestQuery,
};

#[tokio::test]
async fn test_simple_wrapper() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT COALESCE(customer_gender, 'N/A', 'NN'), AVG(avgPrice) mp FROM KibanaSampleDataEcommerce a GROUP BY 1"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let logical_plan = query_plan.as_logical_plan();
    assert!(logical_plan
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains("COALESCE"));

    let _physical_plan = query_plan.as_physical_plan().await.unwrap();
}

#[tokio::test]
async fn test_wrapper_group_by_rollup() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT customer_gender, notes, AVG(avgPrice) mp FROM KibanaSampleDataEcommerce a GROUP BY 1, ROLLUP(2)"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let logical_plan = query_plan.as_logical_plan();
    let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
    assert!(sql.contains("Rollup"));

    let _physical_plan = query_plan.as_physical_plan().await.unwrap();
}

#[tokio::test]
async fn test_wrapper_group_by_rollup_with_aliases() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT customer_gender as \"customer_gender1\", notes as \"notes\", AVG(avgPrice) mp FROM KibanaSampleDataEcommerce a GROUP BY ROLLUP(1, 2)"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let logical_plan = query_plan.as_logical_plan();
    let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
    assert!(sql.contains("Rollup"));

    let _physical_plan = query_plan.as_physical_plan().await.unwrap();
}

#[tokio::test]
async fn test_wrapper_group_by_rollup_nested() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT customer_gender, notes, avg(mp) from (SELECT customer_gender, notes, avg(avgPrice) mp FROM KibanaSampleDataEcommerce a GROUP BY 1, 2) b GROUP BY ROLLUP(1, 2)"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let logical_plan = query_plan.as_logical_plan();
    let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
    assert!(sql.contains("ROLLUP(1, 2)"));

    let _physical_plan = query_plan.as_physical_plan().await.unwrap();
}

#[tokio::test]
async fn test_wrapper_group_by_rollup_nested_from_asterisk() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT customer_gender, notes, avg(avgPrice) from (SELECT * FROM KibanaSampleDataEcommerce) b GROUP BY ROLLUP(1, 2) ORDER BY 1"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let logical_plan = query_plan.as_logical_plan();
    let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
    assert!(sql.contains("Rollup"));

    let _physical_plan = query_plan.as_physical_plan().await.unwrap();
}

#[tokio::test]
async fn test_wrapper_group_by_rollup_nested_with_aliases() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT customer_gender as \"gender\", notes as \"notes\", avg(mp) from (SELECT customer_gender, notes, avg(avgPrice) mp FROM KibanaSampleDataEcommerce a GROUP BY 1, 2) b GROUP BY ROLLUP(1, 2)"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let logical_plan = query_plan.as_logical_plan();
    let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
    assert!(sql.contains("ROLLUP(1, 2)"));

    let _physical_plan = query_plan.as_physical_plan().await.unwrap();
}

#[tokio::test]
async fn test_wrapper_group_by_rollup_nested_complex() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT customer_gender, notes, order_date, last_mod, avg(mp) from \
            (SELECT customer_gender, notes, order_date, last_mod, avg(avgPrice) mp FROM KibanaSampleDataEcommerce a GROUP BY 1, 2, 3, 4) b \
            GROUP BY ROLLUP(1), ROLLUP(2), 3, CUBE(4)"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let logical_plan = query_plan.as_logical_plan();
    let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
    assert!(sql.contains("ROLLUP(1), ROLLUP(2), 3, CUBE(4)"));

    let _physical_plan = query_plan.as_physical_plan().await.unwrap();
}

#[tokio::test]
async fn test_wrapper_group_by_rollup_placeholders() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT customer_gender, notes, AVG(avgPrice) mp FROM KibanaSampleDataEcommerce a GROUP BY ROLLUP(1, 2)"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let logical_plan = query_plan.as_logical_plan();
    let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
    assert!(sql.contains("Rollup"));

    let _physical_plan = query_plan.as_physical_plan().await.unwrap();
}

#[tokio::test]
async fn test_wrapper_group_by_cube() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT customer_gender, notes, AVG(avgPrice) mp FROM KibanaSampleDataEcommerce a GROUP BY CUBE(customer_gender, notes)"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let logical_plan = query_plan.as_logical_plan();
    let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
    assert!(sql.contains("Cube"));

    let _physical_plan = query_plan.as_physical_plan().await.unwrap();
}

#[tokio::test]
async fn test_wrapper_group_by_rollup_complex() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT customer_gender, notes, has_subscription, AVG(avgPrice) mp FROM KibanaSampleDataEcommerce a GROUP BY ROLLUP(customer_gender, notes), has_subscription"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let logical_plan = query_plan.as_logical_plan();
    let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
    assert!(sql.contains("Rollup"));

    let _physical_plan = query_plan.as_physical_plan().await.unwrap();
}

#[tokio::test]
async fn test_simple_subquery_wrapper_projection_empty_source() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT (SELECT 'male' where 1  group by 'male' having 1 order by 'male' limit 1) as gender, avgPrice FROM KibanaSampleDataEcommerce a"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let logical_plan = query_plan.as_logical_plan();
    let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
    assert!(sql.contains("(SELECT"));
    assert!(sql.contains("utf8__male__"));

    let _physical_plan = query_plan.as_physical_plan().await.unwrap();
    //println!("phys plan {:?}", physical_plan);
}

#[tokio::test]
async fn test_simple_subquery_wrapper_filter_empty_source() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT avgPrice FROM KibanaSampleDataEcommerce a where customer_gender = (SELECT 'male' )"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await;

    let logical_plan = query_plan.as_logical_plan();
    let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
    assert!(sql.contains("(SELECT"));
    assert!(sql.contains("utf8__male__"));

    let _physical_plan = query_plan.as_physical_plan().await.unwrap();
    //println!("phys plan {:?}", physical_plan);
}

#[tokio::test]
async fn test_simple_subquery_wrapper_projection_aggregate_empty_source() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT (SELECT 'male'), avg(avgPrice) FROM KibanaSampleDataEcommerce a GROUP BY 1"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await;

    let logical_plan = query_plan.as_logical_plan();
    let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
    assert!(sql.contains("(SELECT"));
    assert!(sql.contains("utf8__male__"));

    let _physical_plan = query_plan.as_physical_plan().await.unwrap();
}

#[tokio::test]
async fn test_simple_subquery_wrapper_filter_in_empty_source() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT customer_gender, avgPrice FROM KibanaSampleDataEcommerce a where customer_gender in (select 'male')"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let logical_plan = query_plan.as_logical_plan();
    let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
    assert!(sql.contains("IN (SELECT"));
    assert!(sql.contains("utf8__male__"));

    let _physical_plan = query_plan.as_physical_plan().await.unwrap();
}

#[tokio::test]
async fn test_simple_subquery_wrapper_filter_and_projection_empty_source() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT (select 'male'), avgPrice FROM KibanaSampleDataEcommerce a where customer_gender in (select 'female')"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let logical_plan = query_plan.as_logical_plan();

    let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;
    assert!(sql.contains("IN (SELECT"));
    assert!(sql.contains("(SELECT"));
    assert!(sql.contains("utf8__male__"));
    assert!(sql.contains("utf8__female__"));

    let _physical_plan = query_plan.as_physical_plan().await.unwrap();
}

#[tokio::test]
async fn test_simple_subquery_wrapper_projection() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT (SELECT customer_gender FROM KibanaSampleDataEcommerce LIMIT 1) as gender, avgPrice FROM KibanaSampleDataEcommerce a"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let logical_plan = query_plan.as_logical_plan();
    assert!(logical_plan
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains("(SELECT"));
    assert!(logical_plan
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains("\\\\\\\"limit\\\\\\\": 1"));

    let _physical_plan = query_plan.as_physical_plan().await.unwrap();
}

#[tokio::test]
async fn test_simple_subquery_wrapper_projection_aggregate() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT (SELECT customer_gender FROM KibanaSampleDataEcommerce WHERE customer_gender = 'male' LIMIT 1), avg(avgPrice) FROM KibanaSampleDataEcommerce a GROUP BY 1"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let logical_plan = query_plan.as_logical_plan();
    assert!(logical_plan
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains("(SELECT"));

    let _physical_plan = query_plan.as_physical_plan().await.unwrap();
}

#[tokio::test]
async fn test_simple_subquery_wrapper_filter_equal() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT customer_gender, avgPrice FROM KibanaSampleDataEcommerce a where customer_gender = (select customer_gender from KibanaSampleDataEcommerce limit 1)"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let logical_plan = query_plan.as_logical_plan();
    assert!(logical_plan
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains("(SELECT"));
    assert!(logical_plan
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains("\\\\\\\"limit\\\\\\\": 1"));

    let _physical_plan = query_plan.as_physical_plan().await.unwrap();
}

#[tokio::test]
async fn test_simple_subquery_wrapper_filter_in() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT customer_gender, avgPrice FROM KibanaSampleDataEcommerce a where customer_gender in (select customer_gender from KibanaSampleDataEcommerce)"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let logical_plan = query_plan.as_logical_plan();
    assert!(logical_plan
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains("IN (SELECT"));

    let _physical_plan = query_plan.as_physical_plan().await.unwrap();
}

#[tokio::test]
async fn test_simple_subquery_wrapper_filter_and_projection() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT (select customer_gender from KibanaSampleDataEcommerce limit 1), avgPrice FROM KibanaSampleDataEcommerce a where customer_gender in (select customer_gender from KibanaSampleDataEcommerce)"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let logical_plan = query_plan.as_logical_plan();

    assert!(logical_plan
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains("IN (SELECT"));

    let _physical_plan = query_plan.as_physical_plan().await.unwrap();
}

// TODO add more time zones
// TODO add more TS syntax variants
// TODO add TIMESTAMPTZ variant
/// Using TIMESTAMP WITH TIME ZONE with actual timezone in wrapper should render proper timestamptz in SQL
#[tokio::test]
async fn test_wrapper_timestamptz() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
SELECT
    customer_gender
FROM KibanaSampleDataEcommerce
WHERE
    order_date >= TIMESTAMP WITH TIME ZONE '2024-02-03T04:05:06Z'
    AND
--   This filter should trigger pushdown
    LOWER(customer_gender) = 'male'
GROUP BY
    1
;
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

    assert!(query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains(
            "${KibanaSampleDataEcommerce.order_date} >= timestamptz '2024-02-03T04:05:06.000Z'"
        ));
}

// TODO add more time zones
// TODO add more TS syntax variants
// TODO add TIMESTAMPTZ variant
/// Using TIMESTAMP WITH TIME ZONE with actual timezone in ungrouped wrapper should render proper timestamptz in SQL
#[tokio::test]
async fn test_wrapper_timestamptz_ungrouped() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
SELECT
    customer_gender
FROM KibanaSampleDataEcommerce
WHERE
    order_date >= TIMESTAMP WITH TIME ZONE '2024-02-03T04:05:06Z'
    AND
--   This filter should trigger pushdown
    LOWER(customer_gender) = 'male'
;
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

    assert!(query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains(
            "${KibanaSampleDataEcommerce.order_date} >= timestamptz '2024-02-03T04:05:06.000Z'"
        ));
}

/// Using NOW() in wrapper should render NOW() in SQL
#[tokio::test]
async fn test_wrapper_now() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
SELECT
    customer_gender
FROM KibanaSampleDataEcommerce
WHERE
    order_date >= NOW()
    AND
--   This filter should trigger pushdown
    LOWER(customer_gender) = 'male'
GROUP BY
    1
;
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

    assert!(query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains("${KibanaSampleDataEcommerce.order_date} >= NOW()"));
}

/// Using NOW() in ungrouped wrapper should render NOW() in SQL
#[tokio::test]
async fn test_wrapper_now_ungrouped() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
SELECT
    customer_gender
FROM KibanaSampleDataEcommerce
WHERE
    order_date >= NOW()
    AND
--   This filter should trigger pushdown
    LOWER(customer_gender) = 'male'
;
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

    assert!(query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains("${KibanaSampleDataEcommerce.order_date} >= NOW()"));
}

#[tokio::test]
async fn test_case_wrapper() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT CASE WHEN COALESCE(customer_gender, 'N/A', 'NN') = 'female' THEN 'f' ELSE 'm' END, AVG(avgPrice) mp FROM KibanaSampleDataEcommerce a GROUP BY 1"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let logical_plan = query_plan.as_logical_plan();
    assert!(logical_plan
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains("CASE WHEN"));

    let physical_plan = query_plan.as_physical_plan().await.unwrap();
    println!(
        "Physical plan: {}",
        displayable(physical_plan.as_ref()).indent()
    );
}

#[tokio::test]
async fn test_case_wrapper_distinct() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        r#"SELECT CASE WHEN customer_gender = 'female' THEN 'f' ELSE 'm' END, COUNT(DISTINCT countDistinct) mp
            FROM KibanaSampleDataEcommerce a
            WHERE
              (
                (
                  ( a.order_date ) >= '2024-01-01'
                  AND ( a.order_date ) < '2024-02-01'
                )
              )
            GROUP BY 1"#
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let logical_plan = query_plan.as_logical_plan();
    assert!(logical_plan
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains("CASE WHEN"));

    let physical_plan = query_plan.as_physical_plan().await.unwrap();
    println!(
        "Physical plan: {}",
        displayable(physical_plan.as_ref()).indent()
    );
}

#[tokio::test]
async fn test_case_wrapper_alias_with_order() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT CASE WHEN customer_gender = 'female' THEN 'f' ELSE 'm' END AS \"f822c516-3515-11c2-8464-5d4845a02f73\", AVG(avgPrice) mp FROM KibanaSampleDataEcommerce a GROUP BY CASE WHEN customer_gender = 'female' THEN 'f' ELSE 'm' END ORDER BY CASE WHEN customer_gender = 'female' THEN 'f' ELSE 'm' END NULLS FIRST LIMIT 500"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let logical_plan = query_plan.as_logical_plan();
    assert!(logical_plan
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains("ORDER BY \"a\".\"case_when_a_cust\""));

    let physical_plan = query_plan.as_physical_plan().await.unwrap();
    println!(
        "Physical plan: {}",
        displayable(physical_plan.as_ref()).indent()
    );
}

#[tokio::test]
async fn test_case_wrapper_ungrouped() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT CASE WHEN customer_gender = 'female' THEN 'f' ELSE 'm' END, AVG(avgPrice) mp FROM KibanaSampleDataEcommerce a GROUP BY 1"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let logical_plan = query_plan.as_logical_plan();
    assert!(logical_plan
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains("CASE WHEN"));

    let physical_plan = query_plan.as_physical_plan().await.unwrap();
    println!(
        "Physical plan: {}",
        displayable(physical_plan.as_ref()).indent()
    );
}

#[tokio::test]
async fn test_case_wrapper_non_strict_match() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let mut config = ConfigObjImpl::default();

    config.disable_strict_agg_type_match = true;

    let query_plan = convert_select_to_query_plan_with_config(
        "SELECT CASE WHEN customer_gender = 'female' THEN 'f' ELSE 'm' END, SUM(avgPrice) mp FROM KibanaSampleDataEcommerce a GROUP BY 1"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
        Arc::new(config)
    )
        .await;

    let logical_plan = query_plan.as_logical_plan();
    assert!(logical_plan
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains("CASE WHEN"));

    let physical_plan = query_plan.as_physical_plan().await.unwrap();
    println!(
        "Physical plan: {}",
        displayable(physical_plan.as_ref()).indent()
    );
}

#[tokio::test]
async fn test_case_wrapper_ungrouped_sorted() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT CASE WHEN customer_gender = 'female' THEN 'f' ELSE 'm' END, AVG(avgPrice) mp FROM KibanaSampleDataEcommerce a GROUP BY 1 ORDER BY 1 DESC"
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
        .contains("ORDER BY"));
}

#[tokio::test]
async fn test_case_wrapper_ungrouped_sorted_aliased() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT x FROM (SELECT CASE WHEN customer_gender = 'female' THEN 'f' ELSE 'm' END x, AVG(avgPrice) mp FROM KibanaSampleDataEcommerce a GROUP BY 1 ORDER BY 1 DESC) b"
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
        // TODO test without depend on column name
        .contains("ORDER BY \"a\".\"case_when"));
}

#[tokio::test]
async fn test_case_wrapper_with_internal_limit() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT CASE WHEN customer_gender = 'female' THEN 'f' ELSE 'm' END, AVG(avgPrice) mp FROM KibanaSampleDataEcommerce a GROUP BY 1 LIMIT 1123"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let logical_plan = query_plan.as_logical_plan();
    assert!(logical_plan
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains("CASE WHEN"));

    assert!(
        logical_plan
            .find_cube_scan_wrapped_sql()
            .wrapped_sql
            .sql
            .contains("1123"),
        "SQL contains 1123: {}",
        logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql
    );

    let physical_plan = query_plan.as_physical_plan().await.unwrap();
    println!(
        "Physical plan: {}",
        displayable(physical_plan.as_ref()).indent()
    );
}

#[tokio::test]
async fn test_case_wrapper_with_system_fields() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT CASE WHEN customer_gender = 'female' THEN 'f' ELSE 'm' END, __user, __cubeJoinField, AVG(avgPrice) mp FROM KibanaSampleDataEcommerce a GROUP BY 1, 2, 3 LIMIT 1123"
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
            .contains(
                "\\\"cube_name\\\":\\\"KibanaSampleDataEcommerce\\\",\\\"alias\\\":\\\"user\\\""
            ),
        r#"SQL contains `\"cube_name\":\"KibanaSampleDataEcommerce\",\"alias\":\"user\"` {}"#,
        logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql
    );

    let physical_plan = query_plan.as_physical_plan().await.unwrap();
    println!(
        "Physical plan: {}",
        displayable(physical_plan.as_ref()).indent()
    );
}

#[tokio::test]
async fn test_case_wrapper_with_limit() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT * FROM (SELECT CASE WHEN customer_gender = 'female' THEN 'f' ELSE 'm' END, AVG(avgPrice) mp FROM KibanaSampleDataEcommerce a GROUP BY 1) q LIMIT 1123"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let logical_plan = query_plan.as_logical_plan();
    assert!(logical_plan
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains("CASE WHEN"));

    assert!(
        logical_plan
            .find_cube_scan_wrapped_sql()
            .wrapped_sql
            .sql
            .contains("1123"),
        "SQL contains 1123: {}",
        logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql
    );

    let physical_plan = query_plan.as_physical_plan().await.unwrap();
    println!(
        "Physical plan: {}",
        displayable(physical_plan.as_ref()).indent()
    );
}

#[tokio::test]
async fn test_case_wrapper_with_null() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT CASE WHEN taxful_total_price IS NULL THEN NULL WHEN taxful_total_price < taxful_total_price * 2 THEN COALESCE(taxful_total_price, 0, 0) END, AVG(avgPrice) FROM KibanaSampleDataEcommerce GROUP BY 1"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let logical_plan = query_plan.as_logical_plan();
    assert!(logical_plan
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains("CASE WHEN"));

    let physical_plan = query_plan.as_physical_plan().await.unwrap();
    println!(
        "Physical plan: {}",
        displayable(physical_plan.as_ref()).indent()
    );
}

#[tokio::test]
async fn test_case_wrapper_ungrouped_on_dimension() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT CASE WHEN SUM(taxful_total_price) > 0 THEN SUM(taxful_total_price) ELSE 0 END FROM KibanaSampleDataEcommerce a"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
        .await;

    let physical_plan = query_plan.as_physical_plan().await.unwrap();
    println!(
        "Physical plan: {}",
        displayable(physical_plan.as_ref()).indent()
    );
}

#[tokio::test]
async fn test_case_wrapper_escaping() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan_customized(
        "SELECT CASE WHEN customer_gender = '\\`' THEN COALESCE(customer_gender, 'N/A', 'NN') ELSE 'N/A' END as \"\\`\", AVG(avgPrice) FROM KibanaSampleDataEcommerce a GROUP BY 1".to_string(),
        DatabaseProtocol::PostgreSQL,
        vec![
            ("expressions/binary".to_string(), "{{ left }} \\`{{ op }} {{ right }}".to_string())
        ],
    ).await;

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
        // Expect 6 backslashes as output is JSON and it's escaped one more time
        .contains("\\\\\\\\\\\\`"));
}

/// Test aliases for grouped CubeScan in wrapper
/// qualifiers from join should get remapped to single from alias
/// long generated aliases from Datafusion should get shortened
#[tokio::test]
async fn test_join_wrapper_cubescan_aliasing() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
WITH
-- This subquery should be represented as CubeScan(ungrouped=false) inside CubeScanWrapper
cube_scan_subq AS (
    SELECT
        logs_alias.content logs_content,
        DATE_TRUNC('month', kibana_alias.last_mod) last_mod_month,
        kibana_alias.__user AS cube_user,
        1 AS literal,
        -- Columns without aliases should also work
        DATE_TRUNC('month', kibana_alias.order_date),
        kibana_alias.__cubeJoinField,
        2,
        CASE
            WHEN sum(kibana_alias."sumPrice") IS NOT NULL
                THEN sum(kibana_alias."sumPrice")
            ELSE 0
            END sum_price
    FROM KibanaSampleDataEcommerce kibana_alias
    JOIN Logs logs_alias
    ON kibana_alias.__cubeJoinField = logs_alias.__cubeJoinField
    GROUP BY 1,2,3,4,5,6,7
),
filter_subq AS (
    SELECT
        Logs.content logs_content_filter
    FROM Logs
    GROUP BY
        logs_content_filter
)
SELECT
    -- Should use SELECT * here to reference columns without aliases.
    -- But it's broken ATM in DF, initial plan contains `Projection: ... #__subquery-0.logs_content_filter` on top, but it should not be there
    -- TODO fix it
    logs_content,
    cube_user,
    literal
FROM cube_scan_subq
WHERE
    -- This subquery filter should trigger wrapping of whole query
    logs_content IN (
        SELECT
            logs_content_filter
        FROM filter_subq
    )
;
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

    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec!["KibanaSampleDataEcommerce.sumPrice".to_string(),]),
            dimensions: Some(vec!["Logs.content".to_string(),]),
            time_dimensions: Some(vec![
                V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.last_mod".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: None,
                },
                V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: None,
                },
            ]),
            segments: Some(vec![]),
            order: Some(vec![]),
            ..Default::default()
        }
    );

    assert_eq!(
        logical_plan.find_cube_scan().member_fields,
        vec![
            MemberField::Member("Logs.content".to_string()),
            MemberField::Member("KibanaSampleDataEcommerce.last_mod.month".to_string()),
            MemberField::Literal(ScalarValue::Utf8(None)),
            MemberField::Literal(ScalarValue::Int64(Some(1))),
            MemberField::Member("KibanaSampleDataEcommerce.order_date.month".to_string()),
            MemberField::Literal(ScalarValue::Utf8(None)),
            MemberField::Literal(ScalarValue::Int64(Some(2))),
            MemberField::Member("KibanaSampleDataEcommerce.sumPrice".to_string()),
        ],
    );

    // Check that all aliases from different tables have same qualifier, and that names are simple and short
    // logs_content => logs_alias.content
    // last_mod_month => DATE_TRUNC('month', kibana_alias.last_mod),
    // sum_price => CASE WHEN sum(kibana_alias."sumPrice") ... END
    let content_re = Regex::new(r#""logs_alias"."[a-zA-Z0-9_]{1,16}" "logs_content""#).unwrap();
    assert!(content_re.is_match(&sql));
    let last_mod_month_re =
        Regex::new(r#""logs_alias"."[a-zA-Z0-9_]{1,16}" "last_mod_month""#).unwrap();
    assert!(last_mod_month_re.is_match(&sql));
    let sum_price_re = Regex::new(r#"CASE WHEN \("logs_alias"."[a-zA-Z0-9_]{1,16}" IS NOT NULL\) THEN "logs_alias"."[a-zA-Z0-9_]{1,16}" ELSE 0 END "sum_price""#)
        .unwrap();
    assert!(sum_price_re.is_match(&sql));
    let cube_user_re = Regex::new(r#""logs_alias"."[a-zA-Z0-9_]{1,16}" "cube_user""#).unwrap();
    assert!(cube_user_re.is_match(&sql));
    let literal_re = Regex::new(r#""logs_alias"."[a-zA-Z0-9_]{1,16}" "literal""#).unwrap();
    assert!(literal_re.is_match(&sql));
}

/// Test that WrappedSelect(... limit=Some(0) ...) will render it correctly
#[tokio::test]
async fn test_wrapper_limit_zero() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
            SELECT
                MIN(t.a)
            FROM (
                SELECT
                    MAX(order_date) AS a
                FROM
                    KibanaSampleDataEcommerce
                LIMIT 10
            ) t LIMIT 0
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
        .contains("LIMIT 0"));

    let _physical_plan = query_plan.as_physical_plan().await.unwrap();
}

/// Tests that Aggregation(Filter(CubeScan(ungrouped=true))) with expresions in filter
/// can be executed as a single ungrouped=false load query
#[tokio::test]
async fn test_wrapper_filter_flatten() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
            SELECT
                customer_gender,
                SUM(sumPrice)
            FROM
                KibanaSampleDataEcommerce
            WHERE
                LOWER(customer_gender) = 'male'
            GROUP BY
                1
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
        TransportLoadRequestQuery {
            measures: Some(vec![json!({
                "cube_name": "KibanaSampleDataEcommerce",
                "alias": "sum_kibanasample",
                "cube_params": ["KibanaSampleDataEcommerce"],
                // This is grouped query, KibanaSampleDataEcommerce.sumPrice is correct in this context
                // SUM(sumPrice) will be incrrect here, it would lead to SUM(SUM(sql)) in generated query
                "expr": "${KibanaSampleDataEcommerce.sumPrice}",
                "grouping_set": null,
            })
            .to_string(),]),
            dimensions: Some(vec![json!({
                "cube_name": "KibanaSampleDataEcommerce",
                "alias": "customer_gender",
                "cube_params": ["KibanaSampleDataEcommerce"],
                "expr": "${KibanaSampleDataEcommerce.customer_gender}",
                "grouping_set": null,
            })
            .to_string(),]),
            segments: Some(vec![json!({
                "cube_name": "KibanaSampleDataEcommerce",
                "alias": "lower_kibanasamp",
                "cube_params": ["KibanaSampleDataEcommerce"],
                "expr": "(LOWER(${KibanaSampleDataEcommerce.customer_gender}) = $0$)",
                "grouping_set": null,
            })
            .to_string(),]),
            time_dimensions: None,
            order: Some(vec![]),
            limit: Some(50000),
            ..Default::default()
        }
    );
}

/// Regular aggregation over CubeScan(limit=n, ungrouped=true) is NOT pushed to CubeScan
/// and inner ungrouped CubeScan should have both proper members and limit
#[tokio::test]
async fn wrapper_agg_over_limit() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
        SELECT
            customer_gender
        FROM (
            SELECT
                customer_gender
            FROM
                KibanaSampleDataEcommerce
            LIMIT 5
        ) scan
        GROUP BY
            1
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
            dimensions: Some(vec![
                "KibanaSampleDataEcommerce.customer_gender".to_string(),
            ]),
            segments: Some(vec![]),
            order: Some(vec![]),
            limit: Some(5),
            ungrouped: Some(true),
            ..Default::default()
        }
    );

    assert!(logical_plan
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains("\"limit\": 5"));
    assert!(query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains("\"ungrouped\": true"));
}

/// Aggregation(dimension) over CubeScan(limit=n, ungrouped=true) is NOT pushed to CubeScan
/// and inner ungrouped CubeScan should have both proper members and limit
#[tokio::test]
async fn wrapper_agg_dimension_over_limit() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
        SELECT
            MAX(customer_gender)
        FROM (
            SELECT
                customer_gender
            FROM
                KibanaSampleDataEcommerce
            LIMIT 5
        ) scan
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
            dimensions: Some(vec![
                "KibanaSampleDataEcommerce.customer_gender".to_string(),
            ]),
            segments: Some(vec![]),
            order: Some(vec![]),
            limit: Some(5),
            ungrouped: Some(true),
            ..Default::default()
        }
    );

    assert!(logical_plan
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains("\"limit\": 5"));
    assert!(query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains("\"ungrouped\": true"));
}

// TODO allow number measures and add test for those
/// Projection(Filter(CubeScan(ungrouped))) should have projection expressions pushed down to Cube
#[tokio::test]
async fn wrapper_projection_flatten_simple_measure() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
SELECT
  maxPrice
FROM
  MultiTypeCube
WHERE
  LOWER(CAST(dim_num0 AS TEXT)) = 'all'
;
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

    let request = query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .request;
    assert_eq!(request.measures.unwrap().len(), 1);
    assert_eq!(request.dimensions.unwrap().len(), 0);
}

#[tokio::test]
async fn wrapper_duplicated_members() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        format!(
            r#"
SELECT
    "foo",
    "bar",
    CASE
        WHEN "bar" IS NOT NULL
        THEN 1
        ELSE 0
        END
    AS "bar_expr"
FROM (
    SELECT
        "rows"."foo" AS "foo",
        "rows"."bar" AS "bar"
    FROM (
        SELECT
            "dim_str0" AS "foo",
            "dim_str0" AS "bar"
        FROM MultiTypeCube
    ) "rows"
    GROUP BY
        "foo",
        "bar"
) "_"
ORDER BY
    "bar_expr"
LIMIT 1
;
        "#
        )
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
    // Generated SQL should contain realiasing of one member to two columns
    assert!(logical_plan
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains(r#""foo" "foo""#));
    assert!(logical_plan
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains(r#""foo" "bar""#));
}

/// Simple wrapper with cast should have explicit members, not zero
#[tokio::test]
async fn wrapper_cast_limit_explicit_members() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
        SELECT
            CAST(dim_date0 AS DATE) AS "dim_date0"
        FROM
            MultiTypeCube
        LIMIT 10
        ;
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

    // Query should mention just a single member
    let request = query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .request;
    assert_eq!(request.measures.unwrap().len(), 1);
    assert_eq!(request.dimensions.unwrap().len(), 0);
}

#[tokio::test]
async fn wrapper_typed_null() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
        SELECT
            dim_str0,
            AVG(avgPrice),
            CASE
                WHEN SUM((NULLIF(0.0, 0.0))) IS NOT NULL THEN SUM((NULLIF(0.0, 0.0)))
                ELSE 0
                END
        FROM MultiTypeCube
        GROUP BY 1
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

    assert!(query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains("SUM(CAST(NULL AS DOUBLE))"));
}

/// Tests that exactly same expression in projection and filter have correct alias after rewriting
#[tokio::test]
async fn test_same_expression_in_projection_and_filter() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
SELECT
    DATE_TRUNC('day', CAST(dim_date0 AS TIMESTAMP))
FROM MultiTypeCube
WHERE
    DATE_TRUNC('day', CAST(dim_date0 AS TIMESTAMP)) >=
     '2025-01-01'
GROUP BY
    1
;
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

    let request = query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .request;
    let dimensions = request.dimensions.unwrap();
    assert_eq!(dimensions.len(), 1);
    let dimension = &dimensions[0];
    assert!(dimension.contains("DATE_TRUNC"));
    let segments = request.segments.unwrap();
    assert_eq!(segments.len(), 1);
    let segment = &segments[0];
    assert!(segment.contains("DATE_TRUNC"));
}

/// Aggregation with falsy filter should NOT get pushed to CubeScan with limit=0
/// This test currently produces WrappedSelect with WHERE FALSE, which is OK for our purposes
#[tokio::test]
async fn select_agg_where_false() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        "SELECT SUM(sumPrice) FROM KibanaSampleDataEcommerce WHERE 1 = 0".to_string(),
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
            segments: Some(vec![]),
            dimensions: Some(vec![]),
            order: Some(vec![]),
            limit: None,
            ungrouped: Some(true),
            ..Default::default()
        }
    );

    let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;

    // Final query uses grouped query to Cube.js with WHERE FALSE, but without LIMIT 0
    assert!(!sql.contains("\"ungrouped\":"));
    assert!(sql.contains(r#"\"expr\":\"FALSE\""#));
    assert!(sql.contains(r#""limit": 50000"#));
}

/// Aggregation(dimension) with falsy filter should NOT get pushed to CubeScan with limit=0
/// This test currently produces WrappedSelect with WHERE FALSE, which is OK for our purposes
#[tokio::test]
async fn wrapper_dimension_agg_where_false() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
        SELECT
            MAX(customer_gender)
        FROM
            KibanaSampleDataEcommerce
        WHERE 1 = 0
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
            limit: None,
            ungrouped: Some(true),
            ..Default::default()
        }
    );

    let sql = logical_plan.find_cube_scan_wrapped_sql().wrapped_sql.sql;

    // Final query uses grouped query to Cube.js with WHERE FALSE, but without LIMIT 0
    assert!(!sql.contains("\"ungrouped\":"));
    assert!(sql.contains(r#"\"expr\":\"FALSE\""#));
    assert!(!sql.contains(r#""limit""#));
    assert!(sql.contains("LIMIT 50000"));
}
