use datafusion::physical_plan::displayable;
use pretty_assertions::assert_eq;
use regex::Regex;

use crate::compile::{
    test::{convert_select_to_query_plan, init_testing_logger, utils::LogicalPlanTestUtils},
    DatabaseProtocol, Rewriter,
};

// TODO Tests more joins with grouped queries
// Join structure:
// * ungrouped inner join grouped CubeScan
// * ungrouped inner join grouped CubeScan with filters with values
// * ungrouped inner join grouped WrappedSelect
// * ungrouped inner join grouped WrappedSelect with filters with values
// * ungrouped left join grouped
// * grouped left join ungrouped
// * ungrouped join EmptyRelation
// Join condition columns:
// * one dim
// * two dim
// * one measure
// * __cubeJoinField
// * one member expression dim (like ON LOWER(dim) = LOWER(column))
// Join condition predicate:
// * =
// * IS NOT DISTINCT FROM
// * COALESCE + IS NULL
// Grouped query:
// * Grouping
// * Aggregation
// * Filter
// * Sort
// * Limit
// * Wrapper
// On top of of join
// * Grouping
// * Aggregation
// * Filter
// * Limit
// Test long and otherwise bad aliases for columns:
// * in both parts
// * in join condition
// * in expressions on top
// Test long and otherwise bad aliases for tables:
// * for grouped join part
// * for ungrouped join part
// * inside grouped join part
// * inside ungrouped join part
// * for result

/// Simple join between ungrouped and grouped query should plan as a push-to-Cube query
/// with subquery_joins and with concrete member expressions in SQL
#[tokio::test]
async fn test_join_ungrouped_with_grouped() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
SELECT
    kibana_grouped.avg_price,
    KibanaSampleDataEcommerce.customer_gender AS gender,
    AVG(KibanaSampleDataEcommerce.avgPrice) AS price
FROM
    KibanaSampleDataEcommerce
INNER JOIN (
    SELECT
        customer_gender,
        AVG(avgPrice) as avg_price
    FROM
        KibanaSampleDataEcommerce
    GROUP BY 1
) kibana_grouped
ON (
    (KibanaSampleDataEcommerce.customer_gender = kibana_grouped.customer_gender)
)
GROUP BY
    1,
    2
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

    assert_eq!(request.ungrouped, None);

    assert_eq!(request.subquery_joins.as_ref().unwrap().len(), 1);

    let subquery = &request.subquery_joins.unwrap()[0];

    assert!(!subquery.sql.contains("ungrouped"));
    assert_eq!(subquery.join_type, "INNER");
    assert!(subquery.on.contains(
        r#"${KibanaSampleDataEcommerce.customer_gender} = \"kibana_grouped\".\"customer_gender\""#
    ));

    // Measure from top aggregation
    assert!(query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains(r#"\"expr\":\"${KibanaSampleDataEcommerce.avgPrice}\""#));
    // Dimension from ungrouped side
    assert!(query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains(r#"\"expr\":\"${KibanaSampleDataEcommerce.customer_gender}\""#));
    // Dimension from grouped side
    assert!(query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains(r#"\"expr\":\"\\\"kibana_grouped\\\".\\\"avg_price\\\"\""#));
}

/// Simple join between ungrouped and grouped query should plan as a push-to-Cube query
/// with subquery_joins and with concrete member expressions in SQL, even without aggregation on top
// TODO complete this test
#[tokio::test]
async fn test_join_ungrouped_with_grouped_no_agg() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
SELECT
    kibana_grouped.avg_price,
    KibanaSampleDataEcommerce.customer_gender AS gender,
    KibanaSampleDataEcommerce.avgPrice AS price
FROM
    KibanaSampleDataEcommerce
INNER JOIN (
    SELECT
        customer_gender,
        AVG(avgPrice) as avg_price
    FROM
        KibanaSampleDataEcommerce
    GROUP BY 1
) kibana_grouped
ON (
    (KibanaSampleDataEcommerce.customer_gender = kibana_grouped.customer_gender)
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

    let request = query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .request;

    assert_eq!(request.ungrouped, Some(true));

    assert_eq!(request.subquery_joins.as_ref().unwrap().len(), 1);

    let subquery = &request.subquery_joins.unwrap()[0];

    assert!(!subquery.sql.contains("ungrouped"));
    assert_eq!(subquery.join_type, "INNER");
    assert!(subquery.on.contains(
        r#"${KibanaSampleDataEcommerce.customer_gender} = \"kibana_grouped\".\"customer_gender\""#
    ));

    // Measure from top aggregation
    assert!(query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains(r#"\"expr\":\"${KibanaSampleDataEcommerce.avgPrice}\""#));
    // Dimension from ungrouped side
    assert!(query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains(r#"\"expr\":\"${KibanaSampleDataEcommerce.customer_gender}\""#));
    // Dimension from grouped side
    assert!(query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains(r#"\"expr\":\"\\\"kibana_grouped\\\".\\\"avg_price\\\"\""#));
}

/// Join between ungrouped and grouped query with two columns join condition
/// should plan as a push-to-Cube query with subquery_joins
#[tokio::test]
async fn test_join_ungrouped_with_grouped_two_columns_condition() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
SELECT
    AVG(KibanaSampleDataEcommerce.avgPrice) AS price
FROM
    KibanaSampleDataEcommerce
INNER JOIN (
    SELECT
        customer_gender,
        notes,
        AVG(avgPrice) as avg_price
    FROM
        KibanaSampleDataEcommerce
    GROUP BY 1, 2
) kibana_grouped
ON (
    KibanaSampleDataEcommerce.customer_gender = kibana_grouped.customer_gender AND KibanaSampleDataEcommerce.notes = kibana_grouped.notes
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

    let request = query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .request;

    assert_eq!(request.ungrouped, None);

    assert_eq!(request.subquery_joins.as_ref().unwrap().len(), 1);

    let subquery = &request.subquery_joins.unwrap()[0];

    assert!(!subquery.sql.contains("ungrouped"));
    assert_eq!(subquery.join_type, "INNER");
    assert!(subquery.on.contains(
        r#"${KibanaSampleDataEcommerce.customer_gender} = \"kibana_grouped\".\"customer_gender\""#
    ));
    assert!(subquery
        .on
        .contains(r#"${KibanaSampleDataEcommerce.notes} = \"kibana_grouped\".\"notes\""#));

    // Measure from top aggregation
    assert!(query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains(r#"\"expr\":\"${KibanaSampleDataEcommerce.avgPrice}\""#));
}

/// Join between ungrouped and grouped query with filter + sort + limit
/// should plan as a push-to-Cube query with subquery_joins
#[tokio::test]
async fn test_join_ungrouped_with_grouped_top1_and_filter() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
SELECT
    KibanaSampleDataEcommerce.customer_gender AS customer_gender,
    AVG(KibanaSampleDataEcommerce.avgPrice) AS price
FROM
    KibanaSampleDataEcommerce
INNER JOIN (
    SELECT
        customer_gender,
        AVG(avgPrice) as avg_price
    FROM
        KibanaSampleDataEcommerce
    WHERE
        notes = 'foo'
    GROUP BY 1
    ORDER BY 2 DESC NULLS LAST
    LIMIT 1
) kibana_grouped
ON (
    KibanaSampleDataEcommerce.customer_gender = kibana_grouped.customer_gender
)
GROUP BY 1
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

    assert_eq!(request.ungrouped, None);

    assert_eq!(request.subquery_joins.as_ref().unwrap().len(), 1);

    let subquery = &request.subquery_joins.unwrap()[0];

    assert!(!subquery.sql.contains("ungrouped"));
    let re = Regex::new(
        r#""order":\s*\[\s*\[\s*"KibanaSampleDataEcommerce.avgPrice",\s*"desc"\s*\]\s*\]"#,
    )
    .unwrap();
    assert!(re.is_match(&subquery.sql));
    assert!(subquery.sql.contains(r#""limit": 1"#));
    assert_eq!(subquery.join_type, "INNER");
    assert!(subquery.on.contains(
        r#"${KibanaSampleDataEcommerce.customer_gender} = \"kibana_grouped\".\"customer_gender\""#
    ));

    // Measure from top aggregation
    assert!(query_plan
        .as_logical_plan()
        .find_cube_scan_wrapped_sql()
        .wrapped_sql
        .sql
        .contains(r#"\"expr\":\"${KibanaSampleDataEcommerce.avgPrice}\""#));
}

#[tokio::test]
async fn test_superset_topk() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let query_plan = convert_select_to_query_plan(
        // language=PostgreSQL
        r#"
SELECT DATE_TRUNC('week', order_date) AS __timestamp,
       MEASURE(KibanaSampleDataEcommerce.avgPrice) AS avgPrice
FROM KibanaSampleDataEcommerce
JOIN
  (SELECT customer_gender AS customer_gender__,
          MEASURE(KibanaSampleDataEcommerce.avgPrice) AS mme_inner__
   FROM KibanaSampleDataEcommerce
   WHERE order_date >= TO_TIMESTAMP('2022-09-16 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
     AND order_date < TO_TIMESTAMP('2024-09-16 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
   GROUP BY customer_gender
   ORDER BY mme_inner__ DESC
   LIMIT 20) AS anon_1 ON customer_gender = customer_gender__
-- filters here are not supported without filter flattening in wrapper
-- TODO enable it when ready
-- WHERE order_date >= TO_TIMESTAMP('2022-09-16 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
--   AND order_date < TO_TIMESTAMP('2024-09-16 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
GROUP BY DATE_TRUNC('week', order_date)
ORDER BY avgPrice DESC
LIMIT 1000
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

    let wrapped_sql_node = query_plan.as_logical_plan().find_cube_scan_wrapped_sql();

    assert_eq!(wrapped_sql_node.request.ungrouped, None);

    assert_eq!(
        wrapped_sql_node
            .request
            .subquery_joins
            .as_ref()
            .unwrap()
            .len(),
        1
    );

    let subquery = &wrapped_sql_node.request.subquery_joins.unwrap()[0];

    assert!(!subquery.sql.contains("ungrouped"));
    let re = Regex::new(
        r#""order":\s*\[\s*\[\s*"KibanaSampleDataEcommerce.avgPrice",\s*"desc"\s*\]\s*\]"#,
    )
    .unwrap();
    assert!(re.is_match(&subquery.sql));
    assert!(subquery.sql.contains(r#""limit": 20"#));
    assert_eq!(subquery.join_type, "INNER");
    assert!(subquery.on.contains(
        r#"${KibanaSampleDataEcommerce.customer_gender} = \"anon_1\".\"customer_gender_\""#
    ));

    // Measure from top aggregation
    assert!(wrapped_sql_node
        .wrapped_sql
        .sql
        .contains(r#"\"expr\":\"${KibanaSampleDataEcommerce.avgPrice}\""#));

    // Outer sort
    assert!(wrapped_sql_node
        .wrapped_sql
        .sql
        .contains(r#"ORDER BY "KibanaSampleDataEcommerce"."measure_kibanasa" DESC NULLS FIRST"#));

    // Outer limit
    assert!(wrapped_sql_node.wrapped_sql.sql.contains("LIMIT 1000"));
}
