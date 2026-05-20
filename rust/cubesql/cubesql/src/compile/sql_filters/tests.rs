use super::*;
use super::{
    matching::MAX_CLAUSE_PREDICATES,
    report::{date_range_upper, normalize_filter_value, report_date_value},
};
use crate::compile::test::get_test_tenant_ctx;
use cubeclient::models::{V1CubeMetaDimension, V1CubeMetaMeasure};
use std::slice;

/// Applies a single action; a convenience wrapper over [`modify_parsed_query`].
fn modify_sql_ast(sql: &str, action: &ModifyAction, ctx: &MetaContext) -> DFResult<(String, bool)> {
    let (sql, applied) =
        modify_parsed_query(sql, slice::from_ref(action), ctx, &ReportedFilters::none())?;
    Ok((sql, applied[0]))
}

#[test]
fn test_modify_sql_ast() -> DFResult<()> {
    let sql = r#"
        SELECT
            KibanaSampleDataEcommerce.customer_gender,
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price,
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure
        FROM KibanaSampleDataEcommerce
        GROUP BY 1
        ORDER BY 1
    "#;

    // Test adding "equals" filter
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["test".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test adding "notEquals" filter with multiple values
    let sql = modified_sql;
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("notEquals".to_string()),
        values: Some(vec![
            "test1".to_string(),
            "test2".to_string(),
            "test3".to_string(),
        ]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"customer_gender\" NOT IN ('test1', 'test2', 'test3') \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test removing existing "notEquals" filter
    let sql = modified_sql;
    let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("notEquals".to_string()),
        values: Some(vec![
            "test1".to_string(),
            "test2".to_string(),
            "test3".to_string(),
        ]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test removing non-existing filter
    let sql = modified_sql;
    let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("notEquals".to_string()),
        values: Some(vec![
            "test1".to_string(),
            "test2".to_string(),
            "test3".to_string(),
        ]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    // Make sure no modifications were made
    assert!(!applied);

    // Test adding "contains" filter with a single value
    let sql = modified_sql;
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("contains".to_string()),
        values: Some(vec!["abc".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%abc%' \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test adding "contains" filter with multiple values (OR-combined, escaped)
    let sql = modified_sql;
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("contains".to_string()),
        values: Some(vec!["x".to_string(), "y%z_w\\v".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%abc%' \
            AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%x%' \
                OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%y\\%z\\_w\\\\v%') \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test adding "notContains" filter with a single value
    let sql = modified_sql;
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("notContains".to_string()),
        values: Some(vec!["foo".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%abc%' \
            AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%x%' \
                OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%y\\%z\\_w\\\\v%') \
            AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%foo%' \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test adding "notContains" filter with multiple values (AND-combined, escaped)
    let sql = modified_sql;
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("notContains".to_string()),
        values: Some(vec!["bar".to_string(), "baz%_\\qux".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%abc%' \
            AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%x%' \
                OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%y\\%z\\_w\\\\v%') \
            AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%foo%' \
            AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%bar%' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%baz\\%\\_\\\\qux%') \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test removing existing single-value "contains" filter
    let sql = modified_sql;
    let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("contains".to_string()),
        values: Some(vec!["abc".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%x%' \
                OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%y\\%z\\_w\\\\v%') \
            AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%foo%' \
            AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%bar%' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%baz\\%\\_\\\\qux%') \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test removing existing multi-value "contains" filter
    let sql = modified_sql;
    let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("contains".to_string()),
        values: Some(vec!["x".to_string(), "y%z_w\\v".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%foo%' \
            AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%bar%' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%baz\\%\\_\\\\qux%') \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test removing existing single-value "notContains" filter
    let sql = modified_sql;
    let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("notContains".to_string()),
        values: Some(vec!["foo".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%bar%' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%baz\\%\\_\\\\qux%') \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test removing existing multi-value "notContains" filter
    let sql = modified_sql;
    let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("notContains".to_string()),
        values: Some(vec!["bar".to_string(), "baz%_\\qux".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test adding "startsWith" filter with a single value
    let sql = modified_sql;
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("startsWith".to_string()),
        values: Some(vec!["pre".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'pre%' \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test adding "startsWith" filter with multiple values (OR-combined, escaped)
    let sql = modified_sql;
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("startsWith".to_string()),
        values: Some(vec!["a".to_string(), "b%".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'pre%' \
            AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'a%' \
                OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'b\\%%') \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test adding "notStartsWith" filter with a single value
    let sql = modified_sql;
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("notStartsWith".to_string()),
        values: Some(vec!["foo".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'pre%' \
            AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'a%' \
                OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'b\\%%') \
            AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'foo%' \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test adding "notStartsWith" filter with multiple values (AND-combined, escaped)
    let sql = modified_sql;
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("notStartsWith".to_string()),
        values: Some(vec!["x".to_string(), "_y".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'pre%' \
            AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'a%' \
                OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'b\\%%') \
            AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'foo%' \
            AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'x%' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '\\_y%') \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test adding "endsWith" filter with a single value
    let sql = modified_sql;
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("endsWith".to_string()),
        values: Some(vec!["end".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'pre%' \
            AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'a%' \
                OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'b\\%%') \
            AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'foo%' \
            AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'x%' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '\\_y%') \
            AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%end' \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test adding "endsWith" filter with multiple values (OR-combined, escaped)
    let sql = modified_sql;
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("endsWith".to_string()),
        values: Some(vec!["m".to_string(), "n\\o".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'pre%' \
            AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'a%' \
                OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'b\\%%') \
            AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'foo%' \
            AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'x%' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '\\_y%') \
            AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%end' \
            AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%m' \
                OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%n\\\\o') \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test adding "notEndsWith" filter with a single value
    let sql = modified_sql;
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("notEndsWith".to_string()),
        values: Some(vec!["tail".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'pre%' \
            AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'a%' \
                OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'b\\%%') \
            AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'foo%' \
            AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'x%' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '\\_y%') \
            AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%end' \
            AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%m' \
                OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%n\\\\o') \
            AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%tail' \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test adding "notEndsWith" filter with multiple values (AND-combined, escaped)
    let sql = modified_sql;
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("notEndsWith".to_string()),
        values: Some(vec!["p".to_string(), "q_r".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'pre%' \
            AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'a%' \
                OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'b\\%%') \
            AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'foo%' \
            AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'x%' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '\\_y%') \
            AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%end' \
            AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%m' \
                OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%n\\\\o') \
            AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%tail' \
            AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%p' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%q\\_r') \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test removing all four newly-added prefix/suffix filters in reverse insertion order
    let remove_ops: Vec<(&str, Vec<&str>)> = vec![
        ("notEndsWith", vec!["p", "q_r"]),
        ("notEndsWith", vec!["tail"]),
        ("endsWith", vec!["m", "n\\o"]),
        ("endsWith", vec!["end"]),
        ("notStartsWith", vec!["x", "_y"]),
        ("notStartsWith", vec!["foo"]),
        ("startsWith", vec!["a", "b%"]),
        ("startsWith", vec!["pre"]),
    ];
    let mut sql = modified_sql;
    for (op, values) in remove_ops {
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some(op.to_string()),
            values: Some(values.into_iter().map(|s| s.to_string()).collect()),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (next_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert!(applied, "remove {} should be applied", op);
        sql = next_sql;
    }
    assert_eq!(
        sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );

    // Test adding "gt" filter (integer value) on a numeric dimension
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
        operator: Some("gt".to_string()),
        values: Some(vec!["42".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 42 \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test adding "gt" filter (decimal value) on same member, AND-combined
    let sql = modified_sql;
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
        operator: Some("gt".to_string()),
        values: Some(vec!["3.14".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 42 \
            AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 3.14 \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test removing the integer "gt" filter
    let sql = modified_sql;
    let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
        operator: Some("gt".to_string()),
        values: Some(vec!["42".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 3.14 \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test "gt" with non-numeric value rejected
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
        operator: Some("gt".to_string()),
        values: Some(vec!["not_a_number".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
    assert!(
        err.to_string().contains("must be numeric"),
        "unexpected error: {}",
        err
    );

    // Test "gt" with wrong number of values rejected
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
        operator: Some("gt".to_string()),
        values: Some(vec!["1".to_string(), "2".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
    assert!(
        err.to_string().contains("Exactly one filter value"),
        "unexpected error: {}",
        err
    );

    // Test adding "gte", "lt", "lte" filters chained on the numeric dimension
    let add_ops: Vec<(&str, &str)> = vec![("gte", "5"), ("lt", "100"), ("lte", "10.5")];
    let mut sql = modified_sql;
    for (op, value) in add_ops {
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
            operator: Some(op.to_string()),
            values: Some(vec![value.to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (next_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert!(applied, "add {} should be applied", op);
        sql = next_sql;
    }
    assert_eq!(
        sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 3.14 \
            AND KibanaSampleDataEcommerce.\"taxful_total_price\" >= 5 \
            AND KibanaSampleDataEcommerce.\"taxful_total_price\" < 100 \
            AND KibanaSampleDataEcommerce.\"taxful_total_price\" <= 10.5 \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );

    // Test removing "gte", "lt", "lte", and remaining "gt" filters
    let remove_ops: Vec<(&str, &str)> =
        vec![("lte", "10.5"), ("lt", "100"), ("gte", "5"), ("gt", "3.14")];
    for (op, value) in remove_ops {
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
            operator: Some(op.to_string()),
            values: Some(vec![value.to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (next_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert!(applied, "remove {} should be applied", op);
        sql = next_sql;
    }
    assert_eq!(
        sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );

    // Test non-numeric value rejected for each of gte/lt/lte
    for op in ["gte", "lt", "lte"] {
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
            operator: Some(op.to_string()),
            values: Some(vec!["not_a_number".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let err = modify_sql_ast(&sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string().contains("must be numeric"),
            "unexpected error for {}: {}",
            op,
            err
        );
    }

    // Test adding "set" filter (no values)
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("set".to_string()),
        values: None,
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"customer_gender\" IS NOT NULL \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test removing existing "set" filter
    let sql = modified_sql;
    let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("set".to_string()),
        values: None,
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test adding "notSet" filter (values ignored)
    let sql = modified_sql;
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("notSet".to_string()),
        values: Some(vec!["ignored".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"customer_gender\" IS NULL \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test removing existing "notSet" filter
    let sql = modified_sql;
    let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("notSet".to_string()),
        values: None,
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test adding "inDateRange" filter
    let sql = modified_sql;
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
        operator: Some("inDateRange".to_string()),
        values: Some(vec!["2024-01-01".to_string(), "2024-12-31".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND (KibanaSampleDataEcommerce.\"order_date\" >= '2024-01-01' \
                AND KibanaSampleDataEcommerce.\"order_date\" <= '2024-12-31T23:59:59.999') \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test removing existing "inDateRange" filter
    let sql = modified_sql;
    let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
        operator: Some("inDateRange".to_string()),
        values: Some(vec!["2024-01-01".to_string(), "2024-12-31".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test "inDateRange" with wrong number of values rejected
    for values in [
        vec![],
        vec!["2024-01-01".to_string()],
        vec![
            "2024-01-01".to_string(),
            "2024-06-01".to_string(),
            "2024-12-31".to_string(),
        ],
    ] {
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("inDateRange".to_string()),
            values: Some(values),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string().contains("Exactly two filter values"),
            "unexpected error: {}",
            err
        );
    }

    // Test adding "notInDateRange" filter
    let sql = modified_sql;
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
        operator: Some("notInDateRange".to_string()),
        values: Some(vec!["2024-01-01".to_string(), "2024-12-31".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"order_date\" NOT BETWEEN '2024-01-01' AND '2024-12-31T23:59:59.999' \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test removing existing "notInDateRange" filter
    let sql = modified_sql;
    let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
        operator: Some("notInDateRange".to_string()),
        values: Some(vec!["2024-01-01".to_string(), "2024-12-31".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test "notInDateRange" with wrong number of values rejected
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
        operator: Some("notInDateRange".to_string()),
        values: Some(vec!["2024-01-01".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
    assert!(
        err.to_string().contains("Exactly two filter values"),
        "unexpected error: {}",
        err
    );

    // Test adding "beforeDate" filter
    let sql = modified_sql;
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
        operator: Some("beforeDate".to_string()),
        values: Some(vec!["2024-06-01".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"order_date\" < '2024-06-01' \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test removing existing "beforeDate" filter
    let sql = modified_sql;
    let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
        operator: Some("beforeDate".to_string()),
        values: Some(vec!["2024-06-01".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test "beforeDate" with wrong number of values rejected
    for values in [vec![], vec!["a".to_string(), "b".to_string()]] {
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("beforeDate".to_string()),
            values: Some(values),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string().contains("Exactly one filter value"),
            "unexpected error: {}",
            err
        );
    }

    // Test adding "beforeOrOnDate" filter
    let sql = modified_sql;
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
        operator: Some("beforeOrOnDate".to_string()),
        values: Some(vec!["2024-06-01".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"order_date\" <= '2024-06-01' \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test removing existing "beforeOrOnDate" filter
    let sql = modified_sql;
    let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
        operator: Some("beforeOrOnDate".to_string()),
        values: Some(vec!["2024-06-01".to_string()]),
        ..Default::default()
    });
    let ctx = get_test_tenant_ctx();
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );
    assert!(applied);

    // Test adding "afterDate" then "afterOrOnDate" filters; verify SQL operators
    let add_ops: Vec<(&str, &str, &str)> = vec![
        ("afterDate", "2024-06-01", ">"),
        ("afterOrOnDate", "2024-07-01", ">="),
    ];
    let mut sql = modified_sql;
    for (op, value, _) in &add_ops {
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some(op.to_string()),
            values: Some(vec![value.to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (next_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert!(applied, "add {} should be applied", op);
        sql = next_sql;
    }
    assert_eq!(
        sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"order_date\" > '2024-06-01' \
            AND KibanaSampleDataEcommerce.\"order_date\" >= '2024-07-01' \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );

    // Test removing both filters
    for (op, value, _) in add_ops.iter().rev() {
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some(op.to_string()),
            values: Some(vec![value.to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (next_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert!(applied, "remove {} should be applied", op);
        sql = next_sql;
    }
    assert_eq!(
        sql,
        "\
        SELECT \
            KibanaSampleDataEcommerce.customer_gender, \
            SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
            MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
        GROUP BY 1 \
        ORDER BY 1\
        "
    );

    Ok(())
}

#[test]
fn test_modify_sql_ast_cte_outermost_only() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    let sql = "\
        WITH gendered AS (\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender AS gender, \
                MAX(KibanaSampleDataEcommerce.maxPrice) AS max_price \
            FROM KibanaSampleDataEcommerce \
            GROUP BY 1\
        ) \
        SELECT gender, max_price FROM gendered\
    ";

    // Dimension exposed by the CTE: filtered in the outermost WHERE,
    // the CTE itself is left untouched
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["test".to_string()]),
        ..Default::default()
    });
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        WITH gendered AS (\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender AS gender, \
                MAX(KibanaSampleDataEcommerce.maxPrice) AS max_price \
            FROM KibanaSampleDataEcommerce \
            GROUP BY 1\
        ) \
        SELECT gender, max_price FROM gendered \
        WHERE gendered.gender = 'test'\
        "
    );
    assert!(applied);

    // Measure exposed by the CTE as an aggregation: filtered as a plain
    // column in the outermost WHERE
    let sql = modified_sql;
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
        operator: Some("gt".to_string()),
        values: Some(vec!["42".to_string()]),
        ..Default::default()
    });
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        WITH gendered AS (\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender AS gender, \
                MAX(KibanaSampleDataEcommerce.maxPrice) AS max_price \
            FROM KibanaSampleDataEcommerce \
            GROUP BY 1\
        ) \
        SELECT gender, max_price FROM gendered \
        WHERE gendered.gender = 'test' AND gendered.max_price > 42\
        "
    );
    assert!(applied);

    // Removing both filters restores the original query
    let sql = modified_sql;
    let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
        operator: Some("gt".to_string()),
        values: Some(vec!["42".to_string()]),
        ..Default::default()
    });
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert!(applied);
    let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["test".to_string()]),
        ..Default::default()
    });
    let (modified_sql, applied) = modify_sql_ast(&modified_sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        WITH gendered AS (\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender AS gender, \
                MAX(KibanaSampleDataEcommerce.maxPrice) AS max_price \
            FROM KibanaSampleDataEcommerce \
            GROUP BY 1\
        ) \
        SELECT gender, max_price FROM gendered\
        "
    );
    assert!(applied);

    // Member not exposed by the CTE is rejected
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
        operator: Some("beforeDate".to_string()),
        values: Some(vec!["2024-06-01".to_string()]),
        ..Default::default()
    });
    let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
    assert!(
        err.to_string()
            .contains("is not available in the outermost SELECT"),
        "unexpected error: {}",
        err
    );

    Ok(())
}

#[test]
fn test_modify_sql_ast_cte_filters_not_removed_from_cte() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    // The filter lives inside the CTE; removal only looks at the outermost
    // SELECT, so nothing is modified
    let sql = "\
        WITH gendered AS (\
            SELECT KibanaSampleDataEcommerce.customer_gender AS gender \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test'\
        ) \
        SELECT * FROM gendered\
    ";
    let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["test".to_string()]),
        ..Default::default()
    });
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(modified_sql, sql);
    assert!(!applied);

    Ok(())
}

#[test]
fn test_modify_sql_ast_derived_table() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    let sql = "\
        SELECT * FROM (\
            SELECT customer_gender FROM KibanaSampleDataEcommerce\
        ) AS t\
    ";
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["test".to_string()]),
        ..Default::default()
    });
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT * FROM (\
            SELECT customer_gender FROM KibanaSampleDataEcommerce\
        ) AS t \
        WHERE t.\"customer_gender\" = 'test'\
        "
    );
    assert!(applied);

    Ok(())
}

#[test]
fn test_modify_sql_ast_outermost_set_operation_rejected() {
    let ctx = get_test_tenant_ctx();
    let sql = "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        UNION ALL \
        SELECT customer_gender FROM KibanaSampleDataEcommerce\
    ";
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["test".to_string()]),
        ..Default::default()
    });
    let err = modify_sql_ast(&sql, &action, &ctx).unwrap_err();
    assert!(
        err.to_string()
            .contains("Only plain SELECT statements are supported at the outermost level"),
        "unexpected error: {}",
        err
    );
}

#[tokio::test]
async fn test_add_delete_sql_filters() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    let sql = "\
        SELECT customer_gender, MAX(maxPrice) AS max_price \
        FROM KibanaSampleDataEcommerce \
        GROUP BY 1\
    ";

    // The original query has no filters
    let filters = get_sql_filters(sql, meta.clone(), session.clone()).await?;
    assert!(filters.is_empty());

    // Add a filter
    let filters = vec![V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["test".to_string()]),
        ..Default::default()
    }];
    let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
    assert_eq!(
        result.sql,
        "\
        SELECT customer_gender, MAX(maxPrice) AS max_price \
        FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
        GROUP BY 1\
        "
    );
    assert!(result
        .filters
        .iter()
        .any(|filter| filter_key(filter, &meta) == filter_key(&filters[0], &meta)));

    // The added filter is extracted from the logical plan
    let extracted = get_sql_filters(&result.sql, meta.clone(), session.clone()).await?;
    assert!(extracted
        .iter()
        .any(|filter| filter_key(filter, &meta) == filter_key(&filters[0], &meta)));

    // Adding the same filter again is a no-op
    let noop_result = add_sql_filters(&result.sql, &filters, meta.clone(), session.clone()).await?;
    assert_eq!(noop_result.sql, result.sql);

    // Delete the filter
    let result = delete_sql_filters(&result.sql, &filters, meta.clone(), session.clone()).await?;
    assert_eq!(
        result.sql,
        "\
        SELECT customer_gender, MAX(maxPrice) AS max_price \
        FROM KibanaSampleDataEcommerce \
        GROUP BY 1\
        "
    );
    assert!(result.filters.is_empty());

    // Deleting a filter that is not present is a no-op
    let result = delete_sql_filters(&result.sql, &filters, meta, session).await?;
    assert_eq!(
        result.sql,
        "\
        SELECT customer_gender, MAX(maxPrice) AS max_price \
        FROM KibanaSampleDataEcommerce \
        GROUP BY 1\
        "
    );

    Ok(())
}

fn or_group(items: Vec<serde_json::Value>) -> V1LoadRequestQueryFilterItem {
    V1LoadRequestQueryFilterItem {
        or: Some(items),
        ..Default::default()
    }
}

fn and_group(items: Vec<serde_json::Value>) -> V1LoadRequestQueryFilterItem {
    V1LoadRequestQueryFilterItem {
        and: Some(items),
        ..Default::default()
    }
}

#[test]
fn test_modify_sql_ast_filter_groups() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

    // Add an "or" filter group
    let group = or_group(vec![
        serde_json::json!({
            "member": "KibanaSampleDataEcommerce.customer_gender",
            "operator": "equals",
            "values": ["male"],
        }),
        serde_json::json!({
            "member": "KibanaSampleDataEcommerce.notes",
            "operator": "contains",
            "values": ["vip"],
        }),
    ]);
    let action = ModifyAction::Add(group.clone());
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE (KibanaSampleDataEcommerce.\"customer_gender\" = 'male' \
            OR KibanaSampleDataEcommerce.\"notes\" ILIKE '%vip%') \
        GROUP BY 1\
        "
    );
    assert!(applied);

    // Removing a group with a different order of filters is not
    // a perfect match, so nothing is removed
    let sql = modified_sql;
    let reversed_group = or_group(vec![
        serde_json::json!({
            "member": "KibanaSampleDataEcommerce.notes",
            "operator": "contains",
            "values": ["vip"],
        }),
        serde_json::json!({
            "member": "KibanaSampleDataEcommerce.customer_gender",
            "operator": "equals",
            "values": ["male"],
        }),
    ]);
    let action = ModifyAction::Remove(reversed_group);
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(modified_sql, sql);
    assert!(!applied);

    // Removing a perfectly matching group works
    let action = ModifyAction::Remove(group);
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1"
    );
    assert!(applied);

    // Add a nested filter group: "and" group inside an "or" group
    let sql = modified_sql;
    let nested_group = or_group(vec![
        serde_json::json!({
            "member": "KibanaSampleDataEcommerce.customer_gender",
            "operator": "equals",
            "values": ["x"],
        }),
        serde_json::json!({
            "and": [
                {
                    "member": "KibanaSampleDataEcommerce.taxful_total_price",
                    "operator": "gt",
                    "values": ["1"],
                },
                {
                    "member": "KibanaSampleDataEcommerce.taxful_total_price",
                    "operator": "lt",
                    "values": ["5"],
                },
            ],
        }),
    ]);
    let action = ModifyAction::Add(nested_group.clone());
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE (KibanaSampleDataEcommerce.\"customer_gender\" = 'x' \
            OR (KibanaSampleDataEcommerce.\"taxful_total_price\" > 1 \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" < 5)) \
        GROUP BY 1\
        "
    );
    assert!(applied);

    // Removing the perfectly matching nested group works
    let sql = modified_sql;
    let action = ModifyAction::Remove(nested_group);
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1"
    );
    assert!(applied);

    // A group mixing dimension (WHERE) and measure (HAVING) filters is rejected
    let mixed_group = or_group(vec![
        serde_json::json!({
            "member": "KibanaSampleDataEcommerce.customer_gender",
            "operator": "equals",
            "values": ["x"],
        }),
        serde_json::json!({
            "member": "KibanaSampleDataEcommerce.maxPrice",
            "operator": "gt",
            "values": ["10"],
        }),
    ]);
    let action = ModifyAction::Add(mixed_group);
    let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
    assert!(
        err.to_string().contains("can't mix"),
        "unexpected error: {}",
        err
    );

    // A filter that is both an "and" and an "or" group is rejected
    let mut invalid_group = and_group(vec![serde_json::json!({
        "member": "KibanaSampleDataEcommerce.customer_gender",
        "operator": "equals",
        "values": ["x"],
    })]);
    invalid_group.or = Some(vec![]);
    let action = ModifyAction::Add(invalid_group);
    let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
    assert!(
        err.to_string().contains("can't be both \"and\" and \"or\""),
        "unexpected error: {}",
        err
    );

    // An empty group is rejected
    let action = ModifyAction::Add(or_group(vec![]));
    let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
    assert!(
        err.to_string().contains("at least one filter"),
        "unexpected error: {}",
        err
    );

    Ok(())
}

#[test]
fn test_modify_sql_ast_replace_filter() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    let sql = "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 42 \
        GROUP BY 1\
    ";

    // Replace a filter in place: the position within the clause is preserved
    let action = ModifyAction::Replace {
        old: V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        },
        new: V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notEquals".to_string()),
            values: Some(vec!["a".to_string(), "b".to_string()]),
            ..Default::default()
        },
    };
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" NOT IN ('a', 'b') \
            AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 42 \
        GROUP BY 1\
        "
    );
    assert!(applied);

    // Replacing a filter that is not present is not applied
    let sql = modified_sql;
    let action = ModifyAction::Replace {
        old: V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["missing".to_string()]),
            ..Default::default()
        },
        new: V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("set".to_string()),
            ..Default::default()
        },
    };
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(modified_sql, sql);
    assert!(!applied);

    // Replace a plain filter with an "or" filter group in place
    let action = ModifyAction::Replace {
        old: V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
            operator: Some("gt".to_string()),
            values: Some(vec!["42".to_string()]),
            ..Default::default()
        },
        new: or_group(vec![
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.taxful_total_price",
                "operator": "lt",
                "values": ["10"],
            }),
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.taxful_total_price",
                "operator": "gt",
                "values": ["100"],
            }),
        ]),
    };
    let (modified_sql, applied) = modify_sql_ast(&modified_sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" NOT IN ('a', 'b') \
            AND (KibanaSampleDataEcommerce.\"taxful_total_price\" < 10 \
                OR KibanaSampleDataEcommerce.\"taxful_total_price\" > 100) \
        GROUP BY 1\
        "
    );
    assert!(applied);

    // Replace a dimension (WHERE) filter with a measure (HAVING) filter
    let sql = modified_sql;
    let action = ModifyAction::Replace {
        old: V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notEquals".to_string()),
            values: Some(vec!["a".to_string(), "b".to_string()]),
            ..Default::default()
        },
        new: V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
            operator: Some("gt".to_string()),
            values: Some(vec!["10".to_string()]),
            ..Default::default()
        },
    };
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE (KibanaSampleDataEcommerce.\"taxful_total_price\" < 10 \
            OR KibanaSampleDataEcommerce.\"taxful_total_price\" > 100) \
        GROUP BY 1 \
        HAVING MAX(KibanaSampleDataEcommerce.\"maxPrice\") > 10\
        "
    );
    assert!(applied);

    Ok(())
}

#[tokio::test]
async fn test_add_delete_sql_filters_group() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

    // Add an "or" filter group
    let filters = vec![or_group(vec![
        serde_json::json!({
            "member": "KibanaSampleDataEcommerce.customer_gender",
            "operator": "equals",
            "values": ["x"],
        }),
        serde_json::json!({
            "member": "KibanaSampleDataEcommerce.notes",
            "operator": "equals",
            "values": ["y"],
        }),
    ])];
    let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
    assert_eq!(
        result.sql,
        "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE (KibanaSampleDataEcommerce.\"customer_gender\" = 'x' \
            OR KibanaSampleDataEcommerce.\"notes\" = 'y') \
        GROUP BY 1\
        "
    );
    assert!(result
        .filters
        .iter()
        .any(|filter| filter_key(filter, &meta) == filter_key(&filters[0], &meta)));

    // Delete the group
    let result = delete_sql_filters(&result.sql, &filters, meta, session).await?;
    assert_eq!(
        result.sql,
        "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1"
    );
    assert!(result.filters.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_replace_sql_filters() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    let sql = "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
        GROUP BY 1\
    ";

    // Replace a plain filter with another plain filter
    let old_filter = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["test".to_string()]),
        ..Default::default()
    };
    let new_filter = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("notEquals".to_string()),
        values: Some(vec!["other".to_string()]),
        ..Default::default()
    };
    let result = replace_sql_filters(
        sql,
        slice::from_ref(&old_filter),
        slice::from_ref(&new_filter),
        meta.clone(),
        session.clone(),
    )
    .await?;
    assert_eq!(
        result.sql,
        "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" <> 'other' \
        GROUP BY 1\
        "
    );
    assert!(result
        .filters
        .iter()
        .any(|filter| filter_key(filter, &meta) == filter_key(&new_filter, &meta)));
    assert!(!result
        .filters
        .iter()
        .any(|filter| filter_key(filter, &meta) == filter_key(&old_filter, &meta)));

    // Replacing a filter that is not present fails
    let err = replace_sql_filters(
        &result.sql,
        slice::from_ref(&old_filter),
        slice::from_ref(&new_filter),
        meta.clone(),
        session.clone(),
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("was not found in the outermost SELECT"),
        "unexpected error: {}",
        err
    );

    // Replace one set of filters with another
    let old_set = vec![new_filter];
    let new_set = vec![
        V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["a".to_string()]),
            ..Default::default()
        },
        V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
            operator: Some("gt".to_string()),
            values: Some(vec!["10".to_string()]),
            ..Default::default()
        },
    ];
    let result = replace_sql_filters(
        &result.sql,
        &old_set,
        &new_set,
        meta.clone(),
        session.clone(),
    )
    .await?;
    assert_eq!(
        result.sql,
        "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'a' \
            AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 10 \
        GROUP BY 1\
        "
    );
    for filter in &new_set {
        assert!(result
            .filters
            .iter()
            .any(|extracted| filter_key(extracted, &meta) == filter_key(filter, &meta)));
    }
    assert!(!result
        .filters
        .iter()
        .any(|extracted| filter_key(extracted, &meta) == filter_key(&old_set[0], &meta)));

    // An empty set of filters to replace is rejected
    let err = replace_sql_filters(&result.sql, &[], &new_set, meta, session)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("At least one filter"),
        "unexpected error: {}",
        err
    );

    Ok(())
}

#[test]
fn test_modify_sql_ast_duplicate_filters() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    // The same filter expression appears twice in the WHERE clause
    let sql = "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 42 \
            AND KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
        GROUP BY 1\
    ";

    // Deleting removes all equal filters
    let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["test".to_string()]),
        ..Default::default()
    });
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"taxful_total_price\" > 42 \
        GROUP BY 1\
        "
    );
    assert!(applied);

    // Replacing replaces all equal filters, preserving positions
    let action = ModifyAction::Replace {
        old: V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        },
        new: V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notEquals".to_string()),
            values: Some(vec!["other".to_string()]),
            ..Default::default()
        },
    };
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" <> 'other' \
            AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 42 \
            AND KibanaSampleDataEcommerce.\"customer_gender\" <> 'other' \
        GROUP BY 1\
        "
    );
    assert!(applied);

    Ok(())
}

#[tokio::test]
async fn test_set_sql_filters() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    let sql = "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 42 \
        GROUP BY 1\
    ";

    // Set replaces all outermost filters with the new set
    let filters = vec![V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.notes".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["y".to_string()]),
        ..Default::default()
    }];
    let result = set_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
    assert_eq!(
        result.sql,
        "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"notes\" = 'y' \
        GROUP BY 1\
        "
    );
    assert!(result
        .filters
        .iter()
        .any(|filter| filter_key(filter, &meta) == filter_key(&filters[0], &meta)));
    assert!(!result.filters.iter().any(
        |filter| filter.member.as_deref() == Some("KibanaSampleDataEcommerce.customer_gender")
    ));

    // Setting an empty set clears all outermost filters
    let result = set_sql_filters(&result.sql, &[], meta, session).await?;
    assert_eq!(
        result.sql,
        "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1"
    );
    assert!(result.filters.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_add_sql_filters_like_family_round_trip() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

    // Several values are emitted as a boolean chain of one predicate per
    // value, which the planner reports as several single-value filters
    for operator in [
        "contains",
        "notContains",
        "startsWith",
        "notStartsWith",
        "endsWith",
        "notEndsWith",
    ] {
        let filters = vec![V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some(operator.to_string()),
            values: Some(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
            ..Default::default()
        }];
        let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
        let extracted = result
            .filters
            .iter()
            .map(|filter| filter_key(filter, &meta))
            .collect::<HashSet<_>>();
        let expected = verification_keys(&filters[0], &meta);
        assert!(
            expected.iter().all(|key| extracted.contains(key)),
            "{} filter did not round trip, wanted {:?}, got {:?}",
            operator,
            expected,
            result.filters
        );
        // The plain operators chain with OR and stay one group, the
        // negated ones chain with AND and are flattened into siblings
        assert_eq!(
            expected.len(),
            if operator.starts_with("not") { 3 } else { 1 },
            "unexpected verification keys for {}: {:?}",
            operator,
            expected
        );
    }

    // LIKE-family filters must survive the planner round trip, including
    // values with characters that have to be escaped in the pattern
    for (operator, value) in [
        ("contains", "abc"),
        ("startsWith", "pre"),
        ("endsWith", "end"),
        ("notContains", "x"),
        ("contains", "50%_off\\now"),
    ] {
        let filters = vec![V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some(operator.to_string()),
            values: Some(vec![value.to_string()]),
            ..Default::default()
        }];
        let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
        assert!(
            result
                .filters
                .iter()
                .any(|filter| filter_key(filter, &meta) == filter_key(&filters[0], &meta)),
            "{} filter on {:?} did not round trip, got {:?}",
            operator,
            value,
            result.filters
        );
    }

    Ok(())
}

#[test]
fn test_modify_sql_ast_numeric_value_validation() {
    let ctx = get_test_tenant_ctx();
    let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

    // Numeric literals are rendered verbatim, so anything that is not a
    // plain number must be rejected instead of reaching the SQL text
    for value in ["0 OR 1=1", "abc", "1; DROP TABLE x", "inf", "NaN", " 1", ""] {
        for operator in ["equals", "notEquals"] {
            let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                operator: Some(operator.to_string()),
                values: Some(vec![value.to_string()]),
                ..Default::default()
            });
            let err = modify_sql_ast(sql, &action, &ctx).unwrap_err();
            assert!(
                err.to_string().contains("must be numeric"),
                "unexpected error for {} {:?}: {}",
                operator,
                value,
                err
            );
        }
    }

    // Multi-value filters validate every value
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["1".to_string(), "2 OR 1=1".to_string()]),
        ..Default::default()
    });
    let err = modify_sql_ast(sql, &action, &ctx).unwrap_err();
    assert!(
        err.to_string().contains("must be numeric"),
        "unexpected error: {}",
        err
    );

    // Well-formed numbers are still accepted
    for value in ["42", "-1", "3.14", "1e3"] {
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec![value.to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(sql, &action, &ctx).unwrap();
        assert!(applied);
        assert!(
            modified_sql.ends_with(&format!(
                "WHERE KibanaSampleDataEcommerce.\"taxful_total_price\" = {} GROUP BY 1",
                value
            )),
            "unexpected SQL for {:?}: {}",
            value,
            modified_sql
        );
    }
}

#[test]
fn test_modify_sql_ast_or_clause_is_parenthesized() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    // AND-ing onto a top-level OR chain must not rebind the disjunction
    let sql = "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'a' \
            OR KibanaSampleDataEcommerce.\"customer_gender\" = 'b' \
        GROUP BY 1\
    ";
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.notes".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["x".to_string()]),
        ..Default::default()
    });
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE (KibanaSampleDataEcommerce.\"customer_gender\" = 'a' \
            OR KibanaSampleDataEcommerce.\"customer_gender\" = 'b') \
            AND KibanaSampleDataEcommerce.\"notes\" = 'x' \
        GROUP BY 1\
        "
    );
    assert!(applied);

    Ok(())
}

#[test]
fn test_modify_sql_ast_parenthesized_clause() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    // A redundantly parenthesized clause must not defeat lookup or removal
    let sql = "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE (KibanaSampleDataEcommerce.\"customer_gender\" = 'a' \
            AND KibanaSampleDataEcommerce.\"notes\" = 'x') \
        GROUP BY 1\
    ";
    let filter = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["a".to_string()]),
        ..Default::default()
    };

    // Already present: adding is a no-op
    let action = ModifyAction::Add(filter.clone());
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(modified_sql, sql.trim_end());
    assert!(!applied);

    // Removal descends into the parentheses. Those around the clause as a
    // whole say nothing about its structure, so they don't survive it
    let action = ModifyAction::Remove(filter);
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"notes\" = 'x' \
        GROUP BY 1\
        "
    );
    assert!(applied);

    Ok(())
}

#[test]
fn test_modify_sql_ast_cte_shadowing_cube_name() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    // The CTE is named after the cube; its body cannot see itself, so the
    // name there is the cube, whose column the CTE exposes as `gender`
    let sql = "\
        WITH KibanaSampleDataEcommerce AS (\
            SELECT customer_gender AS gender FROM KibanaSampleDataEcommerce\
        ) \
        SELECT gender FROM KibanaSampleDataEcommerce\
    ";

    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["test".to_string()]),
        ..Default::default()
    });
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert!(applied);
    assert!(
        modified_sql.ends_with("WHERE KibanaSampleDataEcommerce.gender = 'test'"),
        "{}",
        modified_sql
    );

    // Deleting a filter the query does not hold is a no-op
    let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["test".to_string()]),
        ..Default::default()
    });
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(modified_sql, sql.trim_end());
    assert!(!applied);

    Ok(())
}

#[test]
fn test_modify_sql_ast_unqualified_ref_in_join() {
    let ctx = get_test_tenant_ctx();
    // A bare column reference in a multi-relation FROM can't be attributed
    // to a specific cube, so it doesn't expose the member
    let sql = "\
        SELECT sub.customer_gender FROM (\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            CROSS JOIN Logs\
        ) AS sub\
    ";
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["test".to_string()]),
        ..Default::default()
    });
    let err = modify_sql_ast(sql, &action, &ctx).unwrap_err();
    assert!(
        err.to_string()
            .contains("is not available in the outermost SELECT"),
        "unexpected error: {}",
        err
    );
}

#[tokio::test]
async fn test_delete_sql_filters_unresolvable_member() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    // The member is not selected by the query, so there is nothing to
    // delete - that is a no-op rather than an error
    let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";
    let filters = vec![V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.notes".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["x".to_string()]),
        ..Default::default()
    }];
    let result = delete_sql_filters(sql, &filters, meta, session).await?;
    assert_eq!(result.sql, sql);

    Ok(())
}

#[tokio::test]
async fn test_add_sql_filters_measure_round_trip() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    let sql = "\
        SELECT customer_gender, MAX(maxPrice) AS max_price \
        FROM KibanaSampleDataEcommerce \
        GROUP BY 1\
    ";

    // Measure filters land in HAVING as a synthesized aggregation, which
    // has to be recognized by the filter rewrite rules
    for (member, operator, value) in [
        ("KibanaSampleDataEcommerce.maxPrice", "gt", "10"),
        ("KibanaSampleDataEcommerce.maxPrice", "equals", "42"),
        ("KibanaSampleDataEcommerce.count", "gte", "1"),
    ] {
        let filters = vec![V1LoadRequestQueryFilterItem {
            member: Some(member.to_string()),
            operator: Some(operator.to_string()),
            values: Some(vec![value.to_string()]),
            ..Default::default()
        }];
        let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
        assert!(
            result.sql.contains("HAVING"),
            "{} {} filter did not produce a HAVING clause: {}",
            member,
            operator,
            result.sql
        );
        assert!(
            result
                .filters
                .iter()
                .any(|filter| filter_key(filter, &meta) == filter_key(&filters[0], &meta)),
            "{} {} filter did not round trip, got {:?}",
            member,
            operator,
            result.filters
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_add_sql_filters_date_round_trip() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    let sql = "SELECT order_date FROM KibanaSampleDataEcommerce GROUP BY 1";

    // Date range filters are reconstructed from the time dimension of the
    // plan rather than from its filters, and their values are reshaped by
    // the planner
    let filters = vec![V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
        operator: Some("inDateRange".to_string()),
        values: Some(vec!["2024-01-01".to_string(), "2024-12-31".to_string()]),
        ..Default::default()
    }];
    let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
    assert!(
        result
            .filters
            .iter()
            .any(|filter| filter_key(filter, &meta) == filter_key(&filters[0], &meta)),
        "inDateRange filter did not round trip, got {:?}",
        result.filters
    );

    // A negated range has to be emitted as NOT BETWEEN to be recognized
    let filters = vec![V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
        operator: Some("notInDateRange".to_string()),
        values: Some(vec!["2024-01-01".to_string(), "2024-12-31".to_string()]),
        ..Default::default()
    }];
    let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
    assert_eq!(
        result.sql,
        "\
        SELECT order_date FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"order_date\" \
            NOT BETWEEN '2024-01-01' AND '2024-12-31T23:59:59.999' \
        GROUP BY 1\
        "
    );
    assert!(
        result
            .filters
            .iter()
            .any(|filter| filter_key(filter, &meta) == filter_key(&filters[0], &meta)),
        "notInDateRange filter did not round trip, got {:?}",
        result.filters
    );

    // Single-bound date operators stay plain filters
    for (operator, value) in [
        ("beforeDate", "2024-06-01"),
        ("afterOrOnDate", "2024-07-01"),
    ] {
        let filters = vec![V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some(operator.to_string()),
            values: Some(vec![value.to_string()]),
            ..Default::default()
        }];
        let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
        assert!(
            result
                .filters
                .iter()
                .any(|filter| filter_key(filter, &meta) == filter_key(&filters[0], &meta)),
            "{} filter did not round trip, got {:?}",
            operator,
            result.filters
        );
    }

    Ok(())
}

#[test]
fn test_modify_sql_ast_string_value_quoting() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

    // Single quotes in string values must be doubled, so that the value
    // can't terminate the literal and inject SQL
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["O'Brien' OR 1=1 --".to_string()]),
        ..Default::default()
    });
    let (modified_sql, applied) = modify_sql_ast(sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'O''Brien'' OR 1=1 --' \
        GROUP BY 1\
        "
    );
    assert!(applied);

    // The rewritten SQL still parses as a single predicate
    let query = parse_single_query(&modified_sql)?;
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected a plain SELECT");
    };
    assert!(matches!(
        select.selection,
        Some(ast::Expr::BinaryOp {
            op: ast::BinaryOperator::Eq,
            ..
        })
    ));

    // LIKE patterns quote the same way
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("contains".to_string()),
        values: Some(vec!["O'Brien".to_string()]),
        ..Default::default()
    });
    let (modified_sql, applied) = modify_sql_ast(sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%O''Brien%' \
        GROUP BY 1\
        "
    );
    assert!(applied);

    Ok(())
}

#[tokio::test]
async fn test_delete_sql_filters_also_present_in_cte() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    // The same filter is present both in the CTE and in the outermost
    // SELECT: deleting the outermost one is correct even though the plan
    // still carries the CTE's copy
    let sql = "\
        WITH recent AS (\
            SELECT customer_gender \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test'\
        ) \
        SELECT customer_gender FROM recent \
        WHERE recent.\"customer_gender\" = 'test' \
        GROUP BY 1\
    ";
    let filters = vec![V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["test".to_string()]),
        ..Default::default()
    }];
    let result = delete_sql_filters(sql, &filters, meta, session).await?;
    assert_eq!(
        result.sql,
        "\
        WITH recent AS (\
            SELECT customer_gender \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test'\
        ) \
        SELECT customer_gender FROM recent \
        GROUP BY 1\
        "
    );

    Ok(())
}

#[test]
fn test_modify_sql_ast_cte_name_case_mismatch() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    // Unquoted identifiers are case-insensitive: the CTE reference and its
    // declaration may differ in case
    let sql = "\
        WITH Gendered AS (\
            SELECT customer_gender AS gender FROM KibanaSampleDataEcommerce\
        ) \
        SELECT gender FROM gendered\
    ";
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["test".to_string()]),
        ..Default::default()
    });
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        WITH Gendered AS (\
            SELECT customer_gender AS gender FROM KibanaSampleDataEcommerce\
        ) \
        SELECT gender FROM gendered \
        WHERE gendered.gender = 'test'\
        "
    );
    assert!(applied);

    Ok(())
}

#[tokio::test]
async fn test_set_removes_only_what_the_plan_reports() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    // A filter the plan reports is what `set` removes
    let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce \
               WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' GROUP BY 1";
    let result = set_sql_filters(sql, &[], meta.clone(), session.clone()).await?;
    assert_eq!(
        result.sql,
        "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1"
    );

    // A predicate over a computed expression keeps the clause out of the Cube
    // query, so nothing is reported and `set` may remove nothing
    let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce \
               WHERE LOWER(customer_gender) = 'test' \
               AND KibanaSampleDataEcommerce.\"customer_gender\" = 'test' GROUP BY 1";
    let result = set_sql_filters(sql, &[], meta, session).await?;
    assert_eq!(result.sql, sql);
    assert!(
        result.filters.is_empty(),
        "unexpected: {:?}",
        result.filters
    );

    Ok(())
}

#[test]
fn test_modify_sql_ast_identifier_case_mismatch() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    // The relation is matched case-insensitively, so the qualifier and the
    // column of the projection may be written in a different case
    let sql = "\
        SELECT customer_gender FROM (\
            SELECT KibanaSampleDataEcommerce.CUSTOMER_GENDER AS customer_gender \
            FROM kibanasampledataecommerce\
        ) AS t\
    ";
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["test".to_string()]),
        ..Default::default()
    });
    let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT customer_gender FROM (\
            SELECT KibanaSampleDataEcommerce.CUSTOMER_GENDER AS customer_gender \
            FROM kibanasampledataecommerce\
        ) AS t \
        WHERE t.customer_gender = 'test'\
        "
    );
    assert!(applied);

    Ok(())
}

#[test]
fn test_modify_sql_ast_wildcard_over_join() {
    let ctx = get_test_tenant_ctx();
    // A wildcard over a join exposes columns of every relation, so a bare
    // dimension name can't be attributed to the cube
    let sql = "\
        SELECT customer_gender FROM (\
            SELECT * FROM KibanaSampleDataEcommerce CROSS JOIN Logs\
        ) AS sub \
        GROUP BY 1\
    ";
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["test".to_string()]),
        ..Default::default()
    });
    let err = modify_sql_ast(sql, &action, &ctx).unwrap_err();
    assert!(
        err.to_string()
            .contains("is not available in the outermost SELECT"),
        "unexpected error: {}",
        err
    );
}

#[test]
fn test_modify_sql_ast_count_distinct_measure() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    let sql = "SELECT content, COUNT(DISTINCT agentCount) FROM Logs GROUP BY 1";

    // `countDistinct` is how the meta API spells the aggregation
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("Logs.agentCount".to_string()),
        operator: Some("gt".to_string()),
        values: Some(vec!["10".to_string()]),
        ..Default::default()
    });
    let (modified_sql, applied) = modify_sql_ast(sql, &action, &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT content, COUNT(DISTINCT agentCount) FROM Logs \
        GROUP BY 1 \
        HAVING COUNT(DISTINCT Logs.\"agentCount\") > 10\
        "
    );
    assert!(applied);

    // `countDistinctApprox` has no exact SQL equivalent and stays on the
    // MEASURE path
    let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
        member: Some("Logs.agentCountApprox".to_string()),
        operator: Some("gt".to_string()),
        values: Some(vec!["10".to_string()]),
        ..Default::default()
    });
    let (modified_sql, applied) = modify_sql_ast(sql, &action, &ctx)?;
    assert!(
        modified_sql.contains("HAVING MEASURE(Logs.\"agentCountApprox\") > 10"),
        "unexpected SQL: {}",
        modified_sql
    );
    assert!(applied);

    Ok(())
}

#[tokio::test]
async fn test_add_delete_sql_filters_and_group() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

    // A top-level "and" group is flattened by the planner into sibling
    // filters, so it is verified through its members
    let filters = vec![and_group(vec![
        serde_json::json!({
            "member": "KibanaSampleDataEcommerce.customer_gender",
            "operator": "equals",
            "values": ["x"],
        }),
        serde_json::json!({
            "member": "KibanaSampleDataEcommerce.notes",
            "operator": "equals",
            "values": ["y"],
        }),
    ])];
    let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
    assert_eq!(
        result.sql,
        "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE (KibanaSampleDataEcommerce.\"customer_gender\" = 'x' \
            AND KibanaSampleDataEcommerce.\"notes\" = 'y') \
        GROUP BY 1\
        "
    );

    // A single-member "and" group is emitted as a plain filter
    let single = vec![and_group(vec![serde_json::json!({
        "member": "KibanaSampleDataEcommerce.notes",
        "operator": "equals",
        "values": ["z"],
    })])];
    let result = add_sql_filters(sql, &single, meta.clone(), session.clone()).await?;
    assert_eq!(
        result.sql,
        "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"notes\" = 'z' \
        GROUP BY 1\
        "
    );

    // A single-member "or" group is emitted as a plain filter too
    let single = vec![or_group(vec![serde_json::json!({
        "member": "KibanaSampleDataEcommerce.notes",
        "operator": "equals",
        "values": ["z"],
    })])];
    let result = add_sql_filters(sql, &single, meta.clone(), session.clone()).await?;
    assert_eq!(
        result.sql,
        "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"notes\" = 'z' \
        GROUP BY 1\
        "
    );

    // An "and" group nested inside an "or" survives as a group
    let nested = vec![or_group(vec![
        serde_json::json!({
            "member": "KibanaSampleDataEcommerce.customer_gender",
            "operator": "equals",
            "values": ["x"],
        }),
        serde_json::json!({
            "and": [
                {
                    "member": "KibanaSampleDataEcommerce.notes",
                    "operator": "equals",
                    "values": ["y"],
                },
                {
                    "member": "KibanaSampleDataEcommerce.taxful_total_price",
                    "operator": "gt",
                    "values": ["1"],
                },
            ],
        }),
    ])];
    let result = add_sql_filters(sql, &nested, meta.clone(), session).await?;
    assert!(
        result
            .filters
            .iter()
            .any(|filter| filter_key(filter, &meta) == filter_key(&nested[0], &meta)),
        "nested group did not round trip, got {:?}",
        result.filters
    );

    Ok(())
}

#[tokio::test]
async fn test_add_sql_filters_multi_value_and_null_round_trip() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

    // Multi-value equality becomes IN / NOT IN, and the null checks
    // become IS [NOT] NULL
    for (operator, values) in [
        ("equals", Some(vec!["a".to_string(), "b".to_string()])),
        ("notEquals", Some(vec!["a".to_string(), "b".to_string()])),
        ("set", None),
        ("notSet", None),
    ] {
        let filters = vec![V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some(operator.to_string()),
            values,
            ..Default::default()
        }];
        let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
        assert!(
            result
                .filters
                .iter()
                .any(|filter| filter_key(filter, &meta) == filter_key(&filters[0], &meta)),
            "{} filter did not round trip, got {:?}",
            operator,
            result.filters
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_add_sql_filters_nested_group_normalization() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

    // The normalizations that AST generation and the planner perform reach
    // every depth of a filter tree, not just its root
    let cases = vec![
        // A multi-value LIKE-family filter chains with OR, which the
        // engine collapses into the enclosing group
        (
            "multi-value contains inside an or",
            or_group(vec![
                serde_json::json!({
                    "member": "KibanaSampleDataEcommerce.customer_gender",
                    "operator": "contains",
                    "values": ["a", "b"],
                }),
                serde_json::json!({
                    "member": "KibanaSampleDataEcommerce.notes",
                    "operator": "equals",
                    "values": ["y"],
                }),
            ]),
        ),
        // The negated operators chain with AND, which survives as a group
        // of its own inside the enclosing or
        (
            "multi-value notContains inside an or",
            or_group(vec![
                serde_json::json!({
                    "member": "KibanaSampleDataEcommerce.customer_gender",
                    "operator": "notContains",
                    "values": ["a", "b"],
                }),
                serde_json::json!({
                    "member": "KibanaSampleDataEcommerce.notes",
                    "operator": "equals",
                    "values": ["y"],
                }),
            ]),
        ),
        // A single-member group is emitted as its member alone
        (
            "single-member and inside an or",
            or_group(vec![
                serde_json::json!({
                    "and": [{
                        "member": "KibanaSampleDataEcommerce.notes",
                        "operator": "equals",
                        "values": ["y"],
                    }],
                }),
                serde_json::json!({
                    "member": "KibanaSampleDataEcommerce.customer_gender",
                    "operator": "equals",
                    "values": ["x"],
                }),
            ]),
        ),
    ];

    for (label, filter) in cases {
        let filters = vec![filter];
        let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
        let extracted = result
            .filters
            .iter()
            .map(|filter| filter_key(filter, &meta))
            .collect::<HashSet<_>>();
        let expected = verification_keys(&filters[0], &meta);
        assert!(
            expected.iter().all(|key| extracted.contains(key)),
            "{} did not round trip, wanted {:?}, got {:?}",
            label,
            expected,
            result.filters
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_sql_filters_count_is_bounded() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";
    let leaf = |i: usize| {
        serde_json::json!({
            "member": "KibanaSampleDataEcommerce.customer_gender",
            "operator": "equals",
            "values": [format!("v{}", i)],
        })
    };

    // A group nests any number of leaves inside a single array entry, so
    // counting entries alone would let the bound be walked around
    let nested = vec![or_group((0..MAX_FILTERS + 1).map(leaf).collect())];
    let err = add_sql_filters(sql, &nested, meta.clone(), session.clone())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("At most"),
        "unexpected error: {}",
        err
    );

    // A group whose `and` is present but null is a plain `or` group, and
    // its members still count
    let null_and = vec![or_group(vec![serde_json::json!({
        "and": null,
        "or": (0..MAX_FILTERS + 1).map(leaf).collect::<Vec<_>>(),
    })])];
    let err = add_sql_filters(sql, &null_and, meta.clone(), session.clone())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("At most"),
        "unexpected error: {}",
        err
    );

    // Nesting a level deeper doesn't help either
    let deeply_nested = vec![or_group(vec![serde_json::json!({
        "and": (0..MAX_FILTERS + 1).map(leaf).collect::<Vec<_>>(),
    })])];
    let err = add_sql_filters(sql, &deeply_nested, meta.clone(), session.clone())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("At most"),
        "unexpected error: {}",
        err
    );

    // The bound applies to every operation that takes filters
    let flat = (0..MAX_FILTERS + 1)
        .map(|i| V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec![format!("v{}", i)]),
            ..Default::default()
        })
        .collect::<Vec<_>>();
    for result in [
        set_sql_filters(sql, &flat, meta.clone(), session.clone()).await,
        delete_sql_filters(sql, &flat, meta.clone(), session.clone()).await,
        replace_sql_filters(sql, &flat, &[], meta.clone(), session.clone()).await,
        replace_sql_filters(sql, &flat[..1], &flat, meta.clone(), session.clone()).await,
    ] {
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("At most"),
            "unexpected error: {}",
            err
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_sql_filters_boolean_member() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    let sql = "SELECT has_subscription FROM KibanaSampleDataEcommerce GROUP BY 1";
    let filter = |value: &str| V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.has_subscription".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec![value.to_string()]),
        ..Default::default()
    };

    // A boolean member is filtered with a boolean literal, which is what
    // the filter rewrite rules read
    for value in ["true", "false"] {
        let filters = vec![filter(value)];
        let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
        assert_eq!(
            result.sql,
            format!(
                "SELECT has_subscription FROM KibanaSampleDataEcommerce \
                 WHERE KibanaSampleDataEcommerce.\"has_subscription\" = {} \
                 GROUP BY 1",
                value
            )
        );
        assert!(
            result
                .filters
                .iter()
                .any(|extracted| filter_key(extracted, &meta) == filter_key(&filters[0], &meta)),
            "{} filter did not round trip, got {:?}",
            value,
            result.filters
        );
    }

    // A value that is not a boolean is rejected rather than passed through
    let ctx = get_test_tenant_ctx();
    let err = modify_sql_ast(sql, &ModifyAction::Add(filter("notabool")), &ctx).unwrap_err();
    assert!(
        err.to_string().contains("must be a boolean"),
        "unexpected error: {}",
        err
    );

    Ok(())
}

#[test]
fn test_modify_sql_ast_alias_quoting_is_preserved() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    let action = || {
        ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["x".to_string()]),
            ..Default::default()
        })
    };

    // Quoting decides whether an identifier folds, so the alias is emitted
    // exactly as the projection wrote it - unquoted here,
    let sql = "\
        SELECT t.Gender FROM (\
            SELECT customer_gender AS Gender FROM KibanaSampleDataEcommerce\
        ) AS t \
        GROUP BY 1\
    ";
    let (modified_sql, applied) = modify_sql_ast(sql, &action(), &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT t.Gender FROM (\
            SELECT customer_gender AS Gender FROM KibanaSampleDataEcommerce\
        ) AS t \
        WHERE t.Gender = 'x' \
        GROUP BY 1\
        "
    );
    assert!(applied);

    // and quoted there
    let sql = "\
        SELECT t.\"Gender\" FROM (\
            SELECT customer_gender AS \"Gender\" FROM KibanaSampleDataEcommerce\
        ) AS t \
        GROUP BY 1\
    ";
    let (modified_sql, applied) = modify_sql_ast(sql, &action(), &ctx)?;
    assert_eq!(
        modified_sql,
        "\
        SELECT t.\"Gender\" FROM (\
            SELECT customer_gender AS \"Gender\" FROM KibanaSampleDataEcommerce\
        ) AS t \
        WHERE t.\"Gender\" = 'x' \
        GROUP BY 1\
        "
    );
    assert!(applied);

    Ok(())
}

#[tokio::test]
async fn test_sql_filters_cte_chain() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    let gender = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["test".to_string()]),
        ..Default::default()
    };

    // A member is followed through a chain of relations, each of which may
    // rename it, and the predicate names it as the outermost one does
    let cases = vec![
        (
            "WITH t0 AS (SELECT customer_gender FROM KibanaSampleDataEcommerce), \
             t1 AS (SELECT customer_gender FROM t0) \
             SELECT customer_gender FROM t1 GROUP BY 1",
            "t1.\"customer_gender\" = 'test'",
        ),
        (
            "WITH t0 AS (SELECT customer_gender FROM KibanaSampleDataEcommerce), \
             t1 AS (SELECT customer_gender FROM t0), \
             t2 AS (SELECT customer_gender FROM t1) \
             SELECT customer_gender FROM t2 GROUP BY 1",
            "t2.\"customer_gender\" = 'test'",
        ),
        (
            "WITH t0 AS (SELECT customer_gender AS g FROM KibanaSampleDataEcommerce), \
             t1 AS (SELECT g AS gg FROM t0) \
             SELECT gg FROM t1 GROUP BY 1",
            "t1.gg = 'test'",
        ),
        (
            "SELECT t.customer_gender FROM (\
                 SELECT u.customer_gender FROM (\
                     SELECT customer_gender FROM KibanaSampleDataEcommerce\
                 ) AS u\
             ) AS t GROUP BY 1",
            "t.customer_gender = 'test'",
        ),
    ];

    for (sql, predicate) in cases {
        let result =
            add_sql_filters(sql, slice::from_ref(&gender), meta.clone(), session.clone()).await?;
        assert!(
            result.sql.contains(predicate),
            "expected {} in {}",
            predicate,
            result.sql
        );
        assert!(
            result
                .filters
                .iter()
                .any(|filter| filter_key(filter, &meta) == filter_key(&gender, &meta)),
            "filter did not round trip through {}, got {:?}",
            sql,
            result.filters
        );

        // and deleting it puts the query back as it was
        let deleted = delete_sql_filters(
            &result.sql,
            slice::from_ref(&gender),
            meta.clone(),
            session.clone(),
        )
        .await?;
        assert!(
            !deleted.sql.contains(predicate),
            "predicate survived deletion in {}",
            deleted.sql
        );
    }

    // A measure aggregated at the bottom of a chain is forwarded the same way
    let max_price = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
        operator: Some("gt".to_string()),
        values: Some(vec!["10".to_string()]),
        ..Default::default()
    };
    let sql = "WITH t0 AS (\
                   SELECT customer_gender, MAX(maxPrice) AS mp \
                   FROM KibanaSampleDataEcommerce GROUP BY 1\
               ), t1 AS (SELECT customer_gender, mp FROM t0) \
               SELECT customer_gender, mp FROM t1";
    let result = add_sql_filters(
        sql,
        slice::from_ref(&max_price),
        meta.clone(),
        session.clone(),
    )
    .await?;
    assert!(
        result.sql.contains("WHERE t1.mp > 10"),
        "unexpected SQL: {}",
        result.sql
    );

    // A column computed anywhere along the chain is still not a member
    let sql = "WITH t0 AS (\
                   SELECT LOWER(customer_gender) AS customer_gender \
                   FROM KibanaSampleDataEcommerce\
               ), t1 AS (SELECT customer_gender FROM t0) \
               SELECT customer_gender FROM t1 GROUP BY 1";
    let err = add_sql_filters(sql, slice::from_ref(&gender), meta, session)
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("is not available in the outermost SELECT"),
        "unexpected error: {}",
        err
    );

    Ok(())
}

/// The native layer runs these on a multi-threaded runtime, so their futures
/// must stay `Send`: a `!Sync` value held across an await, such as the
/// relation-expansion budget, would stop this compiling.
#[tokio::test(flavor = "multi_thread")]
async fn test_sql_filters_futures_are_send() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1".to_string();
    let filters = vec![V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["test".to_string()]),
        ..Default::default()
    }];

    let spawned = tokio::spawn({
        let (meta, session, sql, filters) =
            (meta.clone(), session.clone(), sql.clone(), filters.clone());
        async move {
            let added = add_sql_filters(&sql, &filters, meta.clone(), session.clone()).await?;
            let filters = get_sql_filters(&added.sql, meta.clone(), session.clone()).await?;
            let set = set_sql_filters(&added.sql, &filters, meta.clone(), session.clone()).await?;
            let replaced =
                replace_sql_filters(&set.sql, &filters, &filters, meta.clone(), session.clone())
                    .await?;
            delete_sql_filters(&replaced.sql, &filters, meta, session).await
        }
    });

    let result = spawned
        .await
        .map_err(|e| CubeError::internal(format!("join error: {}", e)))??;
    assert_eq!(result.sql, sql);

    Ok(())
}

#[test]
fn test_modify_sql_ast_large_clause() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    let filter = |i: usize| V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec![format!("v{}", i)]),
        ..Default::default()
    };

    // A clause holds a predicate per filter, so a batch the size of the
    // filter limit builds one longer than a recursive walk could carry
    let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";
    let actions = (0..MAX_FILTERS)
        .map(|i| ModifyAction::Add(filter(i)))
        .collect::<Vec<_>>();
    let (built, applied) = modify_parsed_query(sql, &actions, &ctx, &ReportedFilters::none())?;
    assert!(applied.iter().all(|applied| *applied));
    assert_eq!(built.matches(" AND ").count(), MAX_FILTERS - 1);

    // and that clause can be walked again, to find and remove one filter,
    // and to remove every one of them as `set` does
    let (removed, applied) = modify_sql_ast(&built, &ModifyAction::Remove(filter(0)), &ctx)?;
    assert!(applied);
    assert_eq!(removed.matches(" AND ").count(), MAX_FILTERS - 2);
    let all = (0..MAX_FILTERS).map(filter).collect::<Vec<_>>();
    let (cleared, _) = modify_parsed_query(
        &built,
        &set_actions(&all, &[]),
        &ctx,
        &ReportedFilters::of(&all, &ctx),
    )?;
    assert_eq!(
        cleared,
        "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1"
    );

    // An edit that shrinks a clause near the bound is taken at the size it
    // leaves: removing ten and adding five is under the bound, though adding
    // five to the clause as parsed would not be
    let mut clause = String::from("1 = 1");
    for i in 0..MAX_CLAUSE_PREDICATES - 3 {
        clause.push_str(&format!(" AND customer_gender = 'v{}'", i));
    }
    let sql = format!(
        "SELECT customer_gender FROM KibanaSampleDataEcommerce WHERE {} GROUP BY 1",
        clause
    );
    let shrinking = (0..10)
        .map(|i| ModifyAction::Remove(filter(i)))
        .chain((0..5).map(|i| ModifyAction::Add(filter(MAX_CLAUSE_PREDICATES + i))))
        .collect::<Vec<_>>();
    let (_, applied) = modify_parsed_query(&sql, &shrinking, &ctx, &ReportedFilters::none())?;
    assert!(applied.iter().all(|applied| *applied));

    // Past the clause bound the request is refused rather than attempted
    let mut clause = String::from("1 = 1");
    for i in 0..MAX_CLAUSE_PREDICATES {
        clause.push_str(&format!(" AND customer_gender = 'v{}'", i));
    }
    let sql = format!(
        "SELECT customer_gender FROM KibanaSampleDataEcommerce WHERE {} GROUP BY 1",
        clause
    );
    for result in [
        modify_sql_ast(&sql, &ModifyAction::Remove(filter(0)), &ctx).map(|(sql, _)| sql),
        modify_sql_ast(&sql, &ModifyAction::Add(filter(0)), &ctx).map(|(sql, _)| sql),
    ] {
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("more than the"),
            "unexpected error: {}",
            err
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_sql_filters_sibling_relation() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    // The cube is in the FROM, but the projection reads the member from a
    // sibling relation, so finding the cube can't end the search
    let sql = "\
        SELECT x.g FROM (\
            SELECT t.g FROM KibanaSampleDataEcommerce k \
            JOIN (SELECT customer_gender AS g FROM KibanaSampleDataEcommerce) t ON true\
        ) x \
        GROUP BY 1\
    ";
    let filters = vec![V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["test".to_string()]),
        ..Default::default()
    }];
    let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
    assert!(
        result.sql.contains("WHERE x.g = 'test'"),
        "unexpected SQL: {}",
        result.sql
    );
    assert!(
        result
            .filters
            .iter()
            .any(|filter| filter_key(filter, &meta) == filter_key(&filters[0], &meta)),
        "filter did not round trip, got {:?}",
        result.filters
    );

    // and what `add` can write, `set` may drop
    let cleared = set_sql_filters(&result.sql, &[], meta, session).await?;
    assert!(
        !cleared.sql.contains("WHERE"),
        "predicate survived set: {}",
        cleared.sql
    );

    Ok(())
}

#[test]
fn test_modify_sql_ast_group_as_whole_clause() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    let group = and_group(vec![
        serde_json::json!({
            "member": "KibanaSampleDataEcommerce.customer_gender",
            "operator": "equals",
            "values": ["x"],
        }),
        serde_json::json!({
            "member": "KibanaSampleDataEcommerce.notes",
            "operator": "equals",
            "values": ["y"],
        }),
    ]);
    let base = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

    // A group added to a query with no WHERE becomes the whole clause,
    // parentheses and all
    let (with_group, applied) = modify_sql_ast(base, &ModifyAction::Add(group.clone()), &ctx)?;
    assert_eq!(
        with_group,
        "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE (KibanaSampleDataEcommerce.\"customer_gender\" = 'x' \
            AND KibanaSampleDataEcommerce.\"notes\" = 'y') \
        GROUP BY 1\
        "
    );
    assert!(applied);

    // and is still matched as that group: adding it again is a no-op,
    let (again, applied) = modify_sql_ast(&with_group, &ModifyAction::Add(group.clone()), &ctx)?;
    assert_eq!(again, with_group);
    assert!(!applied);

    // deleting it takes the whole clause,
    let (deleted, applied) =
        modify_sql_ast(&with_group, &ModifyAction::Remove(group.clone()), &ctx)?;
    assert_eq!(deleted, base);
    assert!(applied);

    // and replacing it swaps the group rather than its members
    let action = ModifyAction::Replace {
        old: group,
        new: V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.notes".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["z".to_string()]),
            ..Default::default()
        },
    };
    let (replaced, applied) = modify_sql_ast(&with_group, &action, &ctx)?;
    assert_eq!(
        replaced,
        "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"notes\" = 'z' \
        GROUP BY 1\
        "
    );
    assert!(applied);

    // A filter inside those parentheses is still reachable on its own
    let leaf = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["x".to_string()]),
        ..Default::default()
    };
    let (leaf_deleted, applied) = modify_sql_ast(&with_group, &ModifyAction::Remove(leaf), &ctx)?;
    assert_eq!(
        leaf_deleted,
        "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"notes\" = 'y' \
        GROUP BY 1\
        "
    );
    assert!(applied);

    Ok(())
}

#[test]
fn test_modify_sql_ast_group_member_reachability() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    let group = and_group(vec![
        serde_json::json!({
            "member": "KibanaSampleDataEcommerce.customer_gender",
            "operator": "equals",
            "values": ["x"],
        }),
        serde_json::json!({
            "member": "KibanaSampleDataEcommerce.notes",
            "operator": "equals",
            "values": ["y"],
        }),
    ]);
    let member = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["x".to_string()]),
        ..Default::default()
    };
    let absent = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.notes".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["absent".to_string()]),
        ..Default::default()
    };
    let other = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.notes".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["c".to_string()]),
        ..Default::default()
    };

    let base = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";
    let (alone, _) = modify_sql_ast(base, &ModifyAction::Add(group.clone()), &ctx)?;
    let (mixed, _) = modify_sql_ast(&alone, &ModifyAction::Add(other), &ctx)?;

    // The rewrite engine flattens a top-level "and" group into sibling
    // filters, so its members are filters of their own whether the group
    // stands alone in the clause or sits next to something else
    for clause in [&alone, &mixed] {
        // A member is removable on its own, which leaves its siblings
        let (removed, applied) =
            modify_sql_ast(clause, &ModifyAction::Remove(member.clone()), &ctx)?;
        assert!(applied);
        assert!(
            !removed.contains("\"customer_gender\" = 'x'"),
            "member survived removal: {}",
            removed
        );
        assert!(
            removed.contains("\"notes\" = 'y'"),
            "sibling was removed too: {}",
            removed
        );

        // and adding it back is a no-op rather than a duplicate
        let (added, applied) = modify_sql_ast(clause, &ModifyAction::Add(member.clone()), &ctx)?;
        assert_eq!(&added, clause);
        assert!(!applied);

        // The group is still removable as a whole
        let (removed, applied) =
            modify_sql_ast(clause, &ModifyAction::Remove(group.clone()), &ctx)?;
        assert!(applied);
        assert!(
            !removed.contains("\"customer_gender\" = 'x'") && !removed.contains("\"notes\" = 'y'"),
            "group survived removal: {}",
            removed
        );

        // and a filter that isn't there leaves the clause as it was,
        // parentheses included
        let (untouched, applied) =
            modify_sql_ast(clause, &ModifyAction::Remove(absent.clone()), &ctx)?;
        assert_eq!(&untouched, clause);
        assert!(!applied);
    }

    Ok(())
}

#[test]
fn test_modify_sql_ast_group_added_after_its_members() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    let filter = |member: &str, value: &str| V1LoadRequestQueryFilterItem {
        member: Some(format!("KibanaSampleDataEcommerce.{}", member)),
        operator: Some("equals".to_string()),
        values: Some(vec![value.to_string()]),
        ..Default::default()
    };
    let group = and_group(vec![
        serde_json::json!({
            "member": "KibanaSampleDataEcommerce.customer_gender",
            "operator": "equals",
            "values": ["x"],
        }),
        serde_json::json!({
            "member": "KibanaSampleDataEcommerce.notes",
            "operator": "equals",
            "values": ["y"],
        }),
    ]);

    // The rewrite engine reports a top-level "and" group as sibling
    // filters, so a clause that holds its members separately holds the
    // group itself, however the two got there
    let base = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";
    let (with_a, _) = modify_sql_ast(
        base,
        &ModifyAction::Add(filter("customer_gender", "x")),
        &ctx,
    )?;
    let (members, _) = modify_sql_ast(&with_a, &ModifyAction::Add(filter("notes", "y")), &ctx)?;

    // Adding the group they make up is a no-op, not a duplicate
    let (added, applied) = modify_sql_ast(&members, &ModifyAction::Add(group.clone()), &ctx)?;
    assert_eq!(added, members);
    assert!(!applied);

    // Deleting it takes both members
    let (deleted, applied) = modify_sql_ast(&members, &ModifyAction::Remove(group.clone()), &ctx)?;
    assert_eq!(deleted, base);
    assert!(applied);

    // and replacing it drops them for the new filter
    let action = ModifyAction::Replace {
        old: group.clone(),
        new: filter("notes", "z"),
    };
    let (replaced, applied) = modify_sql_ast(&members, &action, &ctx)?;
    assert_eq!(
        replaced,
        "\
        SELECT customer_gender FROM KibanaSampleDataEcommerce \
        WHERE KibanaSampleDataEcommerce.\"notes\" = 'z' \
        GROUP BY 1\
        "
    );
    assert!(applied);

    // With only part of the group present it is not present at all: the
    // filter is neither half-removed nor taken for a duplicate
    let (deleted, applied) = modify_sql_ast(&with_a, &ModifyAction::Remove(group.clone()), &ctx)?;
    assert_eq!(deleted, with_a);
    assert!(!applied);

    let (added, applied) = modify_sql_ast(&with_a, &ModifyAction::Add(group), &ctx)?;
    assert!(added.contains("AND (KibanaSampleDataEcommerce.\"customer_gender\" = 'x'"));
    assert!(applied);

    Ok(())
}

#[tokio::test]
async fn test_sql_filters_round_trip_reported_values() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    // The planner canonicalizes a date to a full timestamp whatever the
    // query wrote, so the values are reported back in the query's own
    // form and a filter handed to delete or replace still matches
    let sql = "SELECT order_date FROM KibanaSampleDataEcommerce \
               WHERE (KibanaSampleDataEcommerce.\"order_date\" >= '2020-01-01' \
               AND KibanaSampleDataEcommerce.\"order_date\" <= '2021-01-01') \
               GROUP BY 1";
    let reported = get_sql_filters(sql, meta.clone(), session.clone()).await?;
    assert_eq!(
        reported[0].values,
        // The query's upper bound is midnight, not the whole of the day
        Some(vec![
            "2020-01-01".to_string(),
            "2021-01-01T00:00:00.000Z".to_string()
        ]),
        "a date is reported in the form the query wrote it"
    );

    // Replacing it with an edited range keeps the query rewritable
    let edited = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
        operator: Some("inDateRange".to_string()),
        values: Some(vec!["2020-01-02".to_string(), "2021-01-01".to_string()]),
        ..Default::default()
    };
    let replaced = replace_sql_filters(
        sql,
        &reported,
        slice::from_ref(&edited),
        meta.clone(),
        session.clone(),
    )
    .await?;
    assert!(
        replaced.sql.contains("'2020-01-02'"),
        "unexpected SQL: {}",
        replaced.sql
    );

    // and deleting it takes the range it came from
    let deleted = delete_sql_filters(sql, &reported, meta.clone(), session.clone()).await?;
    assert_eq!(
        deleted.sql,
        "SELECT order_date FROM KibanaSampleDataEcommerce GROUP BY 1"
    );

    // A value carrying a real time is not a date, and is left as it is
    let sql = "SELECT order_date FROM KibanaSampleDataEcommerce \
               WHERE (KibanaSampleDataEcommerce.\"order_date\" >= '2020-01-01T12:30:00.000' \
               AND KibanaSampleDataEcommerce.\"order_date\" <= '2021-01-01T23:59:59.999') \
               GROUP BY 1";
    let reported = get_sql_filters(sql, meta.clone(), session.clone()).await?;
    let values = reported[0].values.clone().unwrap_or_default();
    assert!(
        values[0].starts_with("2020-01-01T12:30:00") && values[1] == "2021-01-01",
        "a timestamp was reported as something else: {:?}",
        values
    );

    // and it still matches the query it was read from
    let deleted = delete_sql_filters(sql, &reported, meta, session).await?;
    assert_eq!(
        deleted.sql,
        "SELECT order_date FROM KibanaSampleDataEcommerce GROUP BY 1"
    );

    Ok(())
}

/// A column can carry more than one reported filter, and then no filter
/// owns it: matching by column would take the other one with it, which
/// nothing in the response would say.
#[tokio::test]
async fn test_delete_keeps_a_second_filter_on_the_column() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    let sql = "SELECT order_date FROM KibanaSampleDataEcommerce \
               WHERE KibanaSampleDataEcommerce.\"order_date\" \
               >= CURRENT_DATE - INTERVAL '30 days' \
               AND KibanaSampleDataEcommerce.\"order_date\" IS NOT NULL \
               GROUP BY 1";
    let reported = get_sql_filters(sql, meta.clone(), session.clone()).await?;
    let operators = reported
        .iter()
        .filter_map(|filter| filter.operator.clone())
        .collect::<HashSet<_>>();
    assert_eq!(
        operators,
        HashSet::from(["afterOrOnDate".to_string(), "set".to_string()]),
        "the query should report two filters on one member: {:?}",
        reported
    );

    // The engine works the interval out when it plans, so the bound is
    // reported as a date the query does not hold and can only be matched
    // by the column it stands on
    let bound = reported
        .iter()
        .find(|filter| filter.operator.as_deref() == Some("afterOrOnDate"))
        .expect("the bound is reported")
        .clone();
    let deleted =
        delete_sql_filters(sql, slice::from_ref(&bound), meta.clone(), session.clone()).await?;
    assert!(
        deleted.sql.contains("IS NOT NULL"),
        "the filter that was not asked for was removed: {}",
        deleted.sql
    );
    assert!(
        deleted
            .filters
            .iter()
            .any(|filter| filter.operator.as_deref() == Some("set")),
        "unexpected filters: {:?}",
        deleted.filters
    );

    Ok(())
}

/// A date is only two ways of writing one value on a member that holds a
/// time. On a string member the two are two values, and a filter naming
/// one must not reach the other.
#[tokio::test]
async fn test_delete_keeps_a_date_shaped_string() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce \
               WHERE KibanaSampleDataEcommerce.\"customer_gender\" = '2024-01-01' \
               GROUP BY 1";
    let canonical = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["2024-01-01T00:00:00.000Z".to_string()]),
        ..Default::default()
    };

    let deleted = delete_sql_filters(
        sql,
        slice::from_ref(&canonical),
        meta.clone(),
        session.clone(),
    )
    .await?;
    assert!(
        deleted.sql.contains("'2024-01-01'"),
        "a string was matched by a date written another way: {}",
        deleted.sql
    );

    Ok(())
}

/// The filter being added by a replacement is matched under the member it
/// stands on, not under the one it replaces: what counts as the same
/// value differs between a time member and any other.
#[tokio::test]
async fn test_replace_adds_the_new_filter_under_its_own_member() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    // The date is written the way the planner canonicalizes it, and the
    // group being replaced stands on string members alone
    let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce \
               WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'FEMALE' \
               AND KibanaSampleDataEcommerce.\"notes\" = 'x' \
               AND KibanaSampleDataEcommerce.\"order_date\" >= '2020-01-01T00:00:00.000Z' \
               GROUP BY 1";
    let old = V1LoadRequestQueryFilterItem {
        and: Some(vec![
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.customer_gender",
                "operator": "equals",
                "values": ["FEMALE"],
            }),
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.notes",
                "operator": "equals",
                "values": ["x"],
            }),
        ]),
        ..Default::default()
    };
    let new = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
        operator: Some("afterOrOnDate".to_string()),
        values: Some(vec!["2020-01-01".to_string()]),
        ..Default::default()
    };

    let replaced = replace_sql_filters(
        sql,
        slice::from_ref(&old),
        slice::from_ref(&new),
        meta,
        session,
    )
    .await?;
    assert_eq!(
        replaced.sql.matches("order_date").count(),
        1,
        "the date the query already holds was written a second time: {}",
        replaced.sql
    );

    Ok(())
}

/// A CTE is only ever named unqualified: `public.orders` is the cube even
/// when a CTE is called `orders`, and the recognizer has to read it the
/// way the resolver does, or `set` keeps a filter it was asked to replace.
#[tokio::test]
async fn test_set_reads_a_qualified_relation_as_the_cube() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    let sql =
        "WITH KibanaSampleDataEcommerce AS (SELECT LOWER(content) AS customer_gender FROM Logs) \
               SELECT customer_gender, MEASURE(count) FROM public.KibanaSampleDataEcommerce \
               WHERE customer_gender = 'female' GROUP BY 1";
    let wanted = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["male".to_string()]),
        ..Default::default()
    };

    let set = set_sql_filters(sql, slice::from_ref(&wanted), meta.clone(), session).await?;
    assert_eq!(
        set.filters
            .iter()
            .map(|filter| filter_key(filter, &meta))
            .collect::<Vec<_>>(),
        vec![filter_key(&wanted, &meta)],
        "the filter on the cube was kept as if it were the CTE's: {}",
        set.sql
    );

    Ok(())
}

/// A batch keys the clause once, so an addition must see what re-keying would:
/// the query's filters, earlier additions, and the members of their groups.
#[test]
fn test_add_batch_sees_earlier_additions() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    let gender = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["female".to_string()]),
        ..Default::default()
    };
    let notes = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.notes".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["x".to_string()]),
        ..Default::default()
    };
    let group = V1LoadRequestQueryFilterItem {
        and: Some(vec![
            serde_json::to_value(&gender).unwrap(),
            serde_json::to_value(&notes).unwrap(),
        ]),
        ..Default::default()
    };

    let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce \
               WHERE KibanaSampleDataEcommerce.\"notes\" = 'x' GROUP BY 1";
    let actions = vec![
        // Already there
        ModifyAction::Add(notes.clone()),
        // New
        ModifyAction::Add(gender.clone()),
        // The same one again, within the batch
        ModifyAction::Add(gender.clone()),
        // Its members are all present by now
        ModifyAction::Add(group),
    ];
    let (built, applied) = modify_parsed_query(sql, &actions, &ctx, &ReportedFilters::none())?;

    assert_eq!(applied, vec![false, true, false, false]);
    assert_eq!(
        built,
        "SELECT customer_gender FROM KibanaSampleDataEcommerce \
         WHERE KibanaSampleDataEcommerce.\"notes\" = 'x' \
         AND KibanaSampleDataEcommerce.\"customer_gender\" = 'female' GROUP BY 1"
    );

    Ok(())
}

/// An append reaches every key set a batch keeps, one per way of keying, so a
/// plain leaf still sees the members of a group keyed with dates reduced.
#[test]
fn test_add_batch_sees_additions_keyed_the_other_way() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    let leaf = |member: &str, value: &str| V1LoadRequestQueryFilterItem {
        member: Some(format!("KibanaSampleDataEcommerce.{member}")),
        operator: Some("equals".to_string()),
        values: Some(vec![value.to_string()]),
        ..Default::default()
    };
    let range = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
        operator: Some("inDateRange".to_string()),
        values: Some(vec!["2024-01-01".to_string(), "2024-02-01".to_string()]),
        ..Default::default()
    };
    let group = V1LoadRequestQueryFilterItem {
        and: Some(vec![
            serde_json::to_value(&range).unwrap(),
            serde_json::to_value(leaf("notes", "y")).unwrap(),
        ]),
        ..Default::default()
    };

    let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce \
               WHERE KibanaSampleDataEcommerce.\"notes\" = 'x' GROUP BY 1";
    let actions = vec![
        // Keyed without date normalization
        ModifyAction::Add(leaf("customer_gender", "female")),
        // Keyed with it, as a time member is among its members
        ModifyAction::Add(group),
        // Keyed without it again, and already there through the group
        ModifyAction::Add(leaf("notes", "y")),
    ];
    let (built, applied) = modify_parsed_query(sql, &actions, &ctx, &ReportedFilters::none())?;

    assert_eq!(applied, vec![true, true, false]);
    assert_eq!(
        built.matches("'y'").count(),
        1,
        "a filter the group brought in was appended again: {built}"
    );

    Ok(())
}

/// Matching by column is the only path that removes predicates the caller
/// did not name, so its gate is pinned directly: it takes a filter the
/// plan reports alone on its member, and nothing else.
#[test]
fn test_remove_by_column_only_for_the_sole_reported_filter() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    let range = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
        operator: Some("inDateRange".to_string()),
        values: Some(vec!["2024-01-01".to_string(), "2024-02-01".to_string()]),
        ..Default::default()
    };
    let set = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
        operator: Some("set".to_string()),
        ..Default::default()
    };
    // The upper bound is strict, so the range as reported is not spelled
    // out by the query and can only be matched by its column
    let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce \
               WHERE KibanaSampleDataEcommerce.\"order_date\" >= '2024-01-01' \
               AND KibanaSampleDataEcommerce.\"order_date\" < '2024-02-01' GROUP BY 1";
    let remove = [ModifyAction::Remove(range.clone())];

    // Reported, and alone on its member: both bounds go
    let (built, applied) = modify_parsed_query(
        sql,
        &remove,
        &ctx,
        &ReportedFilters::of(slice::from_ref(&range), &ctx),
    )?;
    assert_eq!(applied, vec![true]);
    assert_eq!(
        built,
        "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1"
    );

    // Not reported: nothing is taken, though the column matches
    let (built, applied) = modify_parsed_query(sql, &remove, &ctx, &ReportedFilters::none())?;
    assert_eq!(applied, vec![false]);
    assert_eq!(built, sql);

    // Reported alongside a second filter on the member: nothing is taken
    let sql_with_set = format!(
        "{} AND KibanaSampleDataEcommerce.\"order_date\" IS NOT NULL",
        &sql[..sql.len() - " GROUP BY 1".len()]
    ) + " GROUP BY 1";
    let (built, applied) = modify_parsed_query(
        &sql_with_set,
        &remove,
        &ctx,
        &ReportedFilters::of(&[range, set], &ctx),
    )?;
    assert_eq!(applied, vec![false]);
    assert_eq!(built, sql_with_set);

    Ok(())
}

/// `set` removes what the plan reports and nothing else, so a predicate
/// extraction cannot represent - a LIKE with an inner wildcard - stays,
/// and the rewritten query never returns rows the original did not.
#[test]
fn test_set_leaves_a_predicate_the_plan_does_not_report() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    let gender = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["female".to_string()]),
        ..Default::default()
    };
    let notes = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.notes".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["x".to_string()]),
        ..Default::default()
    };

    let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce \
               WHERE KibanaSampleDataEcommerce.\"notes\" LIKE 'a%b' \
               AND KibanaSampleDataEcommerce.\"customer_gender\" = 'female' GROUP BY 1";
    // What the plan reports: the LIKE has no Cube filter to become
    let reported = [gender];
    let actions = set_actions(&reported, slice::from_ref(&notes));
    let (built, applied) =
        modify_parsed_query(sql, &actions, &ctx, &ReportedFilters::of(&reported, &ctx))?;

    assert_eq!(applied, vec![true, true]);
    assert_eq!(
        built,
        "SELECT customer_gender FROM KibanaSampleDataEcommerce \
         WHERE KibanaSampleDataEcommerce.\"notes\" LIKE 'a%b' \
         AND KibanaSampleDataEcommerce.\"notes\" = 'x' GROUP BY 1"
    );

    Ok(())
}

/// A filter listed twice in `old` is one filter to replace, not one found
/// and one missing.
#[tokio::test]
async fn test_replace_with_a_duplicated_old_filter() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce \
               WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'female' GROUP BY 1";
    let old = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["female".to_string()]),
        ..Default::default()
    };
    let new = V1LoadRequestQueryFilterItem {
        values: Some(vec!["male".to_string()]),
        ..old.clone()
    };

    let replaced = replace_sql_filters(
        sql,
        &[old.clone(), old],
        slice::from_ref(&new),
        meta,
        session,
    )
    .await?;
    assert!(
        replaced.sql.contains("'male'") && !replaced.sql.contains("'female'"),
        "unexpected SQL: {}",
        replaced.sql
    );

    Ok(())
}

/// cubesql resolves an unquoted identifier case-insensitively, so a query
/// writing a camelCase member bare names the same column this API writes
/// quoted, and a filter read back has to find it.
#[test]
fn test_remove_matches_an_identifier_whatever_its_case() -> DFResult<()> {
    use crate::compile::test::get_test_tenant_ctx_with_meta;
    use cubeclient::models::{V1CubeMeta, V1CubeMetaType};

    // The stock context has camelCase measures but no camelCase dimension
    let ctx = get_test_tenant_ctx_with_meta(vec![V1CubeMeta {
        name: "CamelCube".to_string(),
        description: None,
        title: None,
        r#type: V1CubeMetaType::Cube,
        dimensions: vec![V1CubeMetaDimension {
            name: "CamelCube.someString".to_string(),
            r#type: "string".to_string(),
            ..Default::default()
        }],
        measures: vec![V1CubeMetaMeasure {
            name: "CamelCube.maxPrice".to_string(),
            r#type: "number".to_string(),
            agg_type: Some("max".to_string()),
            ..Default::default()
        }],
        segments: vec![],
        joins: None,
        folders: None,
        nested_folders: None,
        hierarchies: None,
        meta: None,
    }]);

    let dimension = V1LoadRequestQueryFilterItem {
        member: Some("CamelCube.someString".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["x".to_string()]),
        ..Default::default()
    };
    for spelling in [
        "someString",
        "SOMESTRING",
        "\"someString\"",
        "CamelCube.somestring",
    ] {
        let sql = format!("SELECT someString FROM CamelCube WHERE {spelling} = 'x' GROUP BY 1");
        let (built, applied) =
            modify_sql_ast(&sql, &ModifyAction::Remove(dimension.clone()), &ctx)?;
        assert!(applied, "not found as {}: {}", spelling, built);
        assert_eq!(built, "SELECT someString FROM CamelCube GROUP BY 1");
    }

    let measure = V1LoadRequestQueryFilterItem {
        member: Some("CamelCube.maxPrice".to_string()),
        operator: Some("gt".to_string()),
        values: Some(vec!["5".to_string()]),
        ..Default::default()
    };
    for spelling in ["MEASURE(maxPrice)", "MEASURE(MAXPRICE)", "MAX(maxprice)"] {
        let sql = format!("SELECT someString FROM CamelCube GROUP BY 1 HAVING {spelling} > 5");
        let (built, applied) = modify_sql_ast(&sql, &ModifyAction::Remove(measure.clone()), &ctx)?;
        assert!(applied, "not found as {}: {}", spelling, built);
        assert_eq!(built, "SELECT someString FROM CamelCube GROUP BY 1");
    }

    Ok(())
}

/// A range's upper bound is written as the whole of its day, as the REST API
/// reads a bare date, and folds back to the date when reported.
#[test]
fn test_date_range_upper_bound_covers_the_day() {
    assert_eq!(date_range_upper("2024-12-31"), "2024-12-31T23:59:59.999");
    // A bound already carrying a time is left as written
    assert_eq!(
        date_range_upper("2024-12-31T12:00:00"),
        "2024-12-31T12:00:00"
    );
    assert_eq!(date_range_upper("last week"), "last week");

    // The end of the day is the date alone as an upper bound only; midnight
    // is the date alone everywhere else
    assert_eq!(
        report_date_value("2024-12-31T23:59:59.999", true),
        "2024-12-31"
    );
    assert_eq!(
        report_date_value("2024-12-31T23:59:59.999", false),
        "2024-12-31T23:59:59.999"
    );
    assert_eq!(
        report_date_value("2024-12-31T00:00:00.000Z", false),
        "2024-12-31"
    );
    assert_eq!(
        report_date_value("2024-12-31T00:00:00.000Z", true),
        "2024-12-31T00:00:00.000Z"
    );
    assert_eq!(
        report_date_value("2024-12-31T12:00:00.000", false),
        "2024-12-31T12:00:00.000"
    );
    assert_ne!(
        normalize_filter_value("2024-12-31T23:59:59.999"),
        normalize_filter_value("2024-12-31")
    );
}

/// A statement that compiles to something other than a plan - SET, SHOW,
/// BEGIN - is the caller's mistake, answered as such rather than as a
/// fault of this API.
#[tokio::test]
async fn test_non_select_statement_is_a_caller_error() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};
    use crate::CubeErrorCauseType;

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    let err = get_sql_filters("SET timezone = 'UTC'", meta, session)
        .await
        .expect_err("a SET statement has no filters to read");
    assert!(
        matches!(err.cause, CubeErrorCauseType::User(_)),
        "not a caller error: {:?}",
        err
    );
    assert!(err.message.contains("Only SELECT"), "{}", err.message);

    Ok(())
}

/// The filter bound is per request: `replace` counts its old and its new
/// filters together, not each array against the bound on its own.
#[tokio::test]
async fn test_replace_counts_old_and_new_filters_together() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let filters = |count: usize| {
        (0..count)
            .map(|i| V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some("equals".to_string()),
                values: Some(vec![format!("v{}", i)]),
                ..Default::default()
            })
            .collect::<Vec<_>>()
    };
    let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

    // Each array alone is under the bound; together they are over it
    let half = MAX_FILTERS / 2 + 1;
    let err = replace_sql_filters(sql, &filters(half), &filters(half), meta, session)
        .await
        .expect_err("old and new are one request");
    assert!(err.message.contains("At most"), "{}", err.message);

    Ok(())
}

/// A batch of removals lands where one at a time would: an exact conjunct, a
/// group's member, a whole group; a group not asked about keeps its parentheses.
#[test]
fn test_removals_are_applied_as_a_batch() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    let leaf = |member: &str, value: &str| V1LoadRequestQueryFilterItem {
        member: Some(format!("KibanaSampleDataEcommerce.{member}")),
        operator: Some("equals".to_string()),
        values: Some(vec![value.to_string()]),
        ..Default::default()
    };

    let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce \
               WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'a' \
               AND (KibanaSampleDataEcommerce.\"notes\" = 'b' AND KibanaSampleDataEcommerce.\"notes\" = 'c') \
               AND (KibanaSampleDataEcommerce.\"notes\" = 'd' AND KibanaSampleDataEcommerce.\"notes\" = 'e') \
               AND KibanaSampleDataEcommerce.\"notes\" = 'f' GROUP BY 1";
    let actions = vec![
        // An exact conjunct
        ModifyAction::Remove(leaf("customer_gender", "a")),
        // A member held inside a parenthesized group
        ModifyAction::Remove(leaf("notes", "b")),
        // One that is not there
        ModifyAction::Remove(leaf("notes", "z")),
        // The last conjunct
        ModifyAction::Remove(leaf("notes", "f")),
    ];
    let (built, applied) = modify_parsed_query(sql, &actions, &ctx, &ReportedFilters::none())?;

    assert_eq!(applied, vec![true, true, false, true]);
    assert_eq!(
        built,
        "SELECT customer_gender FROM KibanaSampleDataEcommerce \
         WHERE KibanaSampleDataEcommerce.\"notes\" = 'c' \
         AND KibanaSampleDataEcommerce.\"notes\" = 'd' \
         AND KibanaSampleDataEcommerce.\"notes\" = 'e' GROUP BY 1"
    );

    Ok(())
}

/// A BETWEEN is read as two filters. Removing one of them leaves the other
/// half spelled out, removing both leaves nothing, and a NOT BETWEEN is not
/// made of them.
#[test]
fn test_remove_one_half_of_a_between() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    let bound = |operator: &str, value: &str| V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
        operator: Some(operator.to_string()),
        values: Some(vec![value.to_string()]),
        ..Default::default()
    };
    let sql = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce \
               WHERE KibanaSampleDataEcommerce.taxful_total_price BETWEEN 100 AND 1000 \
               AND KibanaSampleDataEcommerce.customer_gender = 'a'";

    let (built, applied) = modify_sql_ast(sql, &ModifyAction::Remove(bound("gte", "100")), &ctx)?;
    assert!(applied);
    assert_eq!(
        built,
        "SELECT COUNT(*) FROM KibanaSampleDataEcommerce \
         WHERE KibanaSampleDataEcommerce.taxful_total_price <= 1000 \
         AND KibanaSampleDataEcommerce.customer_gender = 'a'"
    );

    let both = [
        ModifyAction::Remove(bound("gte", "100")),
        ModifyAction::Remove(bound("lte", "1000")),
    ];
    let (built, applied) = modify_parsed_query(sql, &both, &ctx, &ReportedFilters::none())?;
    assert_eq!(applied, vec![true, true]);
    assert_eq!(
        built,
        "SELECT COUNT(*) FROM KibanaSampleDataEcommerce \
         WHERE KibanaSampleDataEcommerce.customer_gender = 'a'"
    );

    // A parenthesized BETWEEN is taken apart the same way
    let nested = sql.replace(
        "KibanaSampleDataEcommerce.taxful_total_price BETWEEN 100 AND 1000",
        "(KibanaSampleDataEcommerce.taxful_total_price BETWEEN 100 AND 1000)",
    );
    assert_ne!(nested, sql);
    let (built, applied) =
        modify_sql_ast(&nested, &ModifyAction::Remove(bound("gte", "100")), &ctx)?;
    assert!(applied);
    assert_eq!(
        built,
        "SELECT COUNT(*) FROM KibanaSampleDataEcommerce \
         WHERE KibanaSampleDataEcommerce.taxful_total_price <= 1000 \
         AND KibanaSampleDataEcommerce.customer_gender = 'a'"
    );

    let negated = sql.replace("BETWEEN", "NOT BETWEEN");
    let (built, applied) =
        modify_sql_ast(&negated, &ModifyAction::Remove(bound("gte", "100")), &ctx)?;
    assert!(!applied);
    assert_eq!(built, negated);

    Ok(())
}

/// Two bounds on one time member are reported as a single range by the
/// planner; adding them together is still an addition that applied, with an
/// exclusive bound moved past the instant it excludes.
#[tokio::test]
async fn test_add_two_bounds_reported_as_one_range() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let sql = "SELECT order_date FROM KibanaSampleDataEcommerce GROUP BY 1";
    let bound = |operator: &str, value: &str| V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
        operator: Some(operator.to_string()),
        values: Some(vec![value.to_string()]),
        ..Default::default()
    };
    let range_of = |filters: &[V1LoadRequestQueryFilterItem]| {
        filters
            .iter()
            .find(|filter| filter.operator.as_deref() == Some("inDateRange"))
            .and_then(|filter| filter.values.as_ref())
            .expect("the bounds are reported as one range")
            .iter()
            .map(|value| normalize_filter_value(value))
            .collect::<Vec<_>>()
    };

    let inclusive = [
        bound("afterOrOnDate", "2024-01-01"),
        bound("beforeOrOnDate", "2024-12-31"),
    ];
    let result = add_sql_filters(sql, &inclusive, meta.clone(), session.clone()).await?;
    assert_eq!(range_of(&result.filters), vec!["2024-01-01", "2024-12-31"]);

    let exclusive = [
        bound("afterDate", "2024-01-01"),
        bound("beforeDate", "2024-12-31"),
    ];
    let result = add_sql_filters(sql, &exclusive, meta.clone(), session.clone()).await?;
    assert_eq!(
        range_of(&result.filters),
        vec!["2024-01-01T00:00:00.001", "2024-12-30"]
    );

    Ok(())
}

/// A typed value is a filter value like any other: what the plan reports on
/// `'2024-01-01'::date` or `CAST(... AS ...)` can be deleted, and replaced.
#[tokio::test]
async fn test_cast_literals_are_filter_values() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    for sql in [
        "SELECT COUNT(*) FROM KibanaSampleDataEcommerce WHERE order_date > '2024-01-01'::date",
        "SELECT COUNT(*) FROM KibanaSampleDataEcommerce \
         WHERE order_date >= CAST('2024-01-01' AS TIMESTAMP)",
        "SELECT COUNT(*) FROM KibanaSampleDataEcommerce WHERE customer_gender = 'male'::text",
    ] {
        let reported = get_sql_filters(sql, meta.clone(), session.clone()).await?;
        assert_eq!(reported.len(), 1, "{sql} reports {reported:?}");

        let deleted = delete_sql_filters(sql, &reported, meta.clone(), session.clone()).await?;
        assert!(
            deleted.filters.is_empty(),
            "{} left {:?} in {}",
            sql,
            deleted.filters,
            deleted.sql
        );
    }

    Ok(())
}

/// A replacement bound the engine folds into a range with a bound already
/// there is applied, as the same filter added with `add` is.
#[tokio::test]
async fn test_replace_with_a_bound_folded_into_a_range() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let sql = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce \
               WHERE order_date >= '2024-01-01' AND customer_gender = 'male'";
    let old = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["male".to_string()]),
        ..Default::default()
    };
    let new = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
        operator: Some("beforeOrOnDate".to_string()),
        values: Some(vec!["2024-12-31".to_string()]),
        ..Default::default()
    };

    let result = replace_sql_filters(sql, &[old], &[new], meta, session).await?;
    let range = result
        .filters
        .iter()
        .find(|filter| filter.operator.as_deref() == Some("inDateRange"))
        .and_then(|filter| filter.values.as_ref())
        .expect("the two bounds are reported as one range")
        .iter()
        .map(|value| normalize_filter_value(value))
        .collect::<Vec<_>>();
    assert_eq!(range, vec!["2024-01-01", "2024-12-31"]);

    Ok(())
}

/// An `E'...'` literal escapes with backslashes; it keys as the value it
/// stands for, so it can be deleted and replaced like any other. A second
/// predicate on the column keeps it from being found by its column alone.
#[tokio::test]
async fn test_escaped_string_literal() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let sql = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce \
               WHERE customer_gender = E'O\\'Brien\\\\' AND customer_gender <> 'z'";
    let filter = |member: &str, value: &str| V1LoadRequestQueryFilterItem {
        member: Some(format!("KibanaSampleDataEcommerce.{member}")),
        operator: Some("equals".to_string()),
        values: Some(vec![value.to_string()]),
        ..Default::default()
    };
    let escaped = filter("customer_gender", "O'Brien\\");

    let deleted = delete_sql_filters(
        sql,
        slice::from_ref(&escaped),
        meta.clone(),
        session.clone(),
    )
    .await?;
    assert_eq!(
        deleted.sql,
        "SELECT COUNT(*) FROM KibanaSampleDataEcommerce WHERE customer_gender <> 'z'"
    );

    let replaced = replace_sql_filters(
        sql,
        &[escaped],
        &[filter("customer_gender", "female")],
        meta,
        session,
    )
    .await?;
    assert!(
        replaced.sql.contains("'female'") && !replaced.sql.contains("Brien"),
        "{}",
        replaced.sql
    );

    Ok(())
}

/// A date value is reduced in a filter's key only on a time member, as
/// reporting reduces it, so two values that differ by a time of day stay two
/// filters on any other member.
#[test]
fn test_filter_key_reduces_dates_on_time_members_only() {
    let meta = get_test_tenant_ctx();
    let filter = |member: &str, value: &str| V1LoadRequestQueryFilterItem {
        member: Some(format!("KibanaSampleDataEcommerce.{member}")),
        operator: Some("equals".to_string()),
        values: Some(vec![value.to_string()]),
        ..Default::default()
    };
    let date = "2024-01-01";
    let instant = "2024-01-01T00:00:00.000Z";

    assert_eq!(
        filter_key(&filter("order_date", date), &meta),
        filter_key(&filter("order_date", instant), &meta)
    );
    assert_ne!(
        filter_key(&filter("customer_gender", date), &meta),
        filter_key(&filter("customer_gender", instant), &meta)
    );
    assert_eq!(
        dedupe_filters(
            &[
                filter("customer_gender", date),
                filter("customer_gender", instant)
            ],
            &meta
        )
        .len(),
        2
    );
}

/// A group mixing a time member with another reduces no date: the other
/// member's value is a string, which a time of day makes a different one.
#[test]
fn test_mixed_group_matches_values_exactly() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    let leaf = |member: &str, operator: &str, value: &str| {
        serde_json::json!({
            "member": format!("KibanaSampleDataEcommerce.{member}"),
            "operator": operator,
            "values": [value],
        })
    };
    let group = |gender: &str| V1LoadRequestQueryFilterItem {
        or: Some(vec![
            leaf("order_date", "afterDate", "2024-01-01"),
            leaf("customer_gender", "equals", gender),
        ]),
        ..Default::default()
    };
    let sql = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce \
               WHERE (KibanaSampleDataEcommerce.order_date > '2024-01-01' \
               OR KibanaSampleDataEcommerce.customer_gender = '2024-01-01')";

    let (built, applied) = modify_sql_ast(
        sql,
        &ModifyAction::Remove(group("2024-01-01T00:00:00.000Z")),
        &ctx,
    )?;
    assert!(!applied);
    assert_eq!(built, sql);

    let (built, applied) = modify_sql_ast(sql, &ModifyAction::Remove(group("2024-01-01")), &ctx)?;
    assert!(applied);
    assert_eq!(built, "SELECT COUNT(*) FROM KibanaSampleDataEcommerce");

    Ok(())
}

/// A bare date is the start of its day except as a range's upper bound, so an
/// end-of-day comparison, a range's end-of-day lower bound and a midnight upper
/// bound are each read back and written back as they are.
#[tokio::test]
async fn test_end_of_day_is_the_date_only_as_a_range_upper_bound() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let from = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce WHERE ";

    for (condition, kept) in [
        (
            "order_date <= '2024-01-01T23:59:59.999'",
            "<= '2024-01-01T23:59:59.999",
        ),
        (
            "order_date >= '2024-01-01T23:59:59.999' AND order_date <= '2024-02-01'",
            ">= '2024-01-01T23:59:59.999",
        ),
        // A midnight upper bound is not the whole of the day
        (
            "order_date >= '2024-01-01' AND order_date <= '2024-02-01'",
            "<= '2024-02-01T00:00:00",
        ),
    ] {
        let sql = format!("{from}{condition}");
        let reported = get_sql_filters(&sql, meta.clone(), session.clone()).await?;
        let set = set_sql_filters(&sql, &reported, meta.clone(), session.clone()).await?;
        assert!(
            set.sql.contains(kept),
            "{} was written back as {}",
            condition,
            set.sql
        );
    }

    // `beforeOrOnDate` of the date alone is midnight, not the end of the day
    let end_of_day = format!("{from}order_date <= '2024-01-01T23:59:59.999'");
    let midnight = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
        operator: Some("beforeOrOnDate".to_string()),
        values: Some(vec!["2024-01-01".to_string()]),
        ..Default::default()
    };
    let deleted = delete_sql_filters(
        &end_of_day,
        slice::from_ref(&midnight),
        meta.clone(),
        session.clone(),
    )
    .await?;
    assert!(deleted.sql.contains("23:59:59.999"), "{}", deleted.sql);

    Ok(())
}

/// A replacement the clause already holds is not written a second time, and a
/// filter replaced by itself stays where it was.
#[test]
fn test_replace_with_a_filter_already_held() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    let leaf = |member: &str, value: &str| V1LoadRequestQueryFilterItem {
        member: Some(format!("KibanaSampleDataEcommerce.{member}")),
        operator: Some("equals".to_string()),
        values: Some(vec![value.to_string()]),
        ..Default::default()
    };
    let sql = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce \
               WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'x' \
               AND KibanaSampleDataEcommerce.\"notes\" = 'y'";

    let replace = |old, new| ModifyAction::Replace { old, new };
    // A group holding the old filter is not held by that filter, which goes
    let group = and_group(vec![
        serde_json::to_value(leaf("customer_gender", "x")).unwrap(),
        serde_json::to_value(leaf("notes", "y")).unwrap(),
    ]);
    let (built, applied) =
        modify_sql_ast(sql, &replace(leaf("customer_gender", "x"), group), &ctx)?;
    assert!(applied);
    assert!(built.contains("\"customer_gender\" = 'x'"), "{}", built);

    let (built, applied) = modify_sql_ast(
        sql,
        &replace(leaf("customer_gender", "x"), leaf("notes", "y")),
        &ctx,
    )?;
    assert!(applied);
    assert_eq!(
        built,
        "SELECT COUNT(*) FROM KibanaSampleDataEcommerce \
         WHERE KibanaSampleDataEcommerce.\"notes\" = 'y'"
    );

    let (built, applied) = modify_sql_ast(
        sql,
        &replace(leaf("customer_gender", "x"), leaf("customer_gender", "x")),
        &ctx,
    )?;
    assert!(applied);
    assert_eq!(built, sql);

    Ok(())
}

/// A cube that appears more than once in the outermost FROM, as in a
/// self-join, leaves no side to filter, so the request is refused.
#[test]
fn test_self_join_member_is_ambiguous() {
    let ctx = get_test_tenant_ctx();
    let filter = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["x".to_string()]),
        ..Default::default()
    };
    let sql = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce k1 \
               JOIN KibanaSampleDataEcommerce k2 ON k1.__cubeJoinField = k2.__cubeJoinField";
    let err = modify_sql_ast(sql, &ModifyAction::Add(filter), &ctx).unwrap_err();
    assert!(err.to_string().contains("ambiguous"), "{}", err);
}

/// A filter on a member more than one relation exposes has no single place to
/// be removed from, so it is not found there: it stays, and so does the rest.
#[tokio::test]
async fn test_ambiguous_member_removal_is_not_found() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    for sql in [
        "SELECT COUNT(*) FROM KibanaSampleDataEcommerce k1 \
         JOIN KibanaSampleDataEcommerce k2 ON k1.__cubeJoinField = k2.__cubeJoinField \
         WHERE k1.customer_gender = 'a'",
        "SELECT a.customer_gender, a.c FROM (SELECT customer_gender, COUNT(*) c \
         FROM KibanaSampleDataEcommerce WHERE customer_gender = 'f' GROUP BY 1) a \
         CROSS JOIN (SELECT customer_gender, COUNT(*) c FROM KibanaSampleDataEcommerce \
         GROUP BY 1) b",
    ] {
        let reported = get_sql_filters(sql, meta.clone(), session.clone()).await?;
        assert_eq!(reported.len(), 1, "{:?}", reported);
        let cleared = set_sql_filters(sql, &[], meta.clone(), session.clone()).await?;
        assert_eq!(cleared.filters.len(), 1, "{}", cleared.sql);
        let deleted = delete_sql_filters(sql, &reported, meta.clone(), session.clone()).await?;
        assert_eq!(deleted.filters.len(), 1, "{}", deleted.sql);
        let err = add_sql_filters(sql, &reported, meta.clone(), session.clone())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("ambiguous"), "{}", err);
        // A filter named to replace is reported ambiguous rather than absent,
        // replaced one for one or removed to make way for others
        for new in [&reported[..], &[]] {
            let err = replace_sql_filters(sql, &reported, new, meta.clone(), session.clone())
                .await
                .unwrap_err();
            assert!(err.to_string().contains("ambiguous"), "{}", err);
        }
    }

    Ok(())
}

/// A single-item `IN` is the same filter as an equality, so it is removed by
/// one even when another reported filter shares its member.
#[tokio::test]
async fn test_single_item_in_is_an_equality() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    for (sql, left) in [
        (
            "SELECT COUNT(*) FROM KibanaSampleDataEcommerce \
             WHERE customer_gender IN ('female') AND customer_gender <> 'other'",
            "SELECT COUNT(*) FROM KibanaSampleDataEcommerce",
        ),
        (
            "SELECT COUNT(*) FROM KibanaSampleDataEcommerce \
             WHERE customer_gender NOT IN ('female') AND customer_gender <> 'other'",
            "SELECT COUNT(*) FROM KibanaSampleDataEcommerce",
        ),
    ] {
        let cleared = set_sql_filters(sql, &[], meta.clone(), session.clone()).await?;
        assert_eq!(cleared.sql, left);
        assert!(cleared.filters.is_empty(), "{:?}", cleared.filters);
    }

    Ok(())
}

/// Removal by column takes a lone predicate, or a time member's bounds; with
/// several predicates on another member, one may not be a Cube filter, so
/// nothing is removed rather than something the plan never reported.
#[test]
fn test_column_removal_needs_one_predicate_or_date_bounds() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    let filter = |member: &str, operator: &str, values: &[&str]| V1LoadRequestQueryFilterItem {
        member: Some(format!("KibanaSampleDataEcommerce.{member}")),
        operator: Some(operator.to_string()),
        values: Some(values.iter().map(|v| v.to_string()).collect()),
        ..Default::default()
    };
    let remove = |sql: &str, reported: V1LoadRequestQueryFilterItem| {
        modify_parsed_query(
            sql,
            &[ModifyAction::Remove(reported.clone())],
            &ctx,
            &ReportedFilters::of(slice::from_ref(&reported), &ctx),
        )
    };
    let from = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce";

    let (built, applied) = remove(
        &format!("{from} WHERE 'female' = customer_gender"),
        filter("customer_gender", "equals", &["female"]),
    )?;
    assert_eq!((built.as_str(), applied[0]), (from, true));

    // Beside it may sit a predicate that is not a Cube filter
    let sql = format!("{from} WHERE 'female' = customer_gender AND customer_gender LIKE '%a%'");
    let (built, applied) = remove(&sql, filter("customer_gender", "equals", &["female"]))?;
    assert_eq!((built, applied[0]), (sql, false));

    // The plan merges a time member's bounds into the one reported range
    let (built, applied) = remove(
        &format!("{from} WHERE order_date >= '2024-01-01' AND '2024-02-01' >= order_date"),
        filter("order_date", "inDateRange", &["2024-01-01", "2024-02-01"]),
    )?;
    assert_eq!((built.as_str(), applied[0]), (from, true));

    Ok(())
}

/// Only `MEASURE` or a measure's own aggregate stands for it: `AVG` over a sum
/// measure is no filter on it, so it is neither removed nor taken as present.
#[tokio::test]
async fn test_other_aggregate_is_not_the_measure() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let gt = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.sumPrice".to_string()),
        operator: Some("gt".to_string()),
        values: Some(vec!["100".to_string()]),
        ..Default::default()
    };
    let head = "SELECT customer_gender, MEASURE(sumPrice) FROM KibanaSampleDataEcommerce \
                GROUP BY 1 HAVING ";

    let avg = format!("{head}AVG(KibanaSampleDataEcommerce.sumPrice) > 100");
    let deleted =
        delete_sql_filters(&avg, slice::from_ref(&gt), meta.clone(), session.clone()).await?;
    assert_eq!(deleted.sql, avg);

    for spelling in ["SUM", "MEASURE"] {
        let sql = format!("{head}{spelling}(KibanaSampleDataEcommerce.sumPrice) > 100");
        let deleted =
            delete_sql_filters(&sql, slice::from_ref(&gt), meta.clone(), session.clone()).await?;
        assert!(!deleted.sql.contains("HAVING"), "{}", deleted.sql);
    }

    Ok(())
}

/// A filter reported from a CTE alone does not make an outer `LIKE` on its
/// column removable: that pattern is no Cube filter, so it stays.
#[tokio::test]
async fn test_cte_filter_does_not_remove_an_outer_like() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let sql = "WITH c AS (SELECT * FROM KibanaSampleDataEcommerce WHERE notes = 'x') \
               SELECT COUNT(*) FROM c WHERE c.notes LIKE 'a%b'";
    let cleared = set_sql_filters(sql, &[], meta, session).await?;
    assert!(
        cleared.sql.ends_with("WHERE c.notes LIKE 'a%b'"),
        "{}",
        cleared.sql
    );

    Ok(())
}

/// A value is written so that parsing the rewritten SQL gives it back whole:
/// a quote after a backslash or a doubled quote cannot end the literal early.
#[tokio::test]
async fn test_values_survive_the_rewritten_sql() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let sql = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce";
    for (operator, value) in [
        ("equals", r"x\' OR customer_gender = 'y"),
        ("equals", "a''b"),
        ("equals", r"trailing\"),
        ("contains", r"x\' OR customer_gender = 'y"),
    ] {
        let filter = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some(operator.to_string()),
            values: Some(vec![value.to_string()]),
            ..Default::default()
        };
        let added =
            add_sql_filters(sql, slice::from_ref(&filter), meta.clone(), session.clone()).await?;
        let reread = get_sql_filters(&added.sql, meta.clone(), session.clone()).await?;
        assert_eq!(
            reread
                .iter()
                .map(|f| filter_key(f, &meta))
                .collect::<Vec<_>>(),
            vec![filter_key(&filter, &meta)],
            "{} {:?} was written as {}",
            operator,
            value,
            added.sql
        );
    }

    Ok(())
}

/// A measure's aggregate is its own cube's: another cube's measure of the
/// same name, of another type, does not make `AVG` stand for a sum measure.
#[test]
fn test_measure_aggregate_is_its_own_cubes() -> DFResult<()> {
    use crate::compile::test::get_test_tenant_ctx_with_meta;
    use cubeclient::models::{V1CubeMeta, V1CubeMetaType};

    let cube = |name: &str, agg: &str| V1CubeMeta {
        name: name.to_string(),
        description: None,
        title: None,
        r#type: V1CubeMetaType::Cube,
        dimensions: vec![V1CubeMetaDimension {
            name: format!("{name}.status"),
            r#type: "string".to_string(),
            ..Default::default()
        }],
        measures: vec![V1CubeMetaMeasure {
            name: format!("{name}.amount"),
            r#type: "number".to_string(),
            agg_type: Some(agg.to_string()),
            ..Default::default()
        }],
        segments: vec![],
        joins: None,
        folders: None,
        nested_folders: None,
        hierarchies: None,
        meta: None,
    };
    let ctx = get_test_tenant_ctx_with_meta(vec![cube("A", "sum"), cube("B", "avg")]);
    let gt = V1LoadRequestQueryFilterItem {
        member: Some("A.amount".to_string()),
        operator: Some("gt".to_string()),
        values: Some(vec!["100".to_string()]),
        ..Default::default()
    };
    let sql = "SELECT status, MEASURE(amount) FROM A GROUP BY 1 HAVING AVG(A.amount) > 100";
    let (built, applied) = modify_sql_ast(sql, &ModifyAction::Remove(gt), &ctx)?;
    assert!(!applied);
    assert_eq!(built, sql);

    Ok(())
}

/// A cube is recognised in the FROM however the resolver would recognise it:
/// schema-qualified, quoted or in another case, and still keys as its measure.
#[tokio::test]
async fn test_measure_matches_however_its_cube_is_named() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let gt = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.sumPrice".to_string()),
        operator: Some("gt".to_string()),
        values: Some(vec!["100".to_string()]),
        ..Default::default()
    };
    for relation in [
        "KibanaSampleDataEcommerce",
        "public.KibanaSampleDataEcommerce",
        "\"public\".\"KibanaSampleDataEcommerce\"",
        "kibanasampledataecommerce",
    ] {
        let sql = format!(
            "SELECT customer_gender, MEASURE(sumPrice) FROM {relation} \
             GROUP BY 1 HAVING MEASURE(sumPrice) > 100"
        );
        let deleted =
            delete_sql_filters(&sql, slice::from_ref(&gt), meta.clone(), session.clone()).await?;
        assert!(
            !deleted.sql.contains("HAVING"),
            "{}: {}",
            relation,
            deleted.sql
        );
    }

    Ok(())
}

/// The query's own literals are rendered so that they read back the same, as
/// the ones this API writes are: an addition keeps an earlier `'a\'''` whole.
#[tokio::test]
async fn test_query_literals_survive_the_rewrite() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let sql = r"SELECT COUNT(*) FROM KibanaSampleDataEcommerce WHERE notes = 'a\'''";
    let before = get_sql_filters(sql, meta.clone(), session.clone()).await?;
    let gender = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["x".to_string()]),
        ..Default::default()
    };
    let added = add_sql_filters(sql, slice::from_ref(&gender), meta.clone(), session).await?;
    let mut expected = before
        .iter()
        .map(|f| filter_key(f, &meta))
        .collect::<Vec<_>>();
    expected.push(filter_key(&gender, &meta));
    let mut after = added
        .filters
        .iter()
        .map(|f| filter_key(f, &meta))
        .collect::<Vec<_>>();
    expected.sort();
    after.sort();
    assert_eq!(after, expected, "{}", added.sql);

    Ok(())
}

/// A query's own literal holding `''` is matched by the value it stands for:
/// the reported filter is removed and replaced, and a different one is not.
#[tokio::test]
async fn test_query_literals_are_matched_by_their_value() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let sql = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce \
               WHERE customer_gender = 'a''''b' AND customer_gender <> 'zz'";
    let equals = |value: &str| V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec![value.to_string()]),
        ..Default::default()
    };
    let kept = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce \
                WHERE customer_gender <> 'zz'";

    let deleted = delete_sql_filters(sql, &[equals("a''b")], meta.clone(), session.clone()).await?;
    assert_eq!(deleted.sql, kept);

    let missed = delete_sql_filters(sql, &[equals("a'b")], meta.clone(), session.clone()).await?;
    assert_eq!(
        missed.sql,
        r"SELECT COUNT(*) FROM KibanaSampleDataEcommerce WHERE customer_gender = E'a\'\'b' AND customer_gender <> 'zz'"
    );

    let replaced =
        replace_sql_filters(sql, &[equals("a''b")], &[equals("c")], meta, session).await?;
    assert!(replaced.sql.contains("= 'c'"), "{}", replaced.sql);

    Ok(())
}

/// A comparison on a string member is reported like any other filter, so it
/// is written back, removed and added as a string rather than refused.
#[tokio::test]
async fn test_string_comparison_is_rewritten() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let sql = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce \
               WHERE notes > 'a' AND customer_gender = 'f'";
    let reported = get_sql_filters(sql, meta.clone(), session.clone()).await?;
    assert_eq!(reported.len(), 2, "{:?}", reported);

    let cleared = set_sql_filters(sql, &[], meta.clone(), session.clone()).await?;
    assert_eq!(
        cleared.sql,
        "SELECT COUNT(*) FROM KibanaSampleDataEcommerce"
    );

    let written = set_sql_filters(sql, &reported, meta.clone(), session.clone()).await?;
    let keys = |filters: &[V1LoadRequestQueryFilterItem]| {
        let mut keys = filters
            .iter()
            .map(|f| filter_key(f, &meta))
            .collect::<Vec<_>>();
        keys.sort();
        keys
    };
    assert_eq!(keys(&written.filters), keys(&reported), "{}", written.sql);

    Ok(())
}

/// A comparison on a time dimension is refused up front, pointing at the date
/// operators, since the plan reads it as one of those and never as `gt`.
#[test]
fn test_comparison_on_a_time_dimension_is_refused() {
    let ctx = get_test_tenant_ctx();
    let gt = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
        operator: Some("gt".to_string()),
        values: Some(vec!["2024-01-01".to_string()]),
        ..Default::default()
    };
    let sql = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce";
    let err = modify_sql_ast(sql, &ModifyAction::Add(gt), &ctx).unwrap_err();
    assert!(err.to_string().contains("afterDate"), "{}", err);
}

/// A number is matched in the form the plan reports it, so `1.0` or `1e5`
/// written by the caller is verified as the `1` or `100000` read back.
#[tokio::test]
async fn test_numbers_are_matched_in_canonical_form() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let sql = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce";
    for value in ["1.0", "1e5", "05", ".5", "+5", "-0", "5."] {
        for operator in ["gt", "equals"] {
            let filter = V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                operator: Some(operator.to_string()),
                values: Some(vec![value.to_string()]),
                ..Default::default()
            };
            add_sql_filters(sql, slice::from_ref(&filter), meta.clone(), session.clone())
                .await
                .map_err(|e| CubeError::internal(format!("{operator} {value}: {e}")))?;
        }
    }

    Ok(())
}

/// Integers are keyed exactly, as the plan reads them: two above 2^53 that
/// one `f64` holds are still two filters, read and replaced apart.
#[tokio::test]
async fn test_large_integers_stay_apart() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let sql = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce \
               WHERE taxful_total_price = 9007199254740993 \
               AND taxful_total_price = 9007199254740992";
    let reported = get_sql_filters(sql, meta.clone(), session.clone()).await?;
    assert_eq!(reported.len(), 2, "{:?}", reported);

    let one = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["1".to_string()]),
        ..Default::default()
    };
    let replaced = replace_sql_filters(
        sql,
        &reported,
        slice::from_ref(&one),
        meta.clone(),
        session.clone(),
    )
    .await?;
    assert!(
        !replaced.sql.contains("900719925474099"),
        "{}",
        replaced.sql
    );

    // Past `i64` the plan reads an integer as a float, and so is it keyed
    for value in ["9223372036854775808", "-9223372036854775808"] {
        let beyond = V1LoadRequestQueryFilterItem {
            values: Some(vec![value.to_string()]),
            ..one.clone()
        };
        add_sql_filters(
            "SELECT COUNT(*) FROM KibanaSampleDataEcommerce",
            slice::from_ref(&beyond),
            meta.clone(),
            session.clone(),
        )
        .await?;
    }

    Ok(())
}

/// An operator the plan reports under another one on its member is refused
/// when written, naming the one to use, rather than failing verification.
#[test]
fn test_operators_the_plan_reports_otherwise_are_refused() {
    let ctx = get_test_tenant_ctx();
    let sql = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce";
    for (member, operator, values, suggested) in [
        ("order_date", "gt", vec!["2024-01-01"], "afterDate"),
        ("taxful_total_price", "afterDate", vec!["5"], "gt"),
        ("taxful_total_price", "inDateRange", vec!["1", "5"], "gte"),
    ] {
        let filter = V1LoadRequestQueryFilterItem {
            member: Some(format!("KibanaSampleDataEcommerce.{member}")),
            operator: Some(operator.to_string()),
            values: Some(values.iter().map(|v| v.to_string()).collect()),
            ..Default::default()
        };
        let err = modify_sql_ast(sql, &ModifyAction::Add(filter), &ctx).unwrap_err();
        assert!(err.to_string().contains(suggested), "{}: {}", operator, err);
    }
}

/// The refusal is for writing only: a comparison the query itself spells on
/// a time dimension is still found and removed.
#[test]
fn test_spelled_time_comparison_can_be_removed() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    let gt = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
        operator: Some("gt".to_string()),
        values: Some(vec!["2024-01-01".to_string()]),
        ..Default::default()
    };
    let sql = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce WHERE order_date > '2024-01-01'";
    let (built, applied) = modify_sql_ast(sql, &ModifyAction::Remove(gt), &ctx)?;
    assert!(applied);
    assert_eq!(built, "SELECT COUNT(*) FROM KibanaSampleDataEcommerce");

    Ok(())
}

/// A number in another form is the filter the plan reports, so it is removed
/// by its column when the query spells it otherwise.
#[tokio::test]
async fn test_number_in_another_form_is_removed() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let equals = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["1.0".to_string()]),
        ..Default::default()
    };
    let sql = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce \
               WHERE taxful_total_price = 1 AND customer_gender = 'x'";
    let deleted = delete_sql_filters(sql, slice::from_ref(&equals), meta, session).await?;
    assert_eq!(
        deleted.sql,
        "SELECT COUNT(*) FROM KibanaSampleDataEcommerce WHERE customer_gender = 'x'"
    );

    Ok(())
}

/// A list of dates is an `IN`, which the plan reports as `equals` on a time
/// dimension too, so it is written and read back rather than refused.
#[tokio::test]
async fn test_date_lists_keep_equals() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    for list in ["IN", "NOT IN"] {
        let sql = format!(
            "SELECT COUNT(*) FROM KibanaSampleDataEcommerce \
             WHERE order_date {list} ('2024-01-01', '2024-02-01')"
        );
        let reported = get_sql_filters(&sql, meta.clone(), session.clone()).await?;
        assert_eq!(reported.len(), 1, "{}: {:?}", list, reported);
        set_sql_filters(&sql, &reported, meta.clone(), session.clone()).await?;
        add_sql_filters(
            "SELECT COUNT(*) FROM KibanaSampleDataEcommerce",
            &reported,
            meta.clone(),
            session.clone(),
        )
        .await?;
    }

    Ok(())
}

/// A single date is a one-item `IN` on a time dimension, alone, beside another
/// predicate or in a group, and whatever form the plan reports it in, every
/// rewrite of the reported filters goes through.
#[tokio::test]
async fn test_single_date_round_trips() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let from = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce";
    for condition in [
        "order_date IN ('2024-01-01')",
        "order_date NOT IN ('2024-01-01')",
        "order_date IN ('2024-01-01') AND customer_gender = 'a'",
        "(order_date IN ('2024-01-01') OR customer_gender = 'a')",
    ] {
        let sql = format!("{from} WHERE {condition}");
        assert_round_trips(&sql, meta.clone(), session.clone()).await?;
    }

    // The plan reads `=` as a date range, so it is no `equals` already there
    let equals = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["2024-01-01".to_string()]),
        ..Default::default()
    };
    let sql = format!("{from} WHERE order_date = '2024-01-01'");
    let added = add_sql_filters(&sql, &[equals], meta, session).await?;
    assert!(added.sql.contains("IN ('2024-01-01')"), "{}", added.sql);

    Ok(())
}

/// Sets, clears, deletes and replaces the filters the query reports, checking
/// that each leaves the query holding what it should.
async fn assert_round_trips(
    sql: &str,
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> Result<(), CubeError> {
    let reported = get_sql_filters(sql, meta.clone(), session.clone()).await?;
    let set = set_sql_filters(sql, &reported, meta.clone(), session.clone()).await?;
    let cleared = set_sql_filters(sql, &[], meta.clone(), session.clone()).await?;
    assert!(cleared.filters.is_empty(), "{}: {}", sql, cleared.sql);
    // The plan reports a filter written twice once, so the SQL tells them apart
    let added = add_sql_filters(&cleared.sql, &reported, meta.clone(), session.clone()).await?;
    assert_eq!(set.sql, added.sql, "{}", sql);
    let deleted = delete_sql_filters(sql, &reported, meta.clone(), session.clone()).await?;
    assert!(deleted.filters.is_empty(), "{}: {}", sql, deleted.sql);
    for filter in &reported {
        let one = slice::from_ref(filter);
        replace_sql_filters(sql, one, one, meta.clone(), session.clone()).await?;
    }
    Ok(())
}

/// A boolean predicate spelled without a value is the `equals` the plan
/// reports it as, so it is found and removed like one.
#[tokio::test]
async fn test_bare_boolean_predicates_are_matched() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let from = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce";
    for condition in [
        "has_subscription",
        "NOT has_subscription",
        "NOT (has_subscription)",
        "has_subscription IS TRUE",
        "has_subscription IS FALSE",
        "has_subscription IS NOT TRUE",
        "(has_subscription OR customer_gender = 'b')",
        "(NOT has_subscription OR customer_gender = 'b')",
        "(customer_gender = 'b' OR (notes = 'c' OR notes = 'd') AND taxful_total_price = 1)",
        "(customer_gender IN ('b') OR taxful_total_price = 1)",
    ] {
        let sql = format!("{from} WHERE {condition} AND customer_gender = 'a'");
        assert_round_trips(&sql, meta.clone(), session.clone()).await?;
        let reported = get_sql_filters(&sql, meta.clone(), session.clone()).await?;
        let deleted = delete_sql_filters(&sql, &reported, meta.clone(), session.clone()).await?;
        assert_eq!(deleted.sql, from, "{}", condition);
    }

    Ok(())
}

/// A predicate under `NOT` is the filter the plan reports with the `NOT`
/// folded in, so it is found and removed like one.
#[tokio::test]
async fn test_negated_predicates_are_matched() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let from = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce";
    for condition in [
        "NOT customer_gender = 'a'",
        "NOT (customer_gender IN ('a'))",
        "NOT (customer_gender IN ('a', 'b'))",
        "NOT (customer_gender IS NULL)",
        "NOT (customer_gender LIKE '%a%')",
        "NOT (NOT has_subscription)",
        "NOT (NOT (has_subscription))",
        "NOT (NOT (customer_gender = 'a'))",
        "NOT (has_subscription = true)",
        "NOT (has_subscription IS TRUE)",
    ] {
        let sql = format!("{from} WHERE {condition} AND notes = 'z'");
        assert_round_trips(&sql, meta.clone(), session.clone()).await?;
        let reported = get_sql_filters(&sql, meta.clone(), session.clone()).await?;
        let deleted = delete_sql_filters(&sql, &reported, meta.clone(), session.clone()).await?;
        assert_eq!(deleted.sql, from, "{}", condition);
    }

    Ok(())
}

/// `COUNT(*)` is the plan's `count` measure on a single cube, so a filter on
/// it is matched like one spelled on the measure.
#[tokio::test]
async fn test_count_rows_matches_the_count_measure() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    for condition in [
        "COUNT(*) > 1",
        "COUNT(1) > 1",
        "COUNT((1)) > 1",
        "COUNT(*) BETWEEN 1 AND 5",
        "(COUNT(*) > 1 OR MAX(maxPrice) < 3)",
    ] {
        let sql = format!(
            "SELECT customer_gender, COUNT(*) FROM KibanaSampleDataEcommerce \
             GROUP BY 1 HAVING {condition}"
        );
        assert_round_trips(&sql, meta.clone(), session.clone()).await?;
    }

    Ok(())
}

/// A reported filter this API cannot write is not found by a replace, as by a
/// removal, rather than refused for its values.
#[tokio::test]
async fn test_unwritable_filter_to_replace_is_not_found() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let sql = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce WHERE taxful_total_price = 'abc'";
    let reported = get_sql_filters(sql, meta.clone(), session.clone()).await?;
    assert_eq!(reported.len(), 1, "{:?}", reported);
    let err = replace_sql_filters(sql, &reported, &reported, meta, session)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not found"), "{}", err);

    Ok(())
}

/// The planner reads `COUNT(*)` as a cube's `count` measure of a `count` or
/// `number` type, so a `number` one is found under it, and a `sum` one is not.
#[test]
fn test_count_rows_matches_a_count_of_a_countable_type() -> DFResult<()> {
    use crate::compile::test::get_test_tenant_ctx_with_meta;
    use cubeclient::models::{V1CubeMeta, V1CubeMetaType};

    let ctx = get_test_tenant_ctx_with_meta(vec![V1CubeMeta {
        name: "Orders".to_string(),
        description: None,
        title: None,
        r#type: V1CubeMetaType::Cube,
        dimensions: vec![V1CubeMetaDimension {
            name: "Orders.status".to_string(),
            r#type: "string".to_string(),
            ..Default::default()
        }],
        measures: vec![V1CubeMetaMeasure {
            name: "Orders.count".to_string(),
            r#type: "number".to_string(),
            agg_type: Some("number".to_string()),
            ..Default::default()
        }],
        segments: vec![],
        joins: None,
        folders: None,
        nested_folders: None,
        hierarchies: None,
        meta: None,
    }]);
    let filter = V1LoadRequestQueryFilterItem {
        member: Some("Orders.count".to_string()),
        operator: Some("gt".to_string()),
        values: Some(vec!["1".to_string()]),
        ..Default::default()
    };
    let sql = "SELECT status, COUNT(*) FROM Orders GROUP BY 1 HAVING COUNT(*) > 1";
    let (built, applied) = modify_sql_ast(sql, &ModifyAction::Remove(filter.clone()), &ctx)?;
    assert!(applied, "{}", built);
    assert_eq!(built, "SELECT status, COUNT(*) FROM Orders GROUP BY 1");

    let mut summed = ctx.cubes.clone();
    summed[0].measures[0].agg_type = Some("sum".to_string());
    let ctx = get_test_tenant_ctx_with_meta(summed);
    let (built, applied) = modify_sql_ast(sql, &ModifyAction::Remove(filter), &ctx)?;
    assert!(!applied, "{}", built);

    Ok(())
}

/// A member is a cube and a name: one with further parts would be read as its
/// first two, and so act on a member the caller did not name.
#[test]
fn test_member_with_extra_parts_is_refused() {
    let ctx = get_test_tenant_ctx();
    let filter = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender.bogus".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["x".to_string()]),
        ..Default::default()
    };
    let sql = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce WHERE customer_gender = 'x'";
    let err = modify_sql_ast(sql, &ModifyAction::Remove(filter), &ctx).unwrap_err();
    assert!(err.to_string().contains("Invalid member format"), "{}", err);
}

/// A predicate comparing the column to another column is no Cube filter, so
/// removing a reported filter by its column leaves it.
#[tokio::test]
async fn test_column_comparisons_are_not_removed_by_column() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    for condition in [
        "IN (c.notes)",
        "BETWEEN c.notes AND 'z'",
        "IN (RANDOM()::text)",
        "IN (CASE WHEN RANDOM() > 0.5 THEN 'x' END)",
        "IN (CURRENT_USER)",
        "= UPPER(NULL)",
        "= COALESCE(NULL, NULL)",
        "IN (NULL)",
    ] {
        let sql = format!(
            "WITH c AS (SELECT * FROM KibanaSampleDataEcommerce WHERE customer_gender = 'a') \
             SELECT COUNT(*) FROM c WHERE c.customer_gender {condition}"
        );
        let reported = get_sql_filters(&sql, meta.clone(), session.clone()).await?;
        assert_eq!(reported.len(), 1, "{}: {:?}", condition, reported);
        let deleted = delete_sql_filters(&sql, &reported, meta.clone(), session.clone()).await?;
        assert!(
            deleted.sql.contains("WHERE c.customer_gender"),
            "{}",
            deleted.sql
        );
    }

    Ok(())
}

/// The values of a request are bounded with its filters, as one `IN` list
/// could otherwise carry any number of them.
#[test]
fn test_filter_values_are_bounded() {
    let filter = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some((0..=MAX_FILTER_VALUES).map(|i| i.to_string()).collect()),
        ..Default::default()
    };
    let err = assert_filter_count([&filter]).unwrap_err();
    assert!(err.to_string().contains("filter values"), "{}", err);

    let nested = and_group(vec![serde_json::to_value(&filter).unwrap()]);
    assert!(assert_filter_count([&nested]).is_err());
}

/// A query holding `N'...'`, which sqlparser writes back unescaped and the
/// planner does not take, is refused by every operation, a rewrite included.
#[tokio::test]
async fn test_national_literals_are_refused() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let filter = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["x".to_string()]),
        ..Default::default()
    };
    let filters = slice::from_ref(&filter);
    let sql = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce WHERE notes = N'x'' OR ''1''=''1'";
    for result in [
        add_sql_filters(sql, filters, meta.clone(), session.clone()).await,
        set_sql_filters(sql, filters, meta.clone(), session.clone()).await,
        delete_sql_filters(sql, filters, meta.clone(), session.clone()).await,
        replace_sql_filters(sql, filters, filters, meta.clone(), session.clone()).await,
    ] {
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("NationalStringLiteral") || err.to_string().contains("N'...'"),
            "{}",
            err
        );
    }

    Ok(())
}

/// A value the planner folds is a filter value however it is spelled, in an
/// `IN` list or a comparison, so the reported filter is removed by its column.
#[tokio::test]
async fn test_computed_values_are_removed_by_column() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let from = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce";
    for condition in [
        "customer_gender IN (('a'), 'b')",
        "customer_gender IN ('a' || 'b')",
        "customer_gender IN (UPPER('a'))",
        "customer_gender IN (pg_catalog.upper('a'))",
        "customer_gender IN (\"upper\"('a'))",
        "customer_gender IN (\"UPPER\"('a'))",
        "customer_gender = UPPER('a')",
        "taxful_total_price = 1 + 1",
        "customer_gender IN (TRIM(' a '), 'b')",
        "customer_gender IN (SUBSTRING('abc' FROM 1 FOR 1))",
        "customer_gender IN (COALESCE('a'))",
        "taxful_total_price IN (1 + 1)",
    ] {
        let sql = format!("{from} WHERE {condition}");
        let reported = get_sql_filters(&sql, meta.clone(), session.clone()).await?;
        assert!(!reported.is_empty(), "{}", condition);
        let deleted = delete_sql_filters(&sql, &reported, meta.clone(), session.clone()).await?;
        assert_eq!(deleted.sql, from, "{}", condition);
    }

    // The engine lowercases a quoted function name, so `"NOW"` is `now`
    let sql = format!("{from} WHERE order_date > \"NOW\"()");
    let cleared = set_sql_filters(&sql, &[], meta, session).await?;
    assert_eq!(cleared.sql, from);

    Ok(())
}

/// A countDistinct measure is written `COUNT(DISTINCT x)`, and a query may
/// spell it `MEASURE(x)`, its argument parenthesized or not: each is the filter.
#[test]
fn test_count_distinct_measure_matches_either_spelling() -> DFResult<()> {
    let ctx = get_test_tenant_ctx();
    let filter = V1LoadRequestQueryFilterItem {
        member: Some("Logs.agentCount".to_string()),
        operator: Some("gt".to_string()),
        values: Some(vec!["10".to_string()]),
        ..Default::default()
    };
    for spelling in [
        "MEASURE(agentCount)",
        "COUNT(DISTINCT agentCount)",
        "COUNT(DISTINCT(agentCount))",
        "MEASURE((agentCount))",
    ] {
        let sql = format!(
            "SELECT content, MEASURE(agentCount) FROM Logs GROUP BY 1 HAVING {spelling} > 10"
        );
        let (built, applied) = modify_sql_ast(&sql, &ModifyAction::Remove(filter.clone()), &ctx)?;
        assert!(applied, "{}: {}", spelling, built);
        assert_eq!(
            built,
            "SELECT content, MEASURE(agentCount) FROM Logs GROUP BY 1"
        );
    }

    Ok(())
}

/// Two date bounds sent as one `and` group are applied as the engine merges
/// them, into one range, as they are when sent apart.
#[tokio::test]
async fn test_bounds_in_an_and_group_are_applied() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let bound = |operator: &str, value: &str| {
        serde_json::json!({
            "member": "KibanaSampleDataEcommerce.order_date",
            "operator": operator,
            "values": [value],
        })
    };
    let group = and_group(vec![
        bound("afterOrOnDate", "2024-01-01"),
        bound("beforeOrOnDate", "2024-12-31"),
    ]);
    let sql = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce";
    let added =
        add_sql_filters(sql, slice::from_ref(&group), meta.clone(), session.clone()).await?;
    assert_eq!(added.filters.len(), 1, "{:?}", added.filters);
    replace_sql_filters(
        &added.sql,
        &added.filters,
        slice::from_ref(&group),
        meta,
        session,
    )
    .await?;

    Ok(())
}

/// A group is the filter a query reports whatever order its items' fields
/// are written in.
#[test]
fn test_reported_group_is_held_whatever_its_field_order() {
    let ctx = get_test_tenant_ctx();
    let group = |gender: serde_json::Value| {
        or_group(vec![
            gender,
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.notes",
                "operator": "equals",
                "values": ["b"],
            }),
        ])
    };
    let reported = group(serde_json::json!({
        "member": "KibanaSampleDataEcommerce.customer_gender",
        "operator": "equals",
        "values": ["a"],
    }));
    let reordered = group(serde_json::json!({
        "operator": "equals",
        "values": ["a"],
        "member": "KibanaSampleDataEcommerce.customer_gender",
    }));
    let reported = ReportedFilters::of(slice::from_ref(&reported), &ctx);
    assert!(reported.holds(&reordered, &ctx));
}

/// A group whose leaves a query parenthesizes one by one is the group this API
/// writes with the parentheses around the whole of it.
#[tokio::test]
async fn test_group_with_parenthesized_leaves_is_matched() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let from = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce";
    for condition in [
        "(customer_gender = 'a') OR (notes = 'x')",
        "((customer_gender = 'a')) OR ((notes = 'x'))",
        "((customer_gender = 'a') OR (notes = 'x')) AND taxful_total_price > 1",
        "(customer_gender = 'a' OR notes = 'x') OR taxful_total_price > 1",
        "((customer_gender = 'a' AND notes = 'x') AND taxful_total_price > 1) OR has_subscription",
    ] {
        let sql = format!("{from} WHERE {condition}");
        assert_round_trips(&sql, meta.clone(), session.clone()).await?;
        let reported = get_sql_filters(&sql, meta.clone(), session.clone()).await?;
        let deleted = delete_sql_filters(&sql, &reported, meta.clone(), session.clone()).await?;
        assert_eq!(deleted.sql, from, "{}", condition);
    }

    Ok(())
}

/// An item holding both an `and` and an `or` group counts the filters of both
/// towards the bound, rather than walking one of them uncounted.
#[test]
fn test_both_groups_of_an_item_are_counted() {
    let leaf = serde_json::json!({
        "member": "KibanaSampleDataEcommerce.customer_gender",
        "operator": "set",
    });
    let item = V1LoadRequestQueryFilterItem {
        and: Some(vec![leaf.clone()]),
        or: Some(vec![leaf; MAX_FILTERS]),
        ..Default::default()
    };
    assert!(assert_filter_count([&item]).is_err());
}

/// Two derived relations exposing the member leave no side to filter, as a
/// cube appearing twice does, so the request is refused.
#[test]
fn test_member_exposed_twice_is_ambiguous() {
    let ctx = get_test_tenant_ctx();
    let filter = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["x".to_string()]),
        ..Default::default()
    };
    for sql in [
        "WITH c AS (SELECT customer_gender FROM KibanaSampleDataEcommerce) \
         SELECT * FROM c c1 JOIN c c2 ON true",
        "SELECT * FROM (SELECT customer_gender FROM KibanaSampleDataEcommerce) a \
         JOIN (SELECT customer_gender FROM KibanaSampleDataEcommerce) b ON true",
    ] {
        let err = modify_sql_ast(sql, &ModifyAction::Add(filter.clone()), &ctx).unwrap_err();
        assert!(err.to_string().contains("ambiguous"), "{}", err);
    }
}

/// A date part the engine does not turn into a range on its own is no filter
/// on its column, so removing a reported range by its column leaves it.
#[tokio::test]
async fn test_unranged_extraction_is_not_removed_by_column() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    for (part, kept) in [
        ("EXTRACT(DOW FROM c.order_date) = 1", "EXTRACT(DOW"),
        ("EXTRACT(MONTH FROM c.order_date) = 3", "EXTRACT(MONTH"),
        ("EXTRACT(DAY FROM c.order_date) = 3", "EXTRACT(DAY"),
        ("DATE_PART('month', c.order_date) = 3", "date_part"),
    ] {
        let sql = format!(
            "WITH c AS (SELECT * FROM KibanaSampleDataEcommerce \
             WHERE order_date >= '2024-01-01' LIMIT 100) \
             SELECT COUNT(*) FROM c WHERE {part}"
        );
        let reported = get_sql_filters(&sql, meta.clone(), session.clone()).await?;
        assert_eq!(reported.len(), 1, "{}: {:?}", part, reported);
        let deleted = delete_sql_filters(&sql, &reported, meta.clone(), session.clone()).await?;
        assert!(
            deleted.sql.to_lowercase().contains(&kept.to_lowercase()),
            "{}",
            deleted.sql
        );
    }

    Ok(())
}

/// Date parts the engine merges into one range with a year are removed with
/// it, however they are spelled.
#[tokio::test]
async fn test_date_parts_merged_into_a_range_are_removed() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let from = "SELECT COUNT(*) FROM KibanaSampleDataEcommerce";
    for condition in [
        "EXTRACT(YEAR FROM order_date) = 2024 AND EXTRACT(QUARTER FROM order_date) = 2",
        "EXTRACT(YEAR FROM order_date) = 2024 AND EXTRACT(WEEK FROM order_date) = 10",
        "DATE_PART('year', order_date) = 2024",
        "EXTRACT(YEAR FROM order_date) = 2024 AND DATE_PART('month', order_date) = 3",
        "EXTRACT(YEAR FROM order_date) = 2024 AND EXTRACT(MONTHS FROM order_date) = 3",
        "EXTRACT(YEAR FROM order_date) = 2024 AND EXTRACT(qtr FROM order_date) = 2",
        "EXTRACT(YEAR FROM order_date) = 2024 AND EXTRACT('month' FROM order_date) = 3",
        "EXTRACT(YEAR FROM order_date) = 2024 AND DATE_PART(E'month', order_date) = 3",
        "EXTRACT(YEARS FROM order_date) = 2024",
        "EXTRACT(YEAR FROM order_date) = 2024 AND EXTRACT(mon FROM order_date) = 3",
        "EXTRACT(YEAR FROM order_date) = 2024 AND DATE_PART('w', order_date) = 10",
        "EXTRACT(yr FROM order_date) = 2024",
    ] {
        let sql = format!("{from} WHERE {condition}");
        let reported = get_sql_filters(&sql, meta.clone(), session.clone()).await?;
        assert_eq!(reported.len(), 1, "{}: {:?}", condition, reported);
        let cleared = set_sql_filters(&sql, &[], meta.clone(), session.clone()).await?;
        assert_eq!(cleared.sql, from, "{}", condition);
        let deleted = delete_sql_filters(&sql, &reported, meta.clone(), session.clone()).await?;
        assert_eq!(deleted.sql, from, "{}", condition);
    }

    Ok(())
}

/// A filter the planner keeps above the Cube query, as over a `LIMIT`
/// subquery, is not reported, and so cannot be verified when it is added.
#[tokio::test]
async fn test_filter_kept_above_the_cube_query_is_not_reported() -> Result<(), CubeError> {
    use crate::compile::{test::get_test_session, DatabaseProtocol};

    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let from = "SELECT * FROM (SELECT customer_gender FROM KibanaSampleDataEcommerce LIMIT 10) t";
    let sql = format!("{from} WHERE t.customer_gender = 'female'");
    let reported = get_sql_filters(&sql, meta.clone(), session.clone()).await?;
    assert!(reported.is_empty(), "{:?}", reported);

    let filter = V1LoadRequestQueryFilterItem {
        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec!["female".to_string()]),
        ..Default::default()
    };
    let err = add_sql_filters(from, slice::from_ref(&filter), meta, session)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("plan reports"), "{}", err);

    Ok(())
}
