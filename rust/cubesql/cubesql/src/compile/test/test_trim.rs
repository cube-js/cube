use crate::compile::{
    test::{LogicalPlanTestUtils, TestContext},
    DatabaseProtocol,
};

fn mssql_trim_templates() -> Vec<(String, String)> {
    // MssqlQuery deliberately has no generic templates for these functions.
    [
        ("TRIM_1", "LTRIM(RTRIM({{ args[0] }}))"),
        ("BTRIM_1", "LTRIM(RTRIM({{ args[0] }}))"),
        ("LTRIM_1", "LTRIM({{ args[0] }})"),
        ("RTRIM_1", "RTRIM({{ args[0] }})"),
    ]
    .iter()
    .map(|(name, sql)| (format!("functions/{name}"), (*sql).into()))
    .collect()
}

#[tokio::test]
async fn test_trim_mssql_filters_and_projection() {
    let context =
        TestContext::with_custom_templates(DatabaseProtocol::PostgreSQL, mssql_trim_templates())
            .await;
    for expr in [
        "TRIM(customer_gender) = ''",
        "NULLIF(TRIM(customer_gender), '') IS NULL",
        "customer_gender IS NULL OR TRIM(customer_gender) = ''",
        "BTRIM(customer_gender) = ''",
        "LTRIM(RTRIM(customer_gender)) = ''",
    ] {
        for group in ["", " GROUP BY customer_gender"] {
            let query = format!(
                "SELECT customer_gender FROM KibanaSampleDataEcommerce WHERE {expr}{group} LIMIT 1"
            );
            let plan = context.convert_sql_to_cube_query(&query).await.unwrap();
            let sql = plan
                .as_logical_plan()
                .find_cube_scan_wrapped_sql()
                .wrapped_sql
                .sql;
            assert!(sql.contains("LTRIM(RTRIM("), "{}: {}", query, sql);
            assert!(
                sql.contains("WHERE")
                    || sql.contains("HAVING")
                    || regex::Regex::new(r#""segments":\s*\[\s*""#)
                        .unwrap()
                        .is_match(&sql),
                "filter must remain in the semantic read: {}",
                sql
            );
        }
    }
    for expr in [
        "TRIM(customer_gender)",
        "LTRIM(customer_gender)",
        "RTRIM(customer_gender)",
    ] {
        let query = format!("SELECT {expr} FROM KibanaSampleDataEcommerce LIMIT 1");
        let plan = context.convert_sql_to_cube_query(&query).await.unwrap();
        let sql = plan
            .as_logical_plan()
            .find_cube_scan_wrapped_sql()
            .wrapped_sql
            .sql;
        assert!(sql.contains("LTRIM(") || sql.contains("RTRIM("), "{}", sql);
    }
}

#[tokio::test]
async fn test_trim_mssql_character_overloads_not_pushed_down() {
    let context =
        TestContext::with_custom_templates(DatabaseProtocol::PostgreSQL, mssql_trim_templates())
            .await;
    for expr in [
        "TRIM('x' FROM customer_gender)",
        "TRIM(BOTH 'x' FROM customer_gender)",
        "TRIM(LEADING 'x' FROM customer_gender)",
        "TRIM(TRAILING 'x' FROM customer_gender)",
        "BTRIM(customer_gender, 'x')",
        "LTRIM(customer_gender, ' ')",
        "RTRIM(customer_gender, NULL)",
    ] {
        let query = format!(
            "SELECT customer_gender FROM KibanaSampleDataEcommerce WHERE {expr} = '' LIMIT 1"
        );
        assert!(
            context.convert_sql_to_cube_query(&query).await.is_err(),
            "{}",
            query
        );
    }
}

#[tokio::test]
async fn test_trim_generic_templates_keep_character_arguments() {
    let context = TestContext::with_custom_templates(
        DatabaseProtocol::PostgreSQL,
        vec![
            ("functions/BTRIM".into(), "BTRIM({{ args_concat }})".into()),
            ("functions/LTRIM".into(), "LTRIM({{ args_concat }})".into()),
            ("functions/RTRIM".into(), "RTRIM({{ args_concat }})".into()),
        ],
    )
    .await;
    for fun in ["BTRIM", "LTRIM", "RTRIM"] {
        let query = format!("SELECT customer_gender FROM KibanaSampleDataEcommerce WHERE {fun}(customer_gender, 'x') = '' LIMIT 1");
        let plan = context.convert_sql_to_cube_query(&query).await.unwrap();
        let sql = plan
            .as_logical_plan()
            .find_cube_scan_wrapped_sql()
            .wrapped_sql
            .sql;
        assert!(sql.contains(&format!("{fun}(")), "{}", sql);
        assert!(
            sql.contains(", $"),
            "character argument must remain parameterized: {}",
            sql
        );
    }
}
