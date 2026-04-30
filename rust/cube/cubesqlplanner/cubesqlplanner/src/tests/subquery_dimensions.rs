use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

#[tokio::test(flavor = "multi_thread")]
async fn test_subquery_dimension_used_in_filter() {
    let schema = MockSchema::from_yaml_file("common/subquery_dimensions.yaml");
    let test_context = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {r#"
        measures:
          - Sales.totalAmount
        filters:
          - member: Customers.totalSpend
            operator: gt
            values:
              - "100"
    "#};

    let sql = test_context
        .build_sql(query_yaml)
        .expect("Should generate SQL for subquery dimension used in filter");

    // Subquery dimension in filter must be resolved as a correlated subquery,
    // not inlined as a raw aggregate expression
    assert!(
        sql.contains("SELECT"),
        "Filter on subquery dimension should produce a subquery in WHERE, got: {sql}"
    );
    let subquery_count = sql.matches("SELECT").count();
    assert!(
        subquery_count >= 2,
        "Expected at least 2 SELECTs (main + subquery), got {subquery_count} in: {sql}"
    );

    // Raw aggregate must not leak into WHERE — it belongs inside the subquery
    let where_pos = sql.find("WHERE").expect("SQL should have WHERE clause");
    let where_clause = &sql[where_pos..];
    assert!(
        !where_clause.contains("sum(\"customer_orders\""),
        "WHERE clause must not contain raw aggregate from CustomerOrders, got: {where_clause}"
    );

    if let Some(result) = test_context
        .try_execute_pg(query_yaml, "subquery_dimensions_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subquery_dimension_in_projection() {
    let schema = MockSchema::from_yaml_file("common/subquery_dimensions.yaml");
    let test_context = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - Sales.totalAmount
        dimensions:
          - Customers.totalSpend
    "};

    let sql = test_context
        .build_sql(query_yaml)
        .expect("Should generate SQL for subquery dimension in projection");

    // Subquery dimension in SELECT must be resolved as a correlated subquery
    let subquery_count = sql.matches("SELECT").count();
    assert!(
        subquery_count >= 2,
        "Expected at least 2 SELECTs (main + subquery), got {subquery_count} in: {sql}"
    );

    // Raw aggregate from CustomerOrders must not appear directly in the outer SELECT
    assert!(
        !sql.starts_with("SELECT sum(\"customer_orders\""),
        "Outer SELECT must not contain raw aggregate from CustomerOrders, got: {sql}"
    );

    if let Some(result) = test_context
        .try_execute_pg(query_yaml, "subquery_dimensions_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}
