use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_case_empty_when.yaml");
    TestContext::new(schema).unwrap()
}

#[cfg(feature = "integration-postgres")]
async fn execute_oracle(sql: &str) -> String {
    use crate::test_fixtures::test_utils::{integration_context, pg_service};
    let client = pg_service::connect_and_seed("integration_basic_tables.sql").await;
    let messages = client.simple_query(sql).await.unwrap();
    integration_context::format_simple_query_results(&messages)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_case_without_when_branches_renders_else() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.status_bucket
          - orders.status
        order:
          - id: orders.status
    "};

    ctx.build_sql(query).unwrap();

    #[cfg(feature = "integration-postgres")]
    {
        let result = ctx
            .try_execute_pg(query, "integration_basic_tables.sql")
            .await
            .unwrap();
        let expected = execute_oracle(indoc! {"
            SELECT 'Unknown' AS orders__status_bucket, status AS orders__status,
                   count(*) AS orders__count
            FROM orders GROUP BY 2 ORDER BY 2
        "})
        .await;
        assert_eq!(result, expected);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_case_without_when_branches_in_filter() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        filters:
          - member: orders.status_bucket
            operator: equals
            values:
              - Unknown
    "};

    ctx.build_sql(query).unwrap();

    #[cfg(feature = "integration-postgres")]
    {
        let result = ctx
            .try_execute_pg(query, "integration_basic_tables.sql")
            .await
            .unwrap();
        let expected = execute_oracle("SELECT count(*) AS orders__count FROM orders").await;
        assert_eq!(result, expected);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_case_without_when_branches_sql_else_label() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.big_or_pending
          - orders.not_big_or_pending
        order:
          - id: orders.big_or_pending
    "};

    ctx.build_sql(query).unwrap();

    #[cfg(feature = "integration-postgres")]
    {
        let result = ctx
            .try_execute_pg(query, "integration_basic_tables.sql")
            .await
            .unwrap();
        let expected = execute_oracle(indoc! {"
            SELECT (amount > 100 OR status = 'pending') AS orders__big_or_pending,
                   NOT (amount > 100 OR status = 'pending') AS orders__not_big_or_pending,
                   count(*) AS orders__count
            FROM orders GROUP BY 1, 2 ORDER BY 1
        "})
        .await;
        assert_eq!(result, expected);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_case_without_when_branches_sql_else_label_in_filter() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        filters:
          - member: orders.big_or_pending
            operator: equals
            values:
              - \"false\"
    "};

    ctx.build_sql(query).unwrap();

    #[cfg(feature = "integration-postgres")]
    {
        let result = ctx
            .try_execute_pg(query, "integration_basic_tables.sql")
            .await
            .unwrap();
        let expected = execute_oracle(
            "SELECT count(*) AS orders__count FROM orders \
             WHERE NOT (amount > 100 OR status = 'pending')",
        )
        .await;
        assert_eq!(result, expected);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_case_without_when_branches_sql_else_label_in_measure() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count_plus_one
          - orders.count_plus_one_doubled
    "};

    ctx.build_sql(query).unwrap();

    #[cfg(feature = "integration-postgres")]
    {
        let result = ctx
            .try_execute_pg(query, "integration_basic_tables.sql")
            .await
            .unwrap();
        let expected = execute_oracle(indoc! {"
            SELECT count(*) + 1 AS orders__count_plus_one,
                   (count(*) + 1) * 2 AS orders__count_plus_one_doubled
            FROM orders
        "})
        .await;
        assert_eq!(result, expected);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_switch_case_else_only_in_measure() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.switch_else_only
          - orders.switch_else_only_doubled
    "};

    ctx.build_sql(query).unwrap();

    #[cfg(feature = "integration-postgres")]
    {
        let result = ctx
            .try_execute_pg(query, "integration_basic_tables.sql")
            .await
            .unwrap();
        let expected = execute_oracle(indoc! {"
            SELECT count(*) + 1 AS orders__switch_else_only,
                   (count(*) + 1) * 2 AS orders__switch_else_only_doubled
            FROM orders
        "})
        .await;
        assert_eq!(result, expected);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_switch_case_single_branch_in_measure() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.switch_by_mode_doubled
        filters:
          - member: orders.mode
            operator: equals
            values:
              - A
    "};

    ctx.build_sql(query).unwrap();

    #[cfg(feature = "integration-postgres")]
    {
        let result = ctx
            .try_execute_pg(query, "integration_basic_tables.sql")
            .await
            .unwrap();
        let expected = execute_oracle(
            "SELECT (count(*) + 1) * 2 AS orders__switch_by_mode_doubled FROM orders",
        )
        .await;
        assert_eq!(result, expected);
    }
}
