use crate::cube_bridge::base_query_options::FilterValue;
use crate::planner::row_limit::MAX_SOURCE_ROW_LIMIT;
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::{formatdoc, indoc};

fn schema() -> MockSchema {
    MockSchema::from_yaml(indoc! {"
        cubes:
            - name: orders
              sql: \"SELECT * FROM orders\"
              dimensions:
                  - name: id
                    type: number
                    sql: id
                    primary_key: true
                  - name: status
                    type: string
                    sql: status
              measures:
                  - name: count
                    type: count
    "})
    .unwrap()
}

fn query(row_limit: &str) -> String {
    formatdoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.status
        filters:
          - member: orders.status
            operator: equals
            values:
              - completed
        row_limit: \"{row_limit}\"
    "}
}

/// The orchestrator substitutes the limit for the placeholder at execution time, so it must
/// reach the SQL as a param, not be dropped.
#[test]
fn max_source_row_limit_renders_as_a_param() {
    let ctx = TestContext::new(schema()).unwrap();

    let (sql, params) = ctx
        .build_sql_and_params(&query(MAX_SOURCE_ROW_LIMIT))
        .unwrap();

    assert!(sql.trim_end().ends_with("LIMIT $2"), "sql: {}", sql);
    assert_eq!(
        params,
        vec![
            FilterValue::Str("completed".to_string()),
            FilterValue::Str(MAX_SOURCE_ROW_LIMIT.to_string()),
        ]
    );
}

#[test]
fn max_source_row_limit_follows_positional_param_order() {
    let ctx = TestContext::new_with_positional_params(schema()).unwrap();

    let (sql, params) = ctx
        .build_sql_and_params(&query(MAX_SOURCE_ROW_LIMIT))
        .unwrap();

    assert!(sql.trim_end().ends_with("LIMIT ?"), "sql: {}", sql);
    assert_eq!(sql.matches('?').count(), params.len(), "sql: {}", sql);
    assert_eq!(
        params.last(),
        Some(&FilterValue::Str(MAX_SOURCE_ROW_LIMIT.to_string()))
    );
}

#[test]
fn numeric_row_limit_stays_inline() {
    let ctx = TestContext::new(schema()).unwrap();

    let (sql, params) = ctx.build_sql_and_params(&query("10")).unwrap();

    assert!(sql.trim_end().ends_with("LIMIT 10"), "sql: {}", sql);
    assert_eq!(params, vec![FilterValue::Str("completed".to_string())]);
}
