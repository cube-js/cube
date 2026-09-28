use crate::cube_bridge::base_query_options::FilterValue;
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

/// The cube SQL reads one security context value from two places. Equal values
/// collapse to a single recorded value, so the planner splices the same param
/// placeholder at both occurrences.
fn schema() -> MockSchema {
    MockSchema::from_yaml(indoc! {"
        cubes:
            - name: orders
              sql: \"SELECT * FROM orders WHERE tenant_id = {SECURITY_VALUE:acme} OR parent_tenant_id = {SECURITY_VALUE:acme}\"
              dimensions:
                  - name: id
                    type: number
                    sql: id
                    primary_key: true
              measures:
                  - name: count
                    type: count
    "})
    .unwrap()
}

const QUERY: &str = indoc! {"
    measures:
      - orders.count
"};

#[test]
fn positional_params_get_one_param_per_placeholder() {
    let ctx = TestContext::new_with_positional_params(schema()).unwrap();

    let (sql, params) = ctx.build_sql_and_params(QUERY).unwrap();

    // `?` carries no index, so both placeholders need their own value.
    assert_eq!(
        sql.matches('?').count(),
        params.len(),
        "sql: {}\nparams: {:?}",
        sql,
        params
    );
    assert_eq!(
        params,
        vec![
            FilterValue::Str("acme".to_string()),
            FilterValue::Str("acme".to_string())
        ]
    );
}

#[test]
fn indexed_params_are_reused_across_placeholders() {
    let ctx = TestContext::new(schema()).unwrap();

    let (sql, params) = ctx.build_sql_and_params(QUERY).unwrap();

    // `$1` addresses its value, so both occurrences share one param — and stay
    // textually equal, which Postgres requires from repeated expressions.
    assert_eq!(sql.matches("$1").count(), 2, "sql: {}", sql);
    assert_eq!(params, vec![FilterValue::Str("acme".to_string())]);
}

/// A query served from an external pre-aggregation is rendered with the CubeStore
/// dialect, so params must follow *its* placeholder form — CubeStore accepts only
/// positional `?`, consumed one per occurrence, whether they are bound over the
/// WS protocol or inlined by the driver.
#[test]
fn external_pre_aggregation_renders_params_with_the_cubestore_dialect() {
    let ctx = TestContext::new_with_external_cubestore(MockSchema::from_yaml_file(
        "common/integration_cubestore_basic.yaml",
    ))
    .unwrap();

    let query = indoc! {"
        measures:
          - visitors.count
        dimensions:
          - visitors.source
        filters:
          - dimension: visitors.source
            operator: equals
            values:
              - some
    "};

    let (_, pre_aggregations) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();
    assert!(
        pre_aggregations
            .iter()
            .all(|u| u.pre_aggregation.external()),
        "the query must be served from an external pre-aggregation"
    );

    let (sql, params) = ctx.build_sql_and_params(query).unwrap();

    assert!(
        !sql.contains("$1"),
        "params must not render in the source dialect's indexed form\nsql: {}",
        sql
    );
    assert_eq!(
        sql.matches('?').count(),
        params.len(),
        "sql: {}\nparams: {:?}",
        sql,
        params
    );
    assert_eq!(params, vec![FilterValue::Str("some".to_string())]);
}
