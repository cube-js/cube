use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

#[test]
fn test_string_measure_without_pre_aggregation() {
    let schema = MockSchema::from_yaml_file("common/string_measures.yaml");
    let test_context = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - visitors_str.sources_list
          - visitors_str.count
        dimensions:
          - visitors_str.status
        time_dimensions:
          - dimension: visitors_str.created_at
            granularity: day
            dateRange:
              - \"2017-01-01\"
              - \"2017-01-10\"
        order:
          - id: visitors_str.status
    "};

    let (sql, pre_aggrs) = test_context
        .build_sql_with_used_pre_aggregations(query_yaml)
        .expect("Should generate SQL");

    assert!(
        pre_aggrs.is_empty(),
        "Should not use pre-aggregation since timeDimension is not in the rollup"
    );

    assert!(
        !sql.contains("string_measure_rollup"),
        "SQL should not contain pre-aggregation table reference, got: {sql}"
    );

    assert!(
        sql.contains("STRING_AGG"),
        "SQL should contain STRING_AGG for string measure, got: {sql}"
    );

    assert!(
        !sql.contains("sum(STRING_AGG"),
        "SQL should not wrap STRING_AGG in sum, got: {sql}"
    );
}

#[test]
fn test_string_measure_with_pre_aggregation() {
    let schema = MockSchema::from_yaml_file("common/string_measures.yaml");
    let test_context = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - visitors_str.sources_list
          - visitors_str.count
        dimensions:
          - visitors_str.status
    "};

    let (sql, pre_aggrs) = test_context
        .build_sql_with_used_pre_aggregations(query_yaml)
        .expect("Should generate SQL");

    assert_eq!(pre_aggrs.len(), 1, "Should use one pre-aggregation");
    assert_eq!(pre_aggrs[0].name(), "string_measure_rollup");

    assert!(
        sql.contains("string_measure_rollup"),
        "SQL should contain pre-aggregation table reference, got: {sql}"
    );

    assert!(
        sql.contains("visitors_str__sources_list"),
        "SQL should reference sources_list column, got: {sql}"
    );

    assert!(
        !sql.contains("sum(\"visitors_str__sources_list\")"),
        "SQL should not wrap string measure in sum, got: {sql}"
    );
}

#[test]
fn test_string_measure_no_match_without_dimension() {
    let schema = MockSchema::from_yaml_file("common/string_measures.yaml");
    let test_context = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - visitors_str.sources_list
          - visitors_str.count
    "};

    let (sql, pre_aggrs) = test_context
        .build_sql_with_used_pre_aggregations(query_yaml)
        .expect("Should generate SQL");

    assert!(
        pre_aggrs.is_empty(),
        "Should not match pre-aggregation when status dimension is missing from query, got: {} pre-aggregations",
        pre_aggrs.len()
    );

    assert!(
        !sql.contains("string_measure_rollup"),
        "SQL should not contain pre-aggregation table reference, got: {sql}"
    );
}
