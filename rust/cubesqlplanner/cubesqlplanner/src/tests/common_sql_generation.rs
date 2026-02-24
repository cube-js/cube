use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

#[test]
fn test_simple_join_sql() {
    let schema = MockSchema::from_yaml_file("common/diamond_joins.yaml");
    let test_context = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - cube_a.count
        dimensions:
          - cube_c.code
    "};

    let sql = test_context
        .build_sql(query_yaml)
        .expect("Should generate SQL for simple join");

    assert!(
        sql.contains(r#"ON "cube_a".c_id = "cube_c".id"#),
        "SQL should contain join condition between cube_a and cube_c"
    );

    insta::assert_snapshot!(sql);
}

#[test]
fn test_simple_paths_in_request_sql() {
    let schema = MockSchema::from_yaml_file("common/diamond_joins.yaml");
    let test_context = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - cube_c.cube_a.count
        dimensions:
          - cube_a.cube_c.code
    "};

    let sql = test_context
        .build_sql(query_yaml)
        .expect("Should generate SQL");

    assert!(
        sql.contains(r#"ON "cube_a".c_id = "cube_c".id"#),
        "SQL should contain join condition between cube_a and cube_c"
    );

    insta::assert_snapshot!(sql);
}

#[test]
fn test_simple_paths_in_time_dimension_request_sql() {
    let schema = MockSchema::from_yaml_file("common/diamond_joins.yaml");
    let test_context = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - cube_a.count
        time_dimensions:
          - dimension: cube_a.cube_c.created_at
            granularity: day
    "};

    let sql = test_context
        .build_sql(query_yaml)
        .expect("Should generate SQL");
    println!("{}", sql);

    assert!(
        sql.contains(r#"ON "cube_a".c_id = "cube_c".id"#),
        "SQL should contain join condition between cube_a and cube_c"
    );

    insta::assert_snapshot!(sql);
}

#[test]
fn test_diamond_join_over_view_sql() {
    let schema = MockSchema::from_yaml_file("common/diamond_joins.yaml");
    let test_context = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - cube_a.count
        dimensions:
          - a_with_b_and_c.code
    "};

    let sql = test_context
        .build_sql(query_yaml)
        .expect("Should generate SQL for simple join");

    assert!(
        sql.contains(r#"ON "cube_a".b_id = "cube_b".id"#),
        "SQL should contain join condition between cube_a and cube_b"
    );

    assert!(
        sql.contains(r#"ON "cube_b".c_id = "cube_c".id"#),
        "SQL should contain join condition between cube_b and cube_c"
    );

    insta::assert_snapshot!(sql);
}

#[test]
fn test_simple_segment_sql() {
    let schema = MockSchema::from_yaml_file("common/simple.yaml");
    let test_context = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - customers.count
        segments:
          - customers.new_york
    "};

    let sql = test_context
        .build_sql(query_yaml)
        .expect("Should generate SQL with segment");

    assert!(
        sql.contains("city = 'New York'"),
        "SQL should contain segment condition"
    );

    insta::assert_snapshot!(sql);
}

#[test]
fn test_segment_as_dimension_in_pre_aggregation_query() {
    let schema = MockSchema::from_yaml_file("common/simple.yaml");
    let test_context = TestContext::new(schema).unwrap();

    // In JS, evaluatePreAggregationReferences() concatenates segments into dimensions
    // before sending the query. So segments arrive as dimensions, not as segments.
    let query_yaml = indoc! {"
        measures:
          - customers.count
        dimensions:
          - customers.new_york
        pre_aggregation_query: true
    "};

    let sql = test_context
        .build_sql(query_yaml)
        .expect("Should generate SQL with segment as dimension");

    assert!(
        !sql.contains("WHERE"),
        "Segment should not be in WHERE clause for pre-aggregation query"
    );

    insta::assert_snapshot!(sql);
}
