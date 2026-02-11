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
fn test_diamond_join_over_path_in_request_sql() {
    let schema = MockSchema::from_yaml_file("common/diamond_joins.yaml");
    let test_context = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - cube_a.count
        dimensions:
          - cube_a.cube_b.cube_c.code
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

    //insta::assert_snapshot!(sql);
}
