use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

#[test]
fn test_equals_filter() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {"
        measures:
          - visitors.count
        filters:
          - dimension: visitors.source
            operator: equals
            values:
              - google
    "};

    let sql = ctx.build_sql(query).expect("Should generate SQL");

    assert!(
        sql.contains(r#""visitors".source = $_0_$"#),
        "SQL should contain equals filter, got: {sql}"
    );
}
