use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn build(filter_yaml: &str) -> String {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let query = format!("measures:\n  - visitors.count\n{}", filter_yaml);
    ctx.build_filter_sql(&query)
        .expect("Should generate filter SQL")
}

#[test]
fn test_equals_string() {
    let sql = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: equals
            values:
              - google
    "});
    assert_eq!(sql, r#"("visitors".source = $_0_$)"#);
}

#[test]
fn test_equals_number() {
    let sql = build(indoc! {"
        filters:
          - dimension: visitors.id
            operator: equals
            values:
              - \"42\"
    "});
    assert_eq!(sql, r#"("visitors".id = $_0_$)"#);
}

#[test]
fn test_equals_multiple_values() {
    let sql = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: equals
            values:
              - google
              - facebook
    "});
    assert_eq!(sql, r#"("visitors".source IN ($_0_$, $_1_$))"#);
}

#[test]
fn test_equals_null() {
    let sql = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: equals
            values:
              -
    "});
    assert_eq!(sql, r#"("visitors".source IS NULL)"#);
}


#[test]
fn test_not_equals_string() {
    let sql = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: notEquals
            values:
              - google
    "});
    assert_eq!(
        sql,
        r#"("visitors".source <> $_0_$ OR "visitors".source IS NULL)"#
    );
}

#[test]
fn test_not_equals_multiple_values() {
    let sql = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: notEquals
            values:
              - google
              - facebook
    "});
    assert_eq!(
        sql,
        r#"("visitors".source NOT IN ($_0_$, $_1_$) OR "visitors".source IS NULL)"#
    );
}

#[test]
fn test_not_equals_null() {
    let sql = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: notEquals
            values:
              -
    "});
    assert_eq!(sql, r#"("visitors".source IS NOT NULL)"#);
}

// ── in / notIn ──────────────────────────────────────────────────────────────

#[test]
fn test_in_filter() {
    let sql = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: in
            values:
              - google
              - facebook
              - twitter
    "});
    assert_eq!(
        sql,
        r#"("visitors".source IN ($_0_$, $_1_$, $_2_$))"#
    );
}

#[test]
fn test_in_with_null() {
    let sql = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: in
            values:
              - google
              -
    "});
    assert_eq!(
        sql,
        r#"("visitors".source IN ($_0_$) OR "visitors".source IS NULL)"#
    );
}

#[test]
fn test_not_in_filter() {
    let sql = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: notIn
            values:
              - google
              - facebook
    "});
    assert_eq!(
        sql,
        r#"("visitors".source NOT IN ($_0_$, $_1_$) OR "visitors".source IS NULL)"#
    );
}

#[test]
fn test_not_in_with_null() {
    let sql = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: notIn
            values:
              - google
              -
    "});
    assert_eq!(sql, r#"("visitors".source NOT IN ($_0_$))"#);
}

// ── set / notSet ────────────────────────────────────────────────────────────

#[test]
fn test_set_filter() {
    let sql = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: set
    "});
    assert_eq!(sql, r#"("visitors".source IS NOT NULL)"#);
}

#[test]
fn test_not_set_filter() {
    let sql = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: notSet
    "});
    assert_eq!(sql, r#"("visitors".source IS NULL)"#);
}

// ── comparison operators ────────────────────────────────────────────────────

#[test]
fn test_gt_filter() {
    let sql = build(indoc! {"
        filters:
          - dimension: visitors.id
            operator: gt
            values:
              - \"100\"
    "});
    assert_eq!(sql, r#"("visitors".id > $_0_$)"#);
}

#[test]
fn test_gte_filter() {
    let sql = build(indoc! {"
        filters:
          - dimension: visitors.id
            operator: gte
            values:
              - \"100\"
    "});
    assert_eq!(sql, r#"("visitors".id >= $_0_$)"#);
}

#[test]
fn test_lt_filter() {
    let sql = build(indoc! {"
        filters:
          - dimension: visitors.id
            operator: lt
            values:
              - \"100\"
    "});
    assert_eq!(sql, r#"("visitors".id < $_0_$)"#);
}

#[test]
fn test_lte_filter() {
    let sql = build(indoc! {"
        filters:
          - dimension: visitors.id
            operator: lte
            values:
              - \"100\"
    "});
    assert_eq!(sql, r#"("visitors".id <= $_0_$)"#);
}

// ── like operators ──────────────────────────────────────────────────────────

#[test]
fn test_contains_filter() {
    let sql = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: contains
            values:
              - goo
    "});
    assert_eq!(
        sql,
        r#"(("visitors".source ILIKE '%' || $_0_$|| '%'))"#
    );
}

#[test]
fn test_not_contains_filter() {
    let sql = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: notContains
            values:
              - goo
    "});
    assert_eq!(
        sql,
        r#"(("visitors".source NOT ILIKE '%' || $_0_$|| '%') OR "visitors".source IS NULL)"#
    );
}

#[test]
fn test_starts_with_filter() {
    let sql = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: startsWith
            values:
              - goo
    "});
    assert_eq!(sql, r#"(("visitors".source ILIKE $_0_$|| '%'))"#);
}

#[test]
fn test_not_starts_with_filter() {
    let sql = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: notStartsWith
            values:
              - goo
    "});
    assert_eq!(
        sql,
        r#"(("visitors".source NOT ILIKE $_0_$|| '%') OR "visitors".source IS NULL)"#
    );
}

#[test]
fn test_ends_with_filter() {
    let sql = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: endsWith
            values:
              - gle
    "});
    assert_eq!(sql, r#"(("visitors".source ILIKE '%' || $_0_$))"#);
}

#[test]
fn test_not_ends_with_filter() {
    let sql = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: notEndsWith
            values:
              - gle
    "});
    assert_eq!(
        sql,
        r#"(("visitors".source NOT ILIKE '%' || $_0_$) OR "visitors".source IS NULL)"#
    );
}

#[test]
fn test_contains_multiple_values() {
    let sql = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: contains
            values:
              - goo
              - face
    "});
    assert_eq!(
        sql,
        r#"(("visitors".source ILIKE '%' || $_0_$|| '%' OR "visitors".source ILIKE '%' || $_1_$|| '%'))"#
    );
}

#[test]
fn test_not_contains_multiple_values() {
    let sql = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: notContains
            values:
              - goo
              - face
    "});
    assert_eq!(
        sql,
        r#"(("visitors".source NOT ILIKE '%' || $_0_$|| '%' AND "visitors".source NOT ILIKE '%' || $_1_$|| '%') OR "visitors".source IS NULL)"#
    );
}

#[test]
fn test_or_filter_group() {
    let sql = build(indoc! {"
        filters:
          - or:
              - dimension: visitors.source
                operator: equals
                values:
                  - google
              - dimension: visitors.source
                operator: equals
                values:
                  - facebook
    "});
    assert_eq!(
        sql,
        r#"(("visitors".source = $_0_$) OR ("visitors".source = $_1_$))"#
    );
}

#[test]
fn test_and_filter_group() {
    let sql = build(indoc! {"
        filters:
          - and:
              - dimension: visitors.source
                operator: equals
                values:
                  - google
              - dimension: visitors.id
                operator: gt
                values:
                  - \"100\"
    "});
    assert_eq!(
        sql,
        r#"(("visitors".source = $_0_$) AND ("visitors".id > $_1_$))"#
    );
}
