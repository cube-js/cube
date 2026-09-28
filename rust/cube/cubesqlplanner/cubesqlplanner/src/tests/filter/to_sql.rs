use super::{assert_filter, build_filter};
use indoc::indoc;

fn build(filter_yaml: &str) -> (String, Vec<String>) {
    build_filter("common/visitors.yaml", filter_yaml)
}

// ── equals ──────────────────────────────────────────────────────────────────

#[test]
fn test_equals_string() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: equals
            values:
              - google
    "});
    assert_filter(&result, r#"("visitors".source = $_0_$)"#, &["google"]);
}

#[test]
fn test_equals_number() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.id
            operator: equals
            values:
              - \"42\"
    "});
    assert_filter(&result, r#"("visitors".id = $_0_$::numeric)"#, &["42"]);
}

#[test]
fn test_equals_boolean() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.is_active
            operator: equals
            values:
              - \"true\"
    "});
    assert_filter(
        &result,
        r#"("visitors".is_active = $_0_$::boolean)"#,
        &["true"],
    );
}

#[test]
fn test_equals_multiple_values() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: equals
            values:
              - google
              - facebook
    "});
    assert_filter(
        &result,
        r#"("visitors".source IN ($_0_$, $_1_$))"#,
        &["google", "facebook"],
    );
}

#[test]
fn test_equals_null() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: equals
            values:
              -
    "});
    assert_filter(&result, r#"("visitors".source IS NULL)"#, &[]);
}

#[test]
fn test_equals_values_with_null() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: equals
            values:
              - google
              -
    "});
    assert_filter(
        &result,
        r#"("visitors".source IN ($_0_$) OR "visitors".source IS NULL)"#,
        &["google"],
    );
}

#[test]
fn test_not_equals_values_with_null() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: notEquals
            values:
              - google
              -
    "});
    assert_filter(
        &result,
        r#"("visitors".source NOT IN ($_0_$))"#,
        &["google"],
    );
}

// ── notEquals ───────────────────────────────────────────────────────────────

#[test]
fn test_not_equals_string() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: notEquals
            values:
              - google
    "});
    assert_filter(
        &result,
        r#"("visitors".source <> $_0_$ OR "visitors".source IS NULL)"#,
        &["google"],
    );
}

#[test]
fn test_not_equals_multiple_values() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: notEquals
            values:
              - google
              - facebook
    "});
    assert_filter(
        &result,
        r#"("visitors".source NOT IN ($_0_$, $_1_$) OR "visitors".source IS NULL)"#,
        &["google", "facebook"],
    );
}

#[test]
fn test_not_equals_null() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: notEquals
            values:
              -
    "});
    assert_filter(&result, r#"("visitors".source IS NOT NULL)"#, &[]);
}

// ── in / notIn ──────────────────────────────────────────────────────────────

#[test]
fn test_in_filter() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: in
            values:
              - google
              - facebook
              - twitter
    "});
    assert_filter(
        &result,
        r#"("visitors".source IN ($_0_$, $_1_$, $_2_$))"#,
        &["google", "facebook", "twitter"],
    );
}

#[test]
fn test_in_with_null() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: in
            values:
              - google
              -
    "});
    assert_filter(
        &result,
        r#"("visitors".source IN ($_0_$) OR "visitors".source IS NULL)"#,
        &["google"],
    );
}

#[test]
fn test_not_in_filter() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: notIn
            values:
              - google
              - facebook
    "});
    assert_filter(
        &result,
        r#"("visitors".source NOT IN ($_0_$, $_1_$) OR "visitors".source IS NULL)"#,
        &["google", "facebook"],
    );
}

#[test]
fn test_not_in_with_null() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: notIn
            values:
              - google
              -
    "});
    assert_filter(
        &result,
        r#"("visitors".source NOT IN ($_0_$))"#,
        &["google"],
    );
}

// ── set / notSet ────────────────────────────────────────────────────────────

#[test]
fn test_set_filter() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: set
    "});
    assert_filter(&result, r#"("visitors".source IS NOT NULL)"#, &[]);
}

#[test]
fn test_not_set_filter() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: notSet
    "});
    assert_filter(&result, r#"("visitors".source IS NULL)"#, &[]);
}

// ── comparison operators ────────────────────────────────────────────────────

#[test]
fn test_gt_filter() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.id
            operator: gt
            values:
              - \"100\"
    "});
    assert_filter(&result, r#"("visitors".id > $_0_$::numeric)"#, &["100"]);
}

#[test]
fn test_gte_filter() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.id
            operator: gte
            values:
              - \"100\"
    "});
    assert_filter(&result, r#"("visitors".id >= $_0_$::numeric)"#, &["100"]);
}

#[test]
fn test_lt_filter() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.id
            operator: lt
            values:
              - \"100\"
    "});
    assert_filter(&result, r#"("visitors".id < $_0_$::numeric)"#, &["100"]);
}

#[test]
fn test_lte_filter() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.id
            operator: lte
            values:
              - \"100\"
    "});
    assert_filter(&result, r#"("visitors".id <= $_0_$::numeric)"#, &["100"]);
}

#[test]
fn test_gt_string_no_cast() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: gt
            values:
              - abc
    "});
    assert_filter(&result, r#"("visitors".source > $_0_$)"#, &["abc"]);
}

#[test]
fn test_lte_string_no_cast() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: lte
            values:
              - zzz
    "});
    assert_filter(&result, r#"("visitors".source <= $_0_$)"#, &["zzz"]);
}

#[test]
fn test_contains_filter() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: contains
            values:
              - goo
    "});
    assert_filter(
        &result,
        r#"(("visitors".source ILIKE '%' || $_0_$|| '%'))"#,
        &["goo"],
    );
}

#[test]
fn test_not_contains_filter() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: notContains
            values:
              - goo
    "});
    assert_filter(
        &result,
        r#"(("visitors".source NOT ILIKE '%' || $_0_$|| '%') OR "visitors".source IS NULL)"#,
        &["goo"],
    );
}

#[test]
fn test_starts_with_filter() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: startsWith
            values:
              - goo
    "});
    assert_filter(
        &result,
        r#"(("visitors".source ILIKE $_0_$|| '%'))"#,
        &["goo"],
    );
}

#[test]
fn test_not_starts_with_filter() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: notStartsWith
            values:
              - goo
    "});
    assert_filter(
        &result,
        r#"(("visitors".source NOT ILIKE $_0_$|| '%') OR "visitors".source IS NULL)"#,
        &["goo"],
    );
}

#[test]
fn test_ends_with_filter() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: endsWith
            values:
              - gle
    "});
    assert_filter(
        &result,
        r#"(("visitors".source ILIKE '%' || $_0_$))"#,
        &["gle"],
    );
}

#[test]
fn test_not_ends_with_filter() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: notEndsWith
            values:
              - gle
    "});
    assert_filter(
        &result,
        r#"(("visitors".source NOT ILIKE '%' || $_0_$) OR "visitors".source IS NULL)"#,
        &["gle"],
    );
}

// ── contains with multiple values ───────────────────────────────────────────

#[test]
fn test_contains_multiple_values() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: contains
            values:
              - goo
              - face
    "});
    assert_filter(
        &result,
        r#"(("visitors".source ILIKE '%' || $_0_$|| '%' OR "visitors".source ILIKE '%' || $_1_$|| '%'))"#,
        &["goo", "face"],
    );
}

#[test]
fn test_not_contains_multiple_values() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: notContains
            values:
              - goo
              - face
    "});
    assert_filter(
        &result,
        r#"(("visitors".source NOT ILIKE '%' || $_0_$|| '%' AND "visitors".source NOT ILIKE '%' || $_1_$|| '%') OR "visitors".source IS NULL)"#,
        &["goo", "face"],
    );
}

// ── like with null ──────────────────────────────────────────────────────────

#[test]
fn test_contains_with_null() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: contains
            values:
              - goo
              -
    "});
    assert_filter(
        &result,
        r#"(("visitors".source ILIKE '%' || $_0_$|| '%') OR "visitors".source IS NULL)"#,
        &["goo"],
    );
}

#[test]
fn test_not_contains_with_null() {
    let result = build(indoc! {"
        filters:
          - dimension: visitors.source
            operator: notContains
            values:
              - goo
              -
    "});
    assert_filter(
        &result,
        r#"(("visitors".source NOT ILIKE '%' || $_0_$|| '%'))"#,
        &["goo"],
    );
}

// ── filter groups (OR / AND) ────────────────────────────────────────────────

#[test]
fn test_or_filter_group() {
    let result = build(indoc! {"
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
    assert_filter(
        &result,
        r#"(("visitors".source = $_0_$) OR ("visitors".source = $_1_$))"#,
        &["google", "facebook"],
    );
}

#[test]
fn test_and_filter_group() {
    let result = build(indoc! {"
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
    assert_filter(
        &result,
        r#"(("visitors".source = $_0_$) AND ("visitors".id > $_1_$::numeric))"#,
        &["google", "100"],
    );
}

#[test]
fn test_nested_and_or_groups() {
    // AND(OR(eq, eq), OR(AND(gt, contains), lt), set)
    let result = build(indoc! {"
        filters:
          - and:
              - or:
                  - dimension: visitors.source
                    operator: equals
                    values:
                      - google
                  - dimension: visitors.source
                    operator: equals
                    values:
                      - facebook
              - or:
                  - and:
                      - dimension: visitors.id
                        operator: gt
                        values:
                          - \"100\"
                      - dimension: visitors.source
                        operator: contains
                        values:
                          - goo
                  - dimension: visitors.id
                    operator: lt
                    values:
                      - \"10\"
              - dimension: visitors.source
                operator: set
    "});
    assert_filter(
        &result,
        r#"((("visitors".source = $_0_$) OR ("visitors".source = $_1_$)) AND ((("visitors".id > $_2_$::numeric) AND (("visitors".source ILIKE '%' || $_3_$|| '%'))) OR ("visitors".id < $_4_$::numeric)) AND ("visitors".source IS NOT NULL))"#,
        &["google", "facebook", "100", "goo", "10"],
    );
}

// ── date operators ──────────────────────────────────────────────────────────

#[test]
fn test_in_date_range_date_only() {
    let result = build(indoc! {r#"
        filters:
          - dimension: visitors.created_at
            operator: inDateRange
            values:
              - "2024-01-01"
              - "2024-12-31"
    "#});
    assert_filter(
        &result,
        r#"("visitors".created_at >= $_0_$::timestamptz AND "visitors".created_at <= $_1_$::timestamptz)"#,
        &["2024-01-01T00:00:00.000", "2024-12-31T23:59:59.999"],
    );
}

#[test]
fn test_in_date_range_full_timestamp() {
    let result = build(indoc! {r#"
        filters:
          - dimension: visitors.created_at
            operator: inDateRange
            values:
              - "2024-01-01T10:00:00.000"
              - "2024-06-15T18:30:00.000"
    "#});
    assert_filter(
        &result,
        r#"("visitors".created_at >= $_0_$::timestamptz AND "visitors".created_at <= $_1_$::timestamptz)"#,
        &["2024-01-01T10:00:00.000", "2024-06-15T18:30:00.000"],
    );
}

#[test]
fn test_not_in_date_range() {
    let result = build(indoc! {r#"
        filters:
          - dimension: visitors.created_at
            operator: notInDateRange
            values:
              - "2024-01-01"
              - "2024-12-31"
    "#});
    assert_filter(
        &result,
        r#"("visitors".created_at < $_0_$::timestamptz OR "visitors".created_at > $_1_$::timestamptz)"#,
        &["2024-01-01T00:00:00.000", "2024-12-31T23:59:59.999"],
    );
}

#[test]
fn test_before_date() {
    let result = build(indoc! {r#"
        filters:
          - dimension: visitors.created_at
            operator: beforeDate
            values:
              - "2024-06-01"
    "#});
    assert_filter(
        &result,
        r#"("visitors".created_at < $_0_$::timestamptz)"#,
        &["2024-06-01T00:00:00.000"],
    );
}

#[test]
fn test_before_or_on_date() {
    let result = build(indoc! {r#"
        filters:
          - dimension: visitors.created_at
            operator: beforeOrOnDate
            values:
              - "2024-06-01"
    "#});
    assert_filter(
        &result,
        r#"("visitors".created_at <= $_0_$::timestamptz)"#,
        &["2024-06-01T23:59:59.999"],
    );
}

#[test]
fn test_after_date() {
    let result = build(indoc! {r#"
        filters:
          - dimension: visitors.created_at
            operator: afterDate
            values:
              - "2024-06-01"
    "#});
    assert_filter(
        &result,
        r#"("visitors".created_at > $_0_$::timestamptz)"#,
        &["2024-06-01T23:59:59.999"],
    );
}

#[test]
fn test_after_or_on_date() {
    let result = build(indoc! {r#"
        filters:
          - dimension: visitors.created_at
            operator: afterOrOnDate
            values:
              - "2024-06-01"
    "#});
    assert_filter(
        &result,
        r#"("visitors".created_at >= $_0_$::timestamptz)"#,
        &["2024-06-01T00:00:00.000"],
    );
}

#[test]
fn test_time_dimension_date_range() {
    let result = build(indoc! {r#"
        time_dimensions:
          - dimension: visitors.created_at
            granularity: day
            dateRange:
              - "2024-01-01"
              - "2024-12-31"
    "#});
    assert_filter(
        &result,
        r#"("visitors".created_at >= $_0_$::timestamptz AND "visitors".created_at <= $_1_$::timestamptz)"#,
        &["2024-01-01T00:00:00.000", "2024-12-31T23:59:59.999"],
    );
}
