use super::{assert_filter, build_filter};
use indoc::indoc;

#[test]
fn test_in_date_range_from_partition_range() {
    let result = build_filter(
        "common/visitors.yaml",
        indoc! {r#"
            filters:
              - dimension: visitors.created_at
                operator: inDateRange
                values:
                  - "__FROM_PARTITION_RANGE"
                  - "2024-12-31"
        "#},
    );
    // __FROM_PARTITION_RANGE skips formatting and tz conversion but still gets allocated + cast
    assert_filter(
        &result,
        r#"("visitors".created_at >= $_0_$::timestamptz AND "visitors".created_at <= $_1_$::timestamptz)"#,
        &["__FROM_PARTITION_RANGE", "2024-12-31T23:59:59.999"],
    );
}

#[test]
fn test_in_date_range_to_partition_range() {
    let result = build_filter(
        "common/visitors.yaml",
        indoc! {r#"
            filters:
              - dimension: visitors.created_at
                operator: inDateRange
                values:
                  - "2024-01-01"
                  - "__TO_PARTITION_RANGE"
        "#},
    );
    assert_filter(
        &result,
        r#"("visitors".created_at >= $_0_$::timestamptz AND "visitors".created_at <= $_1_$::timestamptz)"#,
        &["2024-01-01T00:00:00.000", "__TO_PARTITION_RANGE"],
    );
}

#[test]
fn test_in_date_range_both_partition_range() {
    let result = build_filter(
        "common/visitors.yaml",
        indoc! {r#"
            filters:
              - dimension: visitors.created_at
                operator: inDateRange
                values:
                  - "__FROM_PARTITION_RANGE"
                  - "__TO_PARTITION_RANGE"
        "#},
    );
    assert_filter(
        &result,
        r#"("visitors".created_at >= $_0_$::timestamptz AND "visitors".created_at <= $_1_$::timestamptz)"#,
        &["__FROM_PARTITION_RANGE", "__TO_PARTITION_RANGE"],
    );
}

#[test]
fn test_not_in_date_range_partition_range() {
    let result = build_filter(
        "common/visitors.yaml",
        indoc! {r#"
            filters:
              - dimension: visitors.created_at
                operator: notInDateRange
                values:
                  - "__FROM_PARTITION_RANGE"
                  - "__TO_PARTITION_RANGE"
        "#},
    );
    assert_filter(
        &result,
        r#"("visitors".created_at < $_0_$::timestamptz OR "visitors".created_at > $_1_$::timestamptz)"#,
        &["__FROM_PARTITION_RANGE", "__TO_PARTITION_RANGE"],
    );
}

#[test]
fn test_before_date_partition_range() {
    let result = build_filter(
        "common/visitors.yaml",
        indoc! {r#"
            filters:
              - dimension: visitors.created_at
                operator: beforeDate
                values:
                  - "__TO_PARTITION_RANGE"
        "#},
    );
    assert_filter(
        &result,
        r#"("visitors".created_at < $_0_$::timestamptz)"#,
        &["__TO_PARTITION_RANGE"],
    );
}

#[test]
fn test_after_or_on_date_partition_range() {
    let result = build_filter(
        "common/visitors.yaml",
        indoc! {r#"
            filters:
              - dimension: visitors.created_at
                operator: afterOrOnDate
                values:
                  - "__FROM_PARTITION_RANGE"
        "#},
    );
    assert_filter(
        &result,
        r#"("visitors".created_at >= $_0_$::timestamptz)"#,
        &["__FROM_PARTITION_RANGE"],
    );
}
