//! Masked members and pre-aggregations.
//!
//! A rollup stores a member's raw value, so a masked member read from it must
//! be masked on top of the stored column. A mask that reads something the
//! rollup doesn't store can't be rendered there, so such a rollup must not be
//! used at all: the query goes to the source, where the mask renders as usual.

use crate::logical_plan::PreAggregationUsage;
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use cubenativeutils::CubeError;
use indoc::indoc;

fn build(query_yaml: &str) -> Result<(String, Vec<PreAggregationUsage>), CubeError> {
    TestContext::new(MockSchema::from_yaml_file(
        "common/masked_members_pre_agg.yaml",
    ))?
    .build_sql_with_used_pre_aggregations(query_yaml)
}

fn assert_served_by_rollup(sql: &str, usages: &[PreAggregationUsage]) {
    assert_eq!(
        usages.iter().map(|u| u.name().clone()).collect::<Vec<_>>(),
        vec!["by_gender".to_string()],
        "expected the rollup to serve the query. Generated SQL:\n{sql}"
    );
}

fn assert_served_by_source(sql: &str, usages: &[PreAggregationUsage]) {
    assert!(
        usages.is_empty(),
        "the rollup can't render the mask, so it must not be used. Generated SQL:\n{sql}"
    );
}

#[test]
fn test_masked_dimension_is_masked_over_rollup() -> Result<(), CubeError> {
    let (sql, usages) = build(indoc! {"
        dimensions:
          - workers.gender
        measures:
          - workers.count
        maskedMembers:
          - member: workers.gender
    "})?;

    assert_served_by_rollup(&sql, &usages);
    assert!(sql.contains(r#"(NULL) "workers__gender""#), "{sql}");
    assert!(
        !sql.contains(r#""workers__gender" "workers__gender""#),
        "{sql}"
    );
    Ok(())
}

#[test]
fn test_view_member_masked_at_cube_is_masked_over_rollup() -> Result<(), CubeError> {
    let (sql, usages) = build(indoc! {"
        dimensions:
          - people.gender
        maskedMembers:
          - member: workers.gender
    "})?;

    assert_served_by_rollup(&sql, &usages);
    assert!(sql.contains(r#"(NULL) "people__gender""#), "{sql}");
    assert!(
        !sql.contains(r#""workers__gender" "people__gender""#),
        "{sql}"
    );
    Ok(())
}

#[test]
fn test_masked_dimension_in_filter_is_masked_over_rollup() -> Result<(), CubeError> {
    let (sql, usages) = build(indoc! {"
        measures:
          - workers.count
        filters:
          - member: workers.gender
            operator: equals
            values:
              - F
        maskedMembers:
          - member: workers.gender
    "})?;

    assert_served_by_rollup(&sql, &usages);
    assert!(sql.contains("(NULL) = $_0_$"), "{sql}");
    assert!(!sql.contains(r#""workers__gender" = "#), "{sql}");
    Ok(())
}

#[test]
fn test_masked_time_dimension_is_masked_over_rollup() -> Result<(), CubeError> {
    let (sql, usages) = build(indoc! {"
        measures:
          - workers.count
        time_dimensions:
          - dimension: workers.created_at
            granularity: day
        maskedMembers:
          - member: workers.created_at
    "})?;

    assert_served_by_rollup(&sql, &usages);
    assert!(sql.contains("date_trunc('day', (NULL))"), "{sql}");
    assert!(
        !sql.contains(r#""workers__created_at_day" "workers__created_at_day""#),
        "{sql}"
    );
    Ok(())
}

#[test]
fn test_literal_mask_is_rendered_over_rollup() -> Result<(), CubeError> {
    let (sql, usages) = build(indoc! {"
        dimensions:
          - workers.gender_literal_mask
        maskedMembers:
          - member: workers.gender_literal_mask
    "})?;

    assert_served_by_rollup(&sql, &usages);
    assert!(sql.contains("'***'"), "{sql}");
    Ok(())
}

#[test]
fn test_mask_over_stored_member_is_rendered_over_rollup() -> Result<(), CubeError> {
    let (sql, usages) = build(indoc! {"
        dimensions:
          - workers.gender_stored_member_mask
        maskedMembers:
          - member: workers.gender_stored_member_mask
    "})?;

    assert_served_by_rollup(&sql, &usages);
    assert!(
        sql.contains(r#"CONCAT('***', "workers__full_name")"#),
        "{sql}"
    );
    Ok(())
}

#[test]
fn test_mask_over_source_column_skips_rollup() -> Result<(), CubeError> {
    let (sql, usages) = build(indoc! {"
        dimensions:
          - people.gender_source_mask
        maskedMembers:
          - member: workers.gender_source_mask
    "})?;

    assert_served_by_source(&sql, &usages);
    assert!(sql.contains(r#"CONCAT('***', "workers".gender)"#), "{sql}");
    Ok(())
}

#[test]
fn test_mask_over_unstored_member_skips_rollup() -> Result<(), CubeError> {
    let (sql, usages) = build(indoc! {"
        measures:
          - workers.count
        filters:
          - member: workers.gender_unstored_member_mask
            operator: set
        maskedMembers:
          - member: workers.gender_unstored_member_mask
    "})?;

    assert_served_by_source(&sql, &usages);
    Ok(())
}

#[test]
fn test_conditional_mask_over_stored_member_is_rendered_over_rollup() -> Result<(), CubeError> {
    let (sql, usages) = build(indoc! {"
        dimensions:
          - workers.gender
        maskedMembers:
          - member: workers.gender
            filter:
              member: workers.full_name
              operator: equals
              values:
                - x
    "})?;

    assert_served_by_rollup(&sql, &usages);
    assert!(
        sql.contains(r#"THEN "workers__gender" ELSE (NULL) END"#),
        "{sql}"
    );
    Ok(())
}

#[test]
fn test_conditional_mask_over_unstored_member_skips_rollup() -> Result<(), CubeError> {
    let (sql, usages) = build(indoc! {"
        dimensions:
          - workers.gender
        maskedMembers:
          - member: workers.gender
            filter:
              member: workers.department
              operator: equals
              values:
                - x
    "})?;

    assert_served_by_source(&sql, &usages);
    Ok(())
}

#[test]
fn test_stored_dimension_over_masked_member_skips_rollup() -> Result<(), CubeError> {
    let (sql, usages) = build(indoc! {"
        dimensions:
          - people.full_name_upper
        maskedMembers:
          - member: workers.full_name
    "})?;

    assert_served_by_source(&sql, &usages);
    assert!(sql.contains("UPPER((NULL))"), "{sql}");
    Ok(())
}

#[test]
fn test_stored_measure_over_masked_member_skips_rollup() -> Result<(), CubeError> {
    let (sql, usages) = build(indoc! {"
        measures:
          - workers.full_name_length
        maskedMembers:
          - member: workers.full_name
    "})?;

    assert_served_by_source(&sql, &usages);
    assert!(sql.contains("LENGTH((NULL))"), "{sql}");
    Ok(())
}

#[test]
fn test_stored_measure_filtered_by_masked_member_skips_rollup() -> Result<(), CubeError> {
    let (sql, usages) = build(indoc! {"
        measures:
          - workers.female_count
        maskedMembers:
          - member: workers.gender
    "})?;

    assert_served_by_source(&sql, &usages);
    assert!(sql.contains("(NULL) = 'F'"), "{sql}");
    Ok(())
}

#[test]
fn test_stored_segment_over_masked_member_skips_rollup() -> Result<(), CubeError> {
    let (sql, usages) = build(indoc! {"
        measures:
          - workers.count
        segments:
          - workers.females
        maskedMembers:
          - member: workers.gender
    "})?;

    assert_served_by_source(&sql, &usages);
    assert!(sql.contains("(NULL) = 'F'"), "{sql}");
    Ok(())
}

#[test]
fn test_stored_dimension_over_unmasked_member_is_served_by_rollup() -> Result<(), CubeError> {
    let (sql, usages) = build(indoc! {"
        dimensions:
          - workers.full_name_upper
        maskedMembers:
          - member: workers.gender
    "})?;

    assert_served_by_rollup(&sql, &usages);
    Ok(())
}
