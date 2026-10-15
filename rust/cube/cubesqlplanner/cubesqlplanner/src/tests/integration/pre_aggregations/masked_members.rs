//! Masked members and pre-aggregations: a masked member read from a rollup is
//! masked over the stored column, or the rollup is skipped when it can't be.

use crate::logical_plan::PreAggregationUsage;
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use cubenativeutils::CubeError;
use indoc::indoc;

const SEED: &str = "masked_members_tables.sql";

fn context() -> Result<TestContext, CubeError> {
    TestContext::new(MockSchema::from_yaml_file(
        "common/masked_members_pre_agg.yaml",
    ))
}

fn build(query_yaml: &str) -> Result<(String, Vec<PreAggregationUsage>), CubeError> {
    context()?.build_sql_with_used_pre_aggregations(query_yaml)
}

/// Runs the query on Postgres and, when a rollup serves it, on the rollup's
/// own store as well, snapshotting each result. Empty when no database is
/// available.
async fn execute(name: &str, query_yaml: &str) -> Result<Vec<String>, CubeError> {
    let ctx = context()?;
    let (_, usages) = ctx.build_sql_with_used_pre_aggregations(query_yaml)?;
    let mut results = Vec::new();
    if let Some(result) = ctx.try_execute_pg(query_yaml, SEED).await {
        insta::assert_snapshot!(format!("{name}_pg_result"), result);
        results.push(result);
    }
    if !usages.is_empty() {
        if let Some(result) = ctx.try_execute(query_yaml, SEED).await {
            insta::assert_snapshot!(format!("{name}_cubestore_result"), result);
            results.push(result);
        }
    }
    Ok(results)
}

fn column(result: &str, name: &str) -> Vec<String> {
    if result == "(empty result)" {
        return vec![];
    }
    let mut lines = result.lines();
    let header = lines.next().unwrap_or_default();
    let index = header
        .split(" | ")
        .position(|c| c.trim() == name)
        .unwrap_or_else(|| panic!("no column {name} in:\n{result}"));
    lines
        .skip(1)
        .map(|line| {
            line.split(" | ")
                .nth(index)
                .unwrap_or_default()
                .trim()
                .to_string()
        })
        .collect()
}

fn assert_all_null(result: &str, name: &str) {
    let values = column(result, name);
    assert!(
        !values.is_empty() && values.iter().all(|v| v == "NULL"),
        "expected {name} to be masked:\n{result}"
    );
}

fn assert_nothing_counted(result: &str, name: &str) {
    assert!(
        column(result, name).iter().all(|v| v == "0" || v == "NULL"),
        "expected no rows to be counted by {name}:\n{result}"
    );
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

#[tokio::test(flavor = "multi_thread")]
async fn test_masked_dimension_is_masked_over_rollup() -> Result<(), CubeError> {
    let query = indoc! {"
        dimensions:
          - workers.gender
        measures:
          - workers.count
        order:
          - id: workers.gender
        maskedMembers:
          - member: workers.gender
    "};
    let (sql, usages) = build(query)?;

    assert_served_by_rollup(&sql, &usages);
    assert!(sql.contains(r#"(NULL) "workers__gender""#), "{sql}");
    assert!(
        !sql.contains(r#""workers__gender" "workers__gender""#),
        "{sql}"
    );
    for result in execute("masked_dimension_is_masked_over_rollup", query).await? {
        assert_all_null(&result, "workers__gender");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_member_masked_at_cube_is_masked_over_rollup() -> Result<(), CubeError> {
    let query = indoc! {"
        dimensions:
          - people.gender
        order:
          - id: people.gender
        maskedMembers:
          - member: workers.gender
    "};
    let (sql, usages) = build(query)?;

    assert_served_by_rollup(&sql, &usages);
    assert!(sql.contains(r#"(NULL) "people__gender""#), "{sql}");
    assert!(
        !sql.contains(r#""workers__gender" "people__gender""#),
        "{sql}"
    );
    for result in execute("view_member_masked_at_cube_is_masked_over_rollup", query).await? {
        assert_all_null(&result, "people__gender");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_masked_dimension_in_filter_is_masked_over_rollup() -> Result<(), CubeError> {
    let query = indoc! {"
        measures:
          - workers.count
        filters:
          - member: workers.gender
            operator: equals
            values:
              - F
        maskedMembers:
          - member: workers.gender
    "};
    let (sql, usages) = build(query)?;

    assert_served_by_rollup(&sql, &usages);
    assert!(sql.contains("(NULL) = $_0_$"), "{sql}");
    assert!(!sql.contains(r#""workers__gender" = "#), "{sql}");
    for result in execute("masked_dimension_in_filter_is_masked_over_rollup", query).await? {
        assert_nothing_counted(&result, "workers__count");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_masked_time_dimension_is_masked_over_rollup() -> Result<(), CubeError> {
    let query = indoc! {"
        measures:
          - workers.count
        time_dimensions:
          - dimension: workers.created_at
            granularity: day
        order:
          - id: workers.created_at
        maskedMembers:
          - member: workers.created_at
    "};
    let (sql, usages) = build(query)?;

    assert_served_by_rollup(&sql, &usages);
    assert!(
        sql.contains("date_trunc('day', (NULL)::timestamptz)"),
        "{sql}"
    );
    assert!(
        !sql.contains(r#""workers__created_at_day" "workers__created_at_day""#),
        "{sql}"
    );
    for result in execute("masked_time_dimension_is_masked_over_rollup", query).await? {
        assert_all_null(&result, "workers__created_at_day");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_literal_mask_is_rendered_over_rollup() -> Result<(), CubeError> {
    let query = indoc! {"
        dimensions:
          - workers.gender_literal_mask
        order:
          - id: workers.gender_literal_mask
        maskedMembers:
          - member: workers.gender_literal_mask
    "};
    let (sql, usages) = build(query)?;

    assert_served_by_rollup(&sql, &usages);
    assert!(sql.contains("'***'"), "{sql}");
    for result in execute("literal_mask_is_rendered_over_rollup", query).await? {
        assert_eq!(
            column(&result, "workers__gender_literal_mask"),
            vec!["***"],
            "{result}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_mask_over_stored_member_is_rendered_over_rollup() -> Result<(), CubeError> {
    let query = indoc! {"
        dimensions:
          - workers.gender_stored_member_mask
        order:
          - id: workers.gender_stored_member_mask
        maskedMembers:
          - member: workers.gender_stored_member_mask
    "};
    let (sql, usages) = build(query)?;

    assert_served_by_rollup(&sql, &usages);
    assert!(
        sql.contains(r#"CONCAT('***', "workers__full_name")"#),
        "{sql}"
    );
    for result in execute("mask_over_stored_member_is_rendered_over_rollup", query).await? {
        assert_eq!(
            column(&result, "workers__gender_stored_member_mask"),
            vec!["***Alice", "***Bob", "***Carol"],
            "{result}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_mask_over_source_column_skips_rollup() -> Result<(), CubeError> {
    let query = indoc! {"
        dimensions:
          - people.gender_source_mask
        order:
          - id: people.gender_source_mask
        maskedMembers:
          - member: workers.gender_source_mask
    "};
    let (sql, usages) = build(query)?;

    assert_served_by_source(&sql, &usages);
    assert!(sql.contains(r#"CONCAT('***', "workers".gender)"#), "{sql}");
    for result in execute("mask_over_source_column_skips_rollup", query).await? {
        assert_eq!(
            column(&result, "people__gender_source_mask"),
            vec!["***F", "***M"],
            "{result}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_mask_over_unstored_member_skips_rollup() -> Result<(), CubeError> {
    let query = indoc! {"
        dimensions:
          - workers.gender_unstored_member_mask
        order:
          - id: workers.gender_unstored_member_mask
        maskedMembers:
          - member: workers.gender_unstored_member_mask
    "};
    let (sql, usages) = build(query)?;

    assert_served_by_source(&sql, &usages);
    for result in execute("mask_over_unstored_member_skips_rollup", query).await? {
        assert_eq!(
            column(&result, "workers__gender_unstored_member_mask"),
            vec!["***eng", "***ops"],
            "{result}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_conditional_mask_over_stored_member_is_rendered_over_rollup() -> Result<(), CubeError>
{
    let query = indoc! {"
        dimensions:
          - workers.full_name
          - workers.gender
        order:
          - id: workers.full_name
        maskedMembers:
          - member: workers.gender
            filter:
              member: workers.full_name
              operator: equals
              values:
                - Alice
    "};
    let (sql, usages) = build(query)?;

    assert_served_by_rollup(&sql, &usages);
    assert!(
        sql.contains(r#"THEN "workers__gender" ELSE (NULL) END"#),
        "{sql}"
    );
    for result in execute(
        "conditional_mask_over_stored_member_is_rendered_over_rollup",
        query,
    )
    .await?
    {
        assert_eq!(
            column(&result, "workers__gender"),
            vec!["F", "NULL", "NULL"],
            "{result}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_conditional_mask_over_unstored_member_skips_rollup() -> Result<(), CubeError> {
    let query = indoc! {"
        dimensions:
          - workers.full_name
          - workers.gender
        order:
          - id: workers.full_name
        maskedMembers:
          - member: workers.gender
            filter:
              member: workers.department
              operator: equals
              values:
                - eng
    "};
    let (sql, usages) = build(query)?;

    assert_served_by_source(&sql, &usages);
    for result in execute("conditional_mask_over_unstored_member_skips_rollup", query).await? {
        assert_eq!(
            column(&result, "workers__gender"),
            vec!["F", "M", "NULL"],
            "{result}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stored_dimension_over_masked_member_skips_rollup() -> Result<(), CubeError> {
    let query = indoc! {"
        dimensions:
          - people.full_name_upper
        order:
          - id: people.full_name_upper
        maskedMembers:
          - member: workers.full_name
    "};
    let (sql, usages) = build(query)?;

    assert_served_by_source(&sql, &usages);
    assert!(sql.contains("UPPER((NULL))"), "{sql}");
    for result in execute("stored_dimension_over_masked_member_skips_rollup", query).await? {
        assert_all_null(&result, "people__full_name_upper");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stored_measure_over_masked_member_skips_rollup() -> Result<(), CubeError> {
    let query = indoc! {"
        measures:
          - workers.full_name_length
        maskedMembers:
          - member: workers.full_name
    "};
    let (sql, usages) = build(query)?;

    assert_served_by_source(&sql, &usages);
    assert!(sql.contains("LENGTH((NULL))"), "{sql}");
    for result in execute("stored_measure_over_masked_member_skips_rollup", query).await? {
        assert_all_null(&result, "workers__full_name_length");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stored_measure_filtered_by_masked_member_skips_rollup() -> Result<(), CubeError> {
    let query = indoc! {"
        measures:
          - workers.female_count
        maskedMembers:
          - member: workers.gender
    "};
    let (sql, usages) = build(query)?;

    assert_served_by_source(&sql, &usages);
    assert!(sql.contains("(NULL) = 'F'"), "{sql}");
    for result in execute(
        "stored_measure_filtered_by_masked_member_skips_rollup",
        query,
    )
    .await?
    {
        assert_nothing_counted(&result, "workers__female_count");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stored_segment_over_masked_member_skips_rollup() -> Result<(), CubeError> {
    let query = indoc! {"
        measures:
          - workers.count
        segments:
          - workers.females
        maskedMembers:
          - member: workers.gender
    "};
    let (sql, usages) = build(query)?;

    assert_served_by_source(&sql, &usages);
    assert!(sql.contains("(NULL) = 'F'"), "{sql}");
    for result in execute("stored_segment_over_masked_member_skips_rollup", query).await? {
        assert_nothing_counted(&result, "workers__count");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stored_dimension_over_unmasked_member_is_served_by_rollup() -> Result<(), CubeError> {
    let query = indoc! {"
        dimensions:
          - workers.full_name_upper
        order:
          - id: workers.full_name_upper
        maskedMembers:
          - member: workers.gender
    "};
    let (sql, usages) = build(query)?;

    assert_served_by_rollup(&sql, &usages);
    for result in execute(
        "stored_dimension_over_unmasked_member_is_served_by_rollup",
        query,
    )
    .await?
    {
        assert_eq!(
            column(&result, "workers__full_name_upper"),
            vec!["ALICE", "BOB", "CAROL"],
            "{result}"
        );
    }
    Ok(())
}
