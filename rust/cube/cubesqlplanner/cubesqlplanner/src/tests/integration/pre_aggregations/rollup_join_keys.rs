//! How a rollupJoin resolves the key on each side of a hop: which leg rollup carries it,
//! and which column that rollup stores it in.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use cubenativeutils::CubeError;
use indoc::indoc;

const CHAIN_QUERY: &str = indoc! {"
    dimensions:
      - cube_w.dim_w
      - cube_x.dim_x
      - cube_y.dim_y
      - cube_z.dim_z
"};

const TIME_DIMENSION_KEY_QUERY: &str = indoc! {"
    measures:
      - td_facts.total_amount
    dimensions:
      - td_dates.quarter_label
    time_dimensions:
      - dimension: td_facts.day
        granularity: day
"};

// Chain length is not a limit of its own: a hop resolves the same way at any depth, as long
// as every leg rollup declares the keys of the hops it takes part in.
#[test]
fn test_rollup_join_over_four_cube_chain() -> Result<(), CubeError> {
    let ctx = TestContext::new(MockSchema::from_yaml_file("common/rollup_join_chain.yaml"))?;

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(CHAIN_QUERY)?;

    let names = pre_aggrs
        .iter()
        .map(|pa| format!("{}.{}", pa.cube_name(), pa.name()))
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["cube_w.chain_rollup_join"]);
    for hop in [
        "\"cube_w__dim_w\" = \"cube_x__dim_w\"",
        "\"cube_x__dim_x\" = \"cube_y__dim_x\"",
        "\"cube_y__dim_y\" = \"cube_z__dim_y\"",
    ] {
        assert!(sql.contains(hop), "expected {} in:\n{}", hop, sql);
    }

    Ok(())
}

#[test]
fn test_rollup_join_names_the_hop_a_rollup_has_no_key_for() -> Result<(), CubeError> {
    let ctx = TestContext::new(MockSchema::from_yaml_file(
        "common/rollup_join_chain_missing_key.yaml",
    ))?;

    let err = ctx
        .build_sql_with_used_pre_aggregations(CHAIN_QUERY)
        .map(|_| ())
        .expect_err("A rollup missing its own join key can't resolve the hop")
        .to_string();

    // Which hop failed, which member went unmatched and which rollupJoin to fix are the only
    // things the author can act on.
    for expected in ["cube_w", "cube_x", "cube_x.dim_w", "chain_rollup_join"] {
        assert!(err.contains(expected), "expected {} in: {}", expected, err);
    }

    Ok(())
}

// A key declared as a rollup's time dimension is stored truncated, under a
// granularity-suffixed column, and the ON clause has to read that column rather than the
// bare member alias.
#[test]
fn test_rollup_join_on_time_dimension_key() -> Result<(), CubeError> {
    let ctx = TestContext::new(MockSchema::from_yaml_file(
        "common/rollup_join_time_dimension_key.yaml",
    ))?;

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(TIME_DIMENSION_KEY_QUERY)?;

    let names = pre_aggrs
        .iter()
        .map(|pa| format!("{}.{}", pa.cube_name(), pa.name()))
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["td_dates.td_rollup_join"]);
    assert!(
        sql.contains("\"td_dates__day_day\" = \"td_facts__day_day\""),
        "the join must read the columns the leg rollups store, got:\n{}",
        sql
    );

    Ok(())
}

// The join happens on the stored, truncated values, so the two sides have to be truncated
// the same way — otherwise the comparison quietly matches nothing or the wrong buckets.
#[test]
fn test_rollup_join_rejects_time_dimension_key_granularity_mismatch() -> Result<(), CubeError> {
    let ctx = TestContext::new(MockSchema::from_yaml_file(
        "common/rollup_join_time_dimension_key_mismatch.yaml",
    ))?;

    let err = ctx
        .build_sql_with_used_pre_aggregations(TIME_DIMENSION_KEY_QUERY)
        .map(|_| ())
        .expect_err("Keys truncated to different granularities can't be compared")
        .to_string();

    // Naming the rollup on each side matters: each was chosen on its own, so the author has to
    // see which pair the planner ended up with.
    for expected in [
        "td_dates.td_dates_rollup stores td_dates.day truncated to month",
        "td_facts.td_facts_rollup stores td_facts.day truncated to day",
        "td_rollup_join",
    ] {
        assert!(err.contains(expected), "expected {} in: {}", expected, err);
    }

    Ok(())
}

// Widening the lookup must not make a hop ambiguous: a rollup declaring the key plainly stays
// the one picked, so a schema that resolved before resolves to the same rollup.
#[test]
fn test_rollup_join_prefers_the_rollup_declaring_the_key_plainly() -> Result<(), CubeError> {
    let ctx = TestContext::new(MockSchema::from_yaml_file(
        "common/rollup_join_time_dimension_key_ambiguous.yaml",
    ))?;

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(indoc! {"
        measures:
          - td_facts.total_amount
        dimensions:
          - td_dates.quarter_label
          - td_facts.day
    "})?;

    let names = pre_aggrs
        .iter()
        .map(|pa| format!("{}.{}", pa.cube_name(), pa.name()))
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["td_dates.td_rollup_join"]);
    assert!(
        sql.contains("td_dates__td_dates_plain_rollup"),
        "the plainly declared key must stay the one joined on, got:\n{}",
        sql
    );

    Ok(())
}

#[test]
fn test_rollup_join_rejects_raw_key_against_truncated_one() -> Result<(), CubeError> {
    let ctx = TestContext::new(MockSchema::from_yaml_file(
        "common/rollup_join_time_dimension_key_raw_vs_truncated.yaml",
    ))?;

    let err = ctx
        .build_sql_with_used_pre_aggregations(TIME_DIMENSION_KEY_QUERY)
        .map(|_| ())
        .expect_err("A raw key and a truncated one can't be compared")
        .to_string();

    for expected in [
        "td_dates.td_dates_rollup stores td_dates.day truncated to day",
        "td_facts.td_facts_rollup stores td_facts.day untruncated",
        "td_rollup_join",
    ] {
        assert!(err.contains(expected), "expected {} in: {}", expected, err);
    }

    Ok(())
}

// Storing the key at two granularities gives two columns and no reason to pick either, so
// declaration order must not get to decide which one the join reads.
#[test]
fn test_rollup_join_rejects_key_stored_at_two_granularities() -> Result<(), CubeError> {
    let ctx = TestContext::new(MockSchema::from_yaml_file(
        "common/rollup_join_time_dimension_key_two_granularities.yaml",
    ))?;

    let err = ctx
        .build_sql_with_used_pre_aggregations(TIME_DIMENSION_KEY_QUERY)
        .map(|_| ())
        .expect_err("A key stored at two granularities is ambiguous to join on")
        .to_string();

    for expected in ["td_dates.td_dates_rollup", "td_dates.day", "day, month"] {
        assert!(err.contains(expected), "expected {} in: {}", expected, err);
    }

    Ok(())
}

// A rollup that can't be joined on is not a reason to fail the hop — only a reason to explain
// it if nothing else stands in either.
#[test]
fn test_rollup_join_skips_an_ambiguous_candidate_for_a_resolvable_one() -> Result<(), CubeError> {
    let ctx = TestContext::new(MockSchema::from_yaml_file(
        "common/rollup_join_time_dimension_key_ambiguous_candidate.yaml",
    ))?;

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(TIME_DIMENSION_KEY_QUERY)?;

    let names = pre_aggrs
        .iter()
        .map(|pa| format!("{}.{}", pa.cube_name(), pa.name()))
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["td_dates.td_rollup_join"]);
    assert!(
        sql.contains("td_dates__td_dates_rollup")
            && !sql.contains("td_dates__td_dates_wide_rollup"),
        "the hop must join through the rollup that can be joined on, got:\n{}",
        sql
    );

    Ok(())
}

// The legs store the key only plainly, so a time-dimension read of it renders
// over the key's column.
#[tokio::test(flavor = "multi_thread")]
async fn test_rollup_join_time_dimension_on_a_plainly_stored_key_matches_the_source() {
    const YAML: &str = "common/rollup_join_time_dimension_key_stored_plainly.yaml";
    const SEED: &str = "rollup_join_time_dimension_key_tables.sql";
    let ctx = |pre_aggs: &[&str]| {
        TestContext::new(MockSchema::from_yaml_file(YAML).only_pre_aggregations(pre_aggs)).unwrap()
    };
    let with_date_range = indoc! {"
        measures:
          - td_facts.total_amount
        dimensions:
          - td_dates.quarter_label
        time_dimensions:
          - dimension: td_facts.day
            granularity: day
            dateRange:
              - '2026-01-01'
              - '2026-03-31'
    "};
    for (name, query) in [
        ("plainly_stored_key", TIME_DIMENSION_KEY_QUERY),
        ("plainly_stored_key_with_date_range", with_date_range),
    ] {
        for timezone in ["America/Los_Angeles", "Asia/Kamchatka"] {
            let query = format!("{query}timezone: {timezone}\n");
            let served = ctx(&["td_rollup_join", "td_dates_rollup", "td_facts_rollup"]);
            let (sql, usages) = served.build_sql_with_used_pre_aggregations(&query).unwrap();
            assert!(
                usages.iter().any(|u| u.name() == "td_rollup_join"),
                "expected the rollupJoin to serve the query; SQL:\n{sql}"
            );
            let rollup = served.try_execute_pg(&query, SEED).await;
            let source = ctx(&[]).try_execute_pg(&query, SEED).await;
            assert_eq!(rollup, source, "{timezone}; SQL:\n{sql}");
        }
        let served = ctx(&["td_rollup_join", "td_dates_rollup", "td_facts_rollup"]);
        let (sql, usages) = served.build_sql_with_used_pre_aggregations(query).unwrap();
        assert!(
            usages.iter().any(|u| u.name() == "td_rollup_join"),
            "expected the rollupJoin to serve the query; SQL:\n{sql}"
        );
        let rollup = served.try_execute_pg(query, SEED).await;
        let source = ctx(&[]).try_execute_pg(query, SEED).await;
        assert_eq!(rollup, source, "{sql}");
        if let Some(rows) = served.try_execute(query, SEED).await {
            insta::assert_snapshot!(name, rows);
        }
    }
}

// Only bounds on the key's grain can be checked against a truncated column.
#[tokio::test(flavor = "multi_thread")]
async fn test_rollup_join_date_filter_on_a_truncated_key_needs_bounds_on_its_grain() {
    const YAML: &str = "common/rollup_join_time_dimension_key_truncated.yaml";
    const SEED: &str = "rollup_join_time_dimension_key_tables.sql";
    let ctx = |pre_aggs: &[&str]| {
        TestContext::new(MockSchema::from_yaml_file(YAML).only_pre_aggregations(pre_aggs)).unwrap()
    };
    let query = |operator: &str, values: &str| {
        format!(
            "measures:\n  - td_facts.total_amount\ndimensions:\n  - td_dates.quarter_label\n\
             filters:\n  - member: td_facts.day\n    operator: {operator}\n    values: {values}\n"
        )
    };
    for (operator, values, served_by_rollup) in [
        ("inDateRange", "['2026-01-01', '2026-03-31']", true),
        ("afterDate", "['2026-01-01']", true),
        ("afterDate", "['2026-01-01T12:00:00']", false),
        (
            "inDateRange",
            "['2026-01-01T12:00:00', '2026-03-31']",
            false,
        ),
    ] {
        let query = query(operator, values);
        let served = ctx(&["td_rollup_join", "td_dates_rollup", "td_facts_rollup"]);
        let (sql, usages) = served.build_sql_with_used_pre_aggregations(&query).unwrap();
        assert_eq!(
            usages.iter().any(|u| u.name() == "td_rollup_join"),
            served_by_rollup,
            "{operator} {values}; SQL:\n{sql}"
        );
        let rollup = served.try_execute_pg(&query, SEED).await;
        let source = ctx(&[]).try_execute_pg(&query, SEED).await;
        assert_eq!(rollup, source, "{operator} {values}; SQL:\n{sql}");
    }
}

// The legs store the key only plainly, so a date filter on it reads the key's
// column whatever grain the rollupJoin declares for it.
#[tokio::test(flavor = "multi_thread")]
async fn test_rollup_join_date_filter_on_a_plainly_stored_key_reads_its_column() {
    const YAML: &str = "common/rollup_join_time_dimension_key_stored_plainly.yaml";
    const SEED: &str = "rollup_join_time_dimension_key_tables.sql";
    let ctx = |pre_aggs: &[&str]| {
        TestContext::new(MockSchema::from_yaml_file(YAML).only_pre_aggregations(pre_aggs)).unwrap()
    };
    for timezone in ["UTC", "America/Los_Angeles", "Asia/Kamchatka"] {
        for (operator, values) in [
            ("inDateRange", "['2026-01-01', '2026-03-31']"),
            ("afterDate", "['2026-01-01']"),
        ] {
            let query = format!(
                "measures:\n  - td_facts.total_amount\ndimensions:\n  - td_dates.quarter_label\n\
                 filters:\n  - member: td_facts.day\n    operator: {operator}\n    values: {values}\n\
                 timezone: {timezone}\n"
            );
            let served = ctx(&["td_rollup_join", "td_dates_rollup", "td_facts_rollup"]);
            let (sql, usages) = served.build_sql_with_used_pre_aggregations(&query).unwrap();
            assert!(
                usages.iter().any(|u| u.name() == "td_rollup_join"),
                "expected the rollupJoin to serve the query; SQL:\n{sql}"
            );
            let rollup = served.try_execute_pg(&query, SEED).await;
            let source = ctx(&[]).try_execute_pg(&query, SEED).await;
            assert_eq!(
                rollup, source,
                "{timezone} {operator} {values}; SQL:\n{sql}"
            );
        }
    }
}
