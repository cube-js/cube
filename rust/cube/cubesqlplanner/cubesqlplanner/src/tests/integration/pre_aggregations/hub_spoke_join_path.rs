//! Cubes that connect only through a hub the pre-aggregation, or one measure's
//! share of the query, does not name. The single join tree over all members
//! reaches every cube; a hint set without the hub has no root.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use cubenativeutils::CubeError;
use indoc::indoc;

fn context() -> Result<TestContext, CubeError> {
    TestContext::new(MockSchema::from_yaml_file(
        "common/hub_spoke_join_path.yaml",
    ))
}

fn used_names(ctx: &TestContext, query: &str) -> Result<(String, Vec<String>), CubeError> {
    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query)?;
    let names = pre_aggrs
        .iter()
        .map(|pa| format!("{}.{}", pa.cube_name(), pa.name()))
        .collect();
    Ok((sql, names))
}

// The group of `ledger.amount` alone names `ledger`, `categories` and
// `entities`; it is read through the tree `hub.count` brings in.
#[test]
fn test_measure_resolved_through_other_measure_hub() -> Result<(), CubeError> {
    let ctx = context()?;
    let (sql, names) = used_names(
        &ctx,
        indoc! {"
            measures:
              - hub.count
              - ledger.count
            dimensions:
              - ledger.category_id
              - categories.name
              - entities.name
        "},
    )?;

    assert!(
        names.is_empty(),
        "no rollup stores ledger.count: {:?}",
        names
    );
    assert!(
        sql.contains("\"hub\".entity_id = \"entities\".id"),
        "entities must be joined from hub:\n{}",
        sql
    );

    Ok(())
}

#[test]
fn test_rollup_whose_only_hub_member_is_a_measure_matches() -> Result<(), CubeError> {
    let ctx = context()?;
    let (_, names) = used_names(
        &ctx,
        indoc! {"
            measures:
              - hub.count
              - ledger.amount
            dimensions:
              - ledger.category_id
              - categories.name
              - entities.name
        "},
    )?;

    assert_eq!(names, vec!["hub.income_by_category"]);

    Ok(())
}

// `hub.spoke_only_join` can't be joined, and it is compiled for every query
// touching `hub`. It must cost the query nothing but that candidate.
#[test]
fn test_unjoinable_candidate_does_not_fail_the_query() -> Result<(), CubeError> {
    let ctx = context()?;
    let (_, names) = used_names(
        &ctx,
        indoc! {"
            dimensions:
              - hub.org_id
        "},
    )?;

    assert_eq!(names, vec!["hub.hub_rollup"]);

    Ok(())
}

#[test]
fn test_unjoinable_candidate_asked_for_by_id_fails() -> Result<(), CubeError> {
    let ctx = context()?;
    let err = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - ledger.amount
            dimensions:
              - ledger.category_id
              - categories.name
              - entities.name
            pre_aggregation_id: hub.spoke_only_join
        "})
        .map(|_| ())
        .expect_err("a pre-aggregation asked for by id has to compile")
        .to_string();

    assert!(
        err.contains("Can't find join path to join 'ledger', 'categories', 'entities'"),
        "got: {}",
        err
    );

    Ok(())
}

#[test]
fn test_id_is_not_blocked_by_an_unjoinable_sibling() -> Result<(), CubeError> {
    let ctx = context()?;
    let (_, names) = used_names(
        &ctx,
        indoc! {"
            dimensions:
              - hub.org_id
            pre_aggregation_id: hub.hub_rollup
        "},
    )?;

    assert_eq!(names, vec!["hub.hub_rollup"]);

    Ok(())
}

// The query plans with its filter on `hub`, so matching has to see that filter
// to find the root the selected members have none of.
#[test]
fn test_filter_member_connects_the_query_in_matching() -> Result<(), CubeError> {
    let ctx = context()?;
    let (_, names) = used_names(
        &ctx,
        indoc! {"
            measures:
              - ledger.amount
            dimensions:
              - categories.name
              - entities.name
            filters:
              - dimension: hub.org_id
                operator: equals
                values:
                  - o1
        "},
    )?;

    assert_eq!(names, vec!["hub.with_hub_join"]);

    Ok(())
}
