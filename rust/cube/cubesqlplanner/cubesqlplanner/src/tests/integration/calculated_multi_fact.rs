//! Measures of one cube that need different join trees, combined with a
//! dimension reached through a one-to-many join.
//!
//! The fan-out makes every measure pick a strategy: `count_distinct` survives
//! row multiplication and is aggregated in place, while `sum` has to go through
//! the keys subquery that deduplicates it. The key cube says which primary keys
//! to deduplicate on and the join tree says which joins to build, so measures
//! sharing a cube can still need a subquery each.
//!
//! A calculated measure (`type: number`) carries no aggregate of its own. It
//! rides along wherever its own tree takes it, but it cannot survive the
//! measure subquery that reaching another cube forces - there is nothing to
//! re-apply above it - and those shapes are refused while planning.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_calculated_multi_fact.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_calculated_multi_fact_tables.sql";

// One per (key cube, join tree), each rendering its key set as a `keys`
// subselect. Counting them states which strategy a test holds the planner to,
// which the result snapshot alone cannot.
fn keys_subquery_count(sql: &str) -> usize {
    sql.matches(r#" AS "keys""#).count()
}

fn projects_column_for(sql: &str, measure: &str) -> bool {
    sql.contains(&format!(r#""{}""#, measure.replace('.', "__")))
}

fn expect_no_own_aggregate_error(ctx: &TestContext, query: &str, measure: &str) {
    let result = ctx.build_sql(query);
    assert!(result.is_err(), "expected the query to be refused");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("has no aggregate of its own"),
        "Error should explain the missing aggregate, got: {err_msg}"
    );
    assert!(
        err_msg.contains(measure),
        "Error should name {measure}, got: {err_msg}"
    );
}

/// A calculated measure alone: its components are aggregated over the
/// deduplicated key set, so the fan-out on `p1` does not skew the ratio.
#[tokio::test(flavor = "multi_thread")]
async fn test_calculated_measure_alone_by_fan_out_dimension() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - payments.success_rate
        dimensions:
          - payment_meta.value
        order:
          - id: payment_meta.value
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(keys_subquery_count(&sql), 1, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// The calculated measure's components requested directly, to pin down the
/// numbers the ratio is built from.
#[tokio::test(flavor = "multi_thread")]
async fn test_calculated_measure_components_by_fan_out_dimension() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - payments.success_count
          - payments.count
        dimensions:
          - payment_meta.value
        order:
          - id: payment_meta.value
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(keys_subquery_count(&sql), 0, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// A measure that needs the `rates` join next to one that does not. Both are on
/// `payments`, and `count_distinct` keeps its own value correct under the
/// fan-out while `converted_value` is deduplicated through the keys subquery.
#[tokio::test(flavor = "multi_thread")]
async fn test_joined_measure_with_distinct_count_by_fan_out_dimension() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - payments.converted_value
          - payments.count
        dimensions:
          - payment_meta.value
        order:
          - id: payment_meta.value
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(keys_subquery_count(&sql), 1, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// Two plain `sum` measures of the same cube, one of which needs a wider join.
/// No calculated measure involved - this is the join grouping on its own.
#[tokio::test(flavor = "multi_thread")]
async fn test_two_sums_of_one_cube_needing_different_joins() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - payments.total_amount
          - payments.converted_value
        dimensions:
          - payment_meta.value
        order:
          - id: payment_meta.value
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(keys_subquery_count(&sql), 2, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// A calculated measure alongside a measure that needs a wider join. Both sit on
/// `payments`, so they share a key cube, but they need different join trees and
/// therefore land in separate subqueries.
#[tokio::test(flavor = "multi_thread")]
async fn test_calculated_measure_with_joined_measure() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - payments.success_rate
          - payments.converted_value
        dimensions:
          - payment_meta.value
        order:
          - id: payment_meta.value
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(keys_subquery_count(&sql), 2, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// Same shape, but the calculated measure is built from `sum` components, which
/// do not survive row multiplication on their own.
#[tokio::test(flavor = "multi_thread")]
async fn test_calculated_measure_over_sums_with_joined_measure() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - payments.success_amount_rate
          - payments.converted_value
        dimensions:
          - payment_meta.value
        order:
          - id: payment_meta.value
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(keys_subquery_count(&sql), 2, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// Three measures of one cube needing three nested join trees: `{payments}`,
/// `{payments, rates}` and `{payments, rates, merchants}`.
#[tokio::test(flavor = "multi_thread")]
async fn test_three_nested_join_trees_of_one_cube() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - payments.total_amount
          - payments.converted_value
          - payments.net_value
        dimensions:
          - payment_meta.value
        order:
          - id: payment_meta.value
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(keys_subquery_count(&sql), 3, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// Two lookup cubes that are siblings rather than nested: `{payments, rates}`
/// against `{payments, merchants}`, neither containing the other.
#[tokio::test(flavor = "multi_thread")]
async fn test_two_sibling_lookup_join_trees_of_one_cube() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - payments.converted_value
          - payments.commissioned_value
        dimensions:
          - payment_meta.value
        order:
          - id: payment_meta.value
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(keys_subquery_count(&sql), 2, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// A calculated measure that reaches another cube, on its own. Reaching it
/// forces the measure subquery, which strips the aggregates its components
/// carry and leaves the expression with nothing to re-apply.
#[tokio::test(flavor = "multi_thread")]
async fn test_calculated_measure_reaching_other_cubes_alone() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - payments.rate_vs_commission
        dimensions:
          - payment_meta.value
        order:
          - id: payment_meta.value
    "};

    expect_no_own_aggregate_error(&ctx, query, "payments.rate_vs_commission");
}

/// The same measure next to one that needs neither of the cubes it reaches, so
/// the two land on separate join trees. The calculated leg is refused all the
/// same - splitting the query does not give the expression an aggregate.
#[tokio::test(flavor = "multi_thread")]
async fn test_calculated_measure_over_components_with_different_joins() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - payments.rate_vs_commission
          - payments.total_amount
        dimensions:
          - payment_meta.value
        order:
          - id: payment_meta.value
    "};

    expect_no_own_aggregate_error(&ctx, query, "payments.rate_vs_commission");
}

/// A star: the dimension pulls in one one-to-many branch and the measure pulls
/// in another. A `sum` on `payments` filtered by a `payment_tags` value has no
/// well-defined value once a payment can carry several tags, so it is refused
/// rather than silently double-counted.
#[tokio::test(flavor = "multi_thread")]
async fn test_star_with_two_fan_out_branches_is_rejected() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - payments.total_amount
          - payments.tagged_amount
        dimensions:
          - payment_meta.value
        order:
          - id: payment_meta.value
    "};

    let result = ctx.build_sql(query);
    assert!(
        result.is_err(),
        "A measure reaching a second fan-out branch should not plan"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("lead to row multiplication"),
        "Error should report row multiplication, got: {err_msg}"
    );
    assert!(
        err_msg.contains("payments.tagged_amount"),
        "Error should name the offending measure, got: {err_msg}"
    );
}

/// Measures owned by two sibling fan-out cubes plus the cube they hang off. Each
/// gets its own leaf query joined by `FullKeyAggregate`, but none of them is
/// multiplied - the two fan-out cubes sit on the `many` side of their own joins,
/// and `total_amount`'s tree stops at `{payments}` because the dimension does -
/// so nothing takes the keys path.
#[tokio::test(flavor = "multi_thread")]
async fn test_measures_of_two_sibling_fan_out_cubes() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - payment_meta.count
          - payment_tags.count
          - payments.total_amount
        dimensions:
          - payments.status
        order:
          - id: payments.status
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(keys_subquery_count(&sql), 0, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// Several join-tree shapes at once: a bare aggregate on `{payments}`, a
/// three-cube measure on `{payments, rates, merchants}`, and a calculated
/// measure whose components stay inside `{payments}`. Two legs, not three - the
/// grouping is by join tree, so the calculated measure shares one with the bare
/// aggregate rather than getting its own.
#[tokio::test(flavor = "multi_thread")]
async fn test_several_join_tree_shapes_in_one_query() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - payments.total_amount
          - payments.net_value
          - payments.success_rate
        dimensions:
          - payment_meta.value
        order:
          - id: payment_meta.value
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(keys_subquery_count(&sql), 2, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// A calculated measure whose dependencies are all measures, but whose sql also
/// reads its own cube's table. Letting the component leave would strand that
/// read in a select the cube is not joined into. Paired with a measure of
/// another cube so the query is planned through classification, and grouped by a
/// dimension that multiplies nothing, so the measure renders whole and the
/// result is checkable.
#[tokio::test(flavor = "multi_thread")]
async fn test_calculated_measure_reading_its_own_cube_directly() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - payments.converted_per_max_amount
          - payment_meta.count
        dimensions:
          - payments.status
        order:
          - id: payments.status
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(keys_subquery_count(&sql), 0, "sql: {sql}");
    assert!(
        projects_column_for(&sql, "payments.converted_per_max_amount"),
        "sql: {sql}"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// A calculated measure reaching another cube past its component measure, so the
/// aggregate around `MAX({rates.fx_rate})` is the measure's own.
#[tokio::test(flavor = "multi_thread")]
async fn test_calculated_measure_reaching_past_its_components() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - payments.amount_over_fx
        dimensions:
          - payment_meta.value
        order:
          - id: payment_meta.value
    "};

    expect_no_own_aggregate_error(&ctx, query, "payments.amount_over_fx");
}

/// A calculated measure whose components resolve to join trees rooted at
/// different cubes. Evaluating it in one place and splitting it apart give
/// different answers, so it is refused rather than answered either way.
#[tokio::test(flavor = "multi_thread")]
async fn test_calculated_measure_over_components_rooted_at_different_cubes() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - payments.gold_share
        dimensions:
          - payment_meta.value
        order:
          - id: payment_meta.value
    "};

    expect_no_own_aggregate_error(&ctx, query, "payments.gold_share");
}

/// A dimension of the joined cube puts the calculated measure and the one that
/// reaches into the same group, so the measure subquery is built for a measure
/// that never asked for it.
#[tokio::test(flavor = "multi_thread")]
async fn test_calculated_measure_pulled_into_a_shared_measure_subquery() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - payments.success_rate
          - payments.converted_value
        dimensions:
          - payment_meta.value
          - rates.currency
    "};

    expect_no_own_aggregate_error(&ctx, query, "payments.success_rate");
}

/// The same shape a rollup stores. The measure subquery is never rendered, so
/// the query is answered rather than refused - which is why the check belongs
/// where the subquery is built and not where the plan is.
#[tokio::test(flavor = "multi_thread")]
async fn test_refused_shape_is_answered_from_a_rollup() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - payments.max_fx_rate
        dimensions:
          - payment_meta.value
    "};

    let (_sql, usages) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();
    let names: Vec<&str> = usages.iter().map(|u| u.name().as_str()).collect();
    assert_eq!(names, vec!["max_fx_by_meta_value"]);
}

/// A calculated measure reaching another cube with no component measure at all -
/// its aggregate is written by hand over the joined cube's column.
#[tokio::test(flavor = "multi_thread")]
async fn test_calculated_measure_without_component_measures() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - payments.max_fx_rate
          - payments.total_amount
        dimensions:
          - payment_meta.value
        order:
          - id: payment_meta.value
    "};

    expect_no_own_aggregate_error(&ctx, query, "payments.max_fx_rate");
}

/// A calculated measure that reaches another cube but is not multiplied. It is
/// read off a leaf-measure query that keeps its aggregate, so it needs no
/// decomposition and must be evaluated whole: its components root at different
/// cubes, and splitting them would divide `gold_amount` by a `total_amount`
/// taken over a different set of rows than its own leg sees.
///
/// Evaluated whole, both sides of the ratio see the `customers`-joined rows:
/// `SUCCESS` is 100 * 300 / 700, p5 having no customer, while the sibling
/// `payments.total_amount` column reads 1200 over all rows. The two columns are
/// not meant to reconcile - the ratio is taken within its own leg.
#[tokio::test(flavor = "multi_thread")]
async fn test_calculated_measure_reaching_other_cubes_without_multiplication() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - payments.gold_share
          - payments.total_amount
        dimensions:
          - payments.status
        order:
          - id: payments.status
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(keys_subquery_count(&sql), 0, "sql: {sql}");
    assert!(
        projects_column_for(&sql, "payments.gold_share"),
        "sql: {sql}"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// Split groups whose trees root at different cubes: `total_amount` roots at
/// `payments`, while `gold_amount` reaches `customers`, which owns the join and
/// therefore becomes the root. Customerless payments are unreachable from the
/// second root, so that leg reports only `A` and `B` while the first also
/// reports `C`; the result must carry `C` with a null `gold_amount`.
#[tokio::test(flavor = "multi_thread")]
async fn test_split_groups_rooted_at_different_cubes() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - payments.total_amount
          - payments.gold_amount
        dimensions:
          - payment_meta.value
        order:
          - id: payment_meta.value
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(keys_subquery_count(&sql), 2, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// Without the fan-out dimension there is a single join group, so the same pair
/// of measures plans and runs.
#[tokio::test(flavor = "multi_thread")]
async fn test_calculated_measure_with_joined_measure_without_fan_out() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - payments.success_rate
          - payments.converted_value
        dimensions:
          - payments.status
        order:
          - id: payments.status
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(keys_subquery_count(&sql), 0, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
