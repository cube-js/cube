//! Calculated measures (`type: number` over other measures of the same cube)
//! combined with a dimension reached through a one-to-many join.
//!
//! The fan-out makes every measure pick one of two strategies: `count_distinct`
//! survives row multiplication and is aggregated in place, while `sum` has to go
//! through the keys subquery. A calculated measure is neither — it is an
//! expression over aggregates and can only be evaluated once its components have
//! been re-aggregated.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_calculated_multi_fact.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_calculated_multi_fact_tables.sql";

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

    ctx.build_sql(query).unwrap();

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

    ctx.build_sql(query).unwrap();

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

    ctx.build_sql(query).unwrap();

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

    ctx.build_sql(query).unwrap();

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

    ctx.build_sql(query).unwrap();

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

    ctx.build_sql(query).unwrap();

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

    ctx.build_sql(query).unwrap();

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

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// A calculated measure that reaches other cubes, on its own. Reaching them
/// forces a measure-join subquery, which renders measures without their
/// aggregate - so the components travel down and the ratio is formed above them.
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

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// A calculated measure whose two components need different extra cubes, so its
/// own footprint is their union - next to a measure that needs neither, which
/// puts the two on separate join trees.
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

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
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

/// Measures owned by two sibling fan-out cubes plus the cube they hang off,
/// each needing its own deduplication.
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

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// Several join-tree shapes at once: a bare aggregate on `{payments}`, a
/// three-cube measure on `{payments, rates, merchants}`, and a calculated
/// measure whose components stay inside `{payments}`.
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

    ctx.build_sql(query).unwrap();

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

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// A calculated measure that reaches another cube past its component measure.
/// The component cannot travel on its own - `MAX({rates.fx_rate})` would be left
/// with no `rates` to read from - so the measure stays whole, and whole is where
/// the measure-join subquery drops its aggregation.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "calculated measure that cannot be decomposed loses its aggregation in the measure-join subquery"]
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

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// A calculated measure that reaches another cube and has no component measure
/// to travel in its place. It must keep going through the keys subquery, or the
/// fan-out on `p1` would be counted twice.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "calculated measure that cannot be decomposed loses its aggregation in the measure-join subquery"]
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

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
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

    ctx.build_sql(query).unwrap();

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

    ctx.build_sql(query).unwrap();

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

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
