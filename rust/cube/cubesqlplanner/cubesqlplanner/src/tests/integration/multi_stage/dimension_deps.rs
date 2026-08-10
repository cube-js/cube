//! Multi-stage members whose SQL reads a plain dimension of their own cube.
//!
//! Such a member is rendered against the CTE that computes its aggregated
//! dependency, so the dimension has to be one of that CTE's columns. When no
//! grain supplies it, rendering would fall back to the dimension's own cube
//! alias — a table name nothing in the CTE's FROM brings into scope — and the
//! planner reports the member instead.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_stage.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_stage_tables.sql";

fn month_query(measure: &str) -> String {
    format!(
        indoc! {r#"
            measures:
              - orders.{}
            time_dimensions:
              - dimension: orders.created_at
                granularity: month
                dateRange:
                  - "2024-01-01"
                  - "2024-03-31"
        "#},
        measure
    )
}

fn expect_error(measure: &str) -> String {
    let ctx = create_context();
    match ctx.build_sql(&month_query(measure)) {
        Ok(sql) => panic!("Expected a planning error for orders.{measure}, got SQL:\n{sql}"),
        Err(e) => e.to_string(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_undeclared_time_dimension_read_is_reported() {
    let message = expect_error("amount_first_half_of_month");

    assert!(
        message.contains("orders.amount_first_half_of_month")
            && message.contains("orders.created_at"),
        "The error must name both the member and the dimension it reads:\n{}",
        message
    );
    assert!(
        message.contains("grain.include"),
        "The error must point at the declaration that fixes the model:\n{}",
        message
    );
}

/// The reading member consumes a time-shifted multi-stage measure, so its own
/// grain is settled one stage above the leaf that would have to carry the
/// dimension.
#[tokio::test(flavor = "multi_thread")]
async fn test_undeclared_read_by_a_consumer_of_a_shifted_measure_is_reported() {
    let message = expect_error("amount_prev_month_first_half");

    assert!(
        message.contains("orders.amount_prev_month_first_half")
            && message.contains("orders.created_at"),
        "The error must name both the member and the dimension it reads:\n{}",
        message
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_undeclared_string_dimension_read_is_reported() {
    let message = expect_error("completed_amount_multi_stage");

    assert!(
        message.contains("orders.completed_amount_multi_stage")
            && message.contains("orders.status"),
        "The error must name both the member and the dimension it reads:\n{}",
        message
    );
}

/// Declaring the grain the sql needs is what makes the model plan: the leaf
/// carries the raw dimension, and the measure reads it as a CTE column.
#[tokio::test(flavor = "multi_thread")]
async fn test_declared_grain_plans_and_reads_the_cte_column() {
    let ctx = create_context();

    let query = month_query("amount_first_half_of_month_with_grain");
    let sql = ctx.build_sql(&query).unwrap();

    // The trailing quote is what keeps this from matching `orders__created_at_month`.
    assert!(
        sql.contains("\"orders__created_at\""),
        "Expected the raw time dimension to be materialized as a CTE column:\n{}",
        sql
    );
    assert!(
        !sql.contains("EXTRACT(DAY FROM \"orders\".created_at)"),
        "The measure must not read the dimension off the cube alias:\n{}",
        sql
    );

    if let Some(result) = ctx.try_execute_pg(&query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// The declared grain widens the leaf only — the query still reports months.
#[tokio::test(flavor = "multi_thread")]
async fn test_declared_grain_keeps_the_query_grain() {
    let ctx = create_context();

    let sql = ctx
        .build_sql(&month_query("amount_first_half_of_month_with_grain"))
        .unwrap();

    let (_, final_select) = sql
        .rsplit_once("\nSELECT")
        .expect("the plan must end in a top-level SELECT");
    assert!(
        !final_select.contains("\"orders__created_at\""),
        "The raw time dimension must not reach the query projection:\n{}",
        sql
    );
    assert!(
        final_select.contains("orders__created_at_month"),
        "Expected the month grain in the query projection:\n{}",
        sql
    );
}

/// A dimension the stage's own `reduce_by` drops is still reachable from the
/// keys side, so reading it must not be reported.
///
/// The values are the whole month against the `completed` row rather than the
/// `completed` total: `reduce_by` collapses the measure to a grain without
/// `status`, and the CASE then reads the status of the broadcast row. That is
/// what this shape means, not a defect in the reachability check.
#[tokio::test(flavor = "multi_thread")]
async fn test_dimension_reachable_from_the_keys_side_is_not_reported() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_reduce_status_reading_status
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        order:
          - id: orders.status
          - id: orders.created_at
    "#};

    let sql = ctx.build_sql(query).unwrap();

    assert!(
        sql.contains("\"fk_aggregate_keys\".\"orders__status\" = 'completed'"),
        "Expected the measure to read the dimension off the keys side:\n{}",
        sql
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// A `grain.include` on the measure below widens *that* measure's leaf, not the
/// columns its own CTE projects, so a member reading the dimension one stage up
/// still has nowhere to read it from. Pinned because the difference between
/// widening a leaf and projecting a column is easy to mistake for an oversight.
#[tokio::test(flavor = "multi_thread")]
async fn test_grain_declared_by_a_child_does_not_satisfy_the_parent() {
    let message = expect_error("amount_reading_a_child_leaf_grain");

    assert!(
        message.contains("orders.amount_reading_a_child_leaf_grain")
            && message.contains("orders.status"),
        "The error must name both the member and the dimension it reads:\n{}",
        message
    );
}

/// A dimension the query itself groups by is part of the stage grain, so it
/// resolves without any declaration.
#[tokio::test(flavor = "multi_thread")]
async fn test_dimension_in_the_query_grain_is_not_reported() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.completed_amount_multi_stage
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        order:
          - id: orders.status
          - id: orders.created_at
    "#};

    let sql = ctx.build_sql(query).unwrap();

    assert!(
        sql.contains("\"fk_aggregate\".\"orders__status\" = 'completed'"),
        "Expected the measure to read the dimension off the stage grain:\n{}",
        sql
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// A dimension built out of another one is no column of the CTE itself, but
/// renders from the column its own sql reads — the reachability walk has to
/// follow that, the way reference resolution does.
#[tokio::test(flavor = "multi_thread")]
async fn test_dimension_derived_from_a_grain_dimension_is_not_reported() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_by_category_label
        dimensions:
          - orders.category
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        order:
          - id: orders.category
          - id: orders.created_at
    "#};

    let sql = ctx.build_sql(query).unwrap();

    // The label's own branch condition, which only the derived dimension emits —
    // the bare column appears in the projection either way.
    assert!(
        sql.contains("\"fk_aggregate\".\"orders__category\" = 'books'"),
        "Expected the derived dimension to render from the grain column:\n{}",
        sql
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// The read resolves through a member, but the same expression also reaches a
/// raw cube column, and no CTE column can stand in for that.
#[tokio::test(flavor = "multi_thread")]
async fn test_read_mixing_a_member_and_a_raw_column_is_reported() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_by_status_and_category
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        order:
          - id: orders.status
          - id: orders.created_at
    "#};

    match ctx.build_sql(query) {
        Ok(sql) => panic!("Expected a planning error, got SQL:\n{sql}"),
        Err(e) => assert!(
            e.to_string().contains("orders.status_and_category"),
            "The error must name the dimension that reads the raw column:\n{}",
            e
        ),
    }
}

/// `drill_filters` never reach the rendered SQL, so a dimension named only
/// there puts no column requirement on the CTE.
#[tokio::test(flavor = "multi_thread")]
async fn test_dimension_read_only_by_drill_filters_is_not_reported() {
    let ctx = create_context();

    let query = month_query("amount_with_drill_filters");
    let sql = ctx.build_sql(&query).unwrap();

    assert!(
        !sql.contains("\"orders\".category"),
        "The drill filter must not put a cube-qualified column into the CTE:\n{}",
        sql
    );

    if let Some(result) = ctx.try_execute_pg(&query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// A mask is rendered only for members the security context masks, so a
/// dimension read from an inactive mask puts no column requirement either.
#[tokio::test(flavor = "multi_thread")]
async fn test_dimension_read_only_by_an_inactive_mask_is_not_reported() {
    let ctx = create_context();

    let query = month_query("amount_with_masked_dimension_read");
    let sql = ctx.build_sql(&query).unwrap();

    assert!(
        !sql.contains("\"orders\".category"),
        "The inactive mask must not put a cube-qualified column into the CTE:\n{}",
        sql
    );
}

/// The exclusion has to hold for the cube refs of a mask as well: a dimension
/// whose own sql is clean must not be rejected because its mask names a raw
/// column.
#[tokio::test(flavor = "multi_thread")]
async fn test_raw_column_read_only_by_a_mask_is_not_reported() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_by_status_upper_masked
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        order:
          - id: orders.status
          - id: orders.created_at
    "#};

    let sql = ctx.build_sql(query).unwrap();

    assert!(
        sql.contains("UPPER(\"fk_aggregate\".\"orders__status\")"),
        "Expected the dimension to render from the grain column:\n{}",
        sql
    );
    assert!(
        !sql.contains("\"orders\".category"),
        "The inactive mask must not put a cube-qualified column into the CTE:\n{}",
        sql
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// The exclusion is conditional on the mask not being applied. Once the query
/// masks the member, the mask does reach the SQL, and the dimension it reads has
/// to be reachable like any other.
#[tokio::test(flavor = "multi_thread")]
async fn test_active_mask_reading_an_out_of_grain_dimension_is_reported() {
    let ctx = create_context();

    let query = format!(
        "{}{}",
        month_query("amount_with_masked_dimension_read"),
        indoc! {"
            maskedMembers:
              - member: orders.amount_with_masked_dimension_read
        "},
    );

    match ctx.build_sql(&query) {
        Ok(sql) => panic!("Expected a planning error, got SQL:\n{sql}"),
        Err(e) => assert!(
            e.to_string().contains("orders.category"),
            "The error must name the dimension the mask reads:\n{}",
            e
        ),
    }
}

/// The accept side of the same rule, and the premise the mask branch rests on:
/// an applied mask is rendered inside the multi-stage CTE, so its dimension read
/// resolves against a column of that CTE rather than the cube alias.
///
/// The dimension is grouped by in the query, not merely declared in
/// `grain.include`. An unconditional mask replaces the member's aggregate, so its
/// read sits outside any aggregate and has to be in the stage's own GROUP BY; a
/// declared leaf grain puts the column in the source but not in that GROUP BY,
/// and the database rejects the result. The reachability check does not tell the
/// two apart — it asks only whether a column exists — so that shape still plans
/// and fails at the database.
#[tokio::test(flavor = "multi_thread")]
async fn test_active_mask_reading_a_grouped_dimension_plans() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_masked_by_category_in_grain
        dimensions:
          - orders.category
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        order:
          - id: orders.category
          - id: orders.created_at
        maskedMembers:
          - member: orders.amount_masked_by_category_in_grain
    "#};

    let sql = ctx.build_sql(query).unwrap();

    assert!(
        sql.contains("\"fk_aggregate\".\"orders__category\" = 'books'"),
        "Expected the mask to render against the CTE column:\n{}",
        sql
    );
    assert!(
        !sql.contains("\"orders\".category = 'books'"),
        "The mask must not read the dimension off the cube alias:\n{}",
        sql
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// The same exclusion has to hold when the masked member is a multi-stage time
/// dimension, which reaches its slots through the base symbol it is a view of.
#[tokio::test(flavor = "multi_thread")]
async fn test_masked_multi_stage_time_dimension_is_not_reported() {
    let ctx = create_context();

    // `orders.created_at` is grouped by so that the dimension's own sql dep is
    // reachable — otherwise this would report for that reason and stop guarding
    // the mask.
    let query = indoc! {r#"
        measures:
          - orders.total_amount
        dimensions:
          - orders.created_at
        time_dimensions:
          - dimension: orders.created_at_masked_multi_stage
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
    "#};

    let sql = ctx.build_sql(query).unwrap();

    assert!(
        !sql.contains("\"orders\".category"),
        "The inactive mask must not put a cube-qualified column into the CTE:\n{}",
        sql
    );
}
