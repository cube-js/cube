//! View members in the contexts where a view member carries something of its
//! own: an access-policy mask, a custom granularity, a geo target.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const SEED: &str = "integration_multi_fact_tables.sql";

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_view_members.yaml")
        .only_pre_aggregations(&[]);
    TestContext::new(schema).unwrap()
}

// A masked view dimension and a masked view measure next to their unmasked
// siblings in a grouped query: only the members named in `maskedMembers` render
// their mask.
#[tokio::test(flavor = "multi_thread")]
async fn test_view_masked_members_grouped() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders_view.masked_total_const
          - orders_view.total_amount
        dimensions:
          - orders_view.masked_status
          - orders_view.status
        order:
          - id: orders_view.status
        maskedMembers:
          - member: orders_view.masked_total_const
          - member: orders_view.masked_status
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Masks whose SQL references another member, in an ungrouped query: the rows
// must carry the mask values, never the source values.
#[tokio::test(flavor = "multi_thread")]
async fn test_view_masked_members_ungrouped() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders_view.masked_total
          - orders_view.masked_total_const
        dimensions:
          - orders_view.id
          - orders_view.masked_status_dep
        order:
          - id: orders_view.id
        ungrouped: true
        maskedMembers:
          - member: orders_view.masked_total
          - member: orders_view.masked_total_const
          - member: orders_view.masked_status_dep
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// A conditional mask on an aggregate view measure whose filter member is in the
// GROUP BY: rows matching the filter keep the original value.
#[tokio::test(flavor = "multi_thread")]
async fn test_view_conditional_measure_mask_filter_in_group_by() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders_view.masked_total_const
        dimensions:
          - orders_view.status
        order:
          - id: orders_view.status
        maskedMembers:
          - member: orders_view.masked_total_const
            filter:
              member: orders_view.status
              operator: equals
              values: ['completed']
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// The same conditional mask with its filter member outside the GROUP BY: the
// aggregate renders the mask value directly.
#[tokio::test(flavor = "multi_thread")]
async fn test_view_conditional_measure_mask_filter_outside_group_by() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders_view.masked_total_const
        dimensions:
          - orders_view.name
        order:
          - id: orders_view.name
        maskedMembers:
          - member: orders_view.masked_total_const
            filter:
              member: orders_view.status
              operator: equals
              values: ['completed']
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_conditional_dimension_mask() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders_view.count
        dimensions:
          - orders_view.masked_status
          - orders_view.status
        order:
          - id: orders_view.status
        maskedMembers:
          - member: orders_view.masked_status
            filter:
              member: orders_view.status
              operator: equals
              values: ['completed']
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// A masked view dimension re-exported from a joined cube.
#[tokio::test(flavor = "multi_thread")]
async fn test_view_masked_joined_dimension() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders_view.count
        dimensions:
          - orders_view.city
          - orders_view.name
        order:
          - id: orders_view.name
        maskedMembers:
          - member: orders_view.city
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// A filter on a masked view dimension compares the mask value.
#[tokio::test(flavor = "multi_thread")]
async fn test_view_filtered_by_masked_dimension() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders_view.count
        dimensions:
          - orders_view.masked_status
        filters:
          - dimension: orders_view.masked_status
            operator: equals
            values:
              - hidden
        maskedMembers:
          - member: orders_view.masked_status
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_custom_granularity_with_origin() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders_view.count
        time_dimensions:
          - dimension: orders_view.created_at
            granularity: bi_weekly
            dateRange:
              - \"2025-01-01\"
              - \"2025-12-31\"
        order:
          - id: orders_view.created_at
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_custom_granularity_with_offset() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders_view.count
          - orders_view.total_amount
        time_dimensions:
          - dimension: orders_view.created_at
            granularity: fiscal_year
        order:
          - id: orders_view.created_at
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// A view re-exports a geo dimension without its latitude and longitude; the
// view member renders the coordinates of the dimension it references.
#[tokio::test(flavor = "multi_thread")]
async fn test_view_geo_dimension() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders_view.count
        dimensions:
          - orders_view.location
        order:
          - id: orders_view.location
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Members declared by the view itself: a calculated dimension over a cube
// member and a sibling view member, a calculated measure over view members, and
// a hand-written direct reference to a cube measure.
#[tokio::test(flavor = "multi_thread")]
async fn test_view_calculated_members() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders_view.amount_per_order
          - orders_view.total_amount_ref
        dimensions:
          - orders_view.status_label
        order:
          - id: orders_view.status_label
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_sibling_reference() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders_view.count
        dimensions:
          - orders_view.status_ref
        order:
          - id: orders_view.status_ref
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// A calculated view measure over a measure the join multiplies: each customer
// is counted once, however many orders it has, as on the cube.
#[tokio::test(flavor = "multi_thread")]
async fn test_view_calculated_measure_over_multiplied_measure() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - customers_view.count
          - customers_view.total_amount
          - customers_view.amount_per_customer
        dimensions:
          - customers_view.name
        order:
          - id: customers_view.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Control: the same calculated measure declared on the cube.
#[tokio::test(flavor = "multi_thread")]
async fn test_cube_calculated_measure_over_multiplied_measure() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - customers.count
          - orders.total_amount
          - customers.amount_per_customer
        dimensions:
          - customers.name
        order:
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// A view's own aggregation over a dimension it names aggregates like a cube
// measure would.
#[tokio::test(flavor = "multi_thread")]
async fn test_view_own_aggregations_over_a_dimension() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders_view.distinct_customers
          - orders_view.amount_sum
        dimensions:
          - orders_view.status
        order:
          - id: orders_view.status
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// A view member whose target renders a compound expression keeps it
// parenthesized when an arithmetic expression uses it.
#[tokio::test(flavor = "multi_thread")]
async fn test_view_member_in_arithmetic_keeps_its_parentheses() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders_view.half_amount_plus_100
        dimensions:
          - orders_view.status
        order:
          - id: orders_view.status
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

fn table_rows(result: &str) -> Vec<Vec<String>> {
    result
        .lines()
        .skip(2)
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            line.split('|')
                .map(|cell| cell.trim().to_string())
                .collect()
        })
        .collect()
}

fn ungrouped_conditional_mask_query(prefix: &str, dimensions: &[&str]) -> String {
    let dimensions = dimensions
        .iter()
        .map(|d| format!("  - {prefix}.{d}\n"))
        .collect::<String>();
    format!(
        "measures:\n  - {prefix}.masked_total_const\ndimensions:\n{dimensions}order:\n  - id: {prefix}.id\nungrouped: true\nmaskedMembers:\n  - member: {prefix}.masked_total_const\n    filter:\n      member: {prefix}.status\n      operator: equals\n      values: ['completed']\n"
    )
}

// A conditionally masked view measure in an ungrouped query masks exactly as the
// cube measure it references, whether or not the filter member is selected.
#[tokio::test(flavor = "multi_thread")]
async fn test_view_conditional_measure_mask_ungrouped_matches_the_cube() {
    let ctx = create_context();

    for dimensions in [&["id", "status"][..], &["id"][..]] {
        let view_query = ungrouped_conditional_mask_query("orders_view", dimensions);
        let cube_query = ungrouped_conditional_mask_query("orders", dimensions);
        ctx.build_sql(&view_query).unwrap();
        ctx.build_sql(&cube_query).unwrap();

        let (Some(view), Some(cube)) = (
            ctx.try_execute_pg(&view_query, SEED).await,
            ctx.try_execute_pg(&cube_query, SEED).await,
        ) else {
            continue;
        };
        let view_rows = table_rows(&view);
        assert_eq!(view_rows.len(), 8, "{view}");
        assert_eq!(view_rows, table_rows(&cube), "view:\n{view}\ncube:\n{cube}");
    }
}

// A multi-stage measure the view declares over a member it re-exports applies
// its own time shift.
#[tokio::test(flavor = "multi_thread")]
async fn test_view_own_time_shifted_measure() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders_view.total_amount
          - orders_view.total_amount_prior_year
        time_dimensions:
          - dimension: orders_view.created_at
            granularity: year
            dateRange:
              - \"2025-01-01\"
              - \"2026-12-31\"
        order:
          - id: orders_view.created_at
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
