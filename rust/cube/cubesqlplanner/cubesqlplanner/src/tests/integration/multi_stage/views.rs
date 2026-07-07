use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_stage.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_stage_tables.sql";

fn assert_uses_window(sql: &str) {
    assert!(
        sql.contains("OVER (PARTITION BY"),
        "expected SQL to use a window function (`... OVER (PARTITION BY ...)`),\n\
         got:\n{}",
        sql,
    );
}

/// PARTITION BY clause bodies (text between `PARTITION BY` and the following
/// `ORDER BY`) of every window function in `sql`.
fn partition_by_clauses(sql: &str) -> Vec<String> {
    sql.match_indices("PARTITION BY")
        .map(|(i, _)| {
            let rest = &sql[i + "PARTITION BY".len()..];
            let end = rest.find("ORDER BY").unwrap_or(rest.len());
            rest[..end].to_string()
        })
        .collect()
}

// CORE-550 through a view: the query groups by view dimensions
// (orders_ms_view.status / .created_at) while the measure's reduce_by targets
// the underlying base cube member (orders.created_at). The granular view time
// dimension (created_at_month) must still be dropped from the window's
// PARTITION BY, leaving [status]. Guards that the fix resolves the reduce_by
// reference through the view→base reference chain, not just for plain cubes.
#[tokio::test(flavor = "multi_thread")]
async fn test_view_reduce_by_time_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders_ms_view.total_amount
          - orders_ms_view.amount_reduce_time
        dimensions:
          - orders_ms_view.status
        time_dimensions:
          - dimension: orders_ms_view.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        order:
          - id: orders_ms_view.status
          - id: orders_ms_view.created_at
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert_uses_window(&sql);
    for clause in partition_by_clauses(&sql) {
        assert!(
            !clause.contains("created_at"),
            "reduce_by(created_at) must drop the view time dimension from PARTITION BY,\n\
             partition clause was: {}\nfull SQL:\n{}",
            clause,
            sql,
        );
        assert!(
            clause.contains("status"),
            "status must remain in PARTITION BY, partition clause was: {}",
            clause,
        );
    }

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// CORE-606: the `filter: exclude` directive must drop the query-level filter
// from the measure's inner leaf CTE identically whether the measure is queried
// on the cube directly or through a view. Regression: through the view the
// status filter leaked into the exclude leaf, so amount_exclude_status collapsed
// to total_amount (any derived percentage became 100%).
#[tokio::test(flavor = "multi_thread")]
async fn test_view_exclude_drops_dim_filter_from_leaf() {
    let ctx = create_context();

    let cube_query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_exclude_status
        dimensions:
          - orders.category
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - completed
        order:
          - id: orders.category
    "#};

    let view_query = indoc! {r#"
        measures:
          - orders_ms_view.total_amount
          - orders_ms_view.amount_exclude_status
        dimensions:
          - orders_ms_view.category
        filters:
          - dimension: orders_ms_view.status
            operator: equals
            values:
              - completed
        order:
          - id: orders_ms_view.category
    "#};

    // The status filter belongs only in the total_amount leaf, never in the
    // amount_exclude_status leaf. Count its occurrences: querying the cube
    // directly yields exactly one; through the view it must be the same.
    let count_status_filter = |sql: &str| sql.matches("\"orders\".status =").count();
    let cube_sql = ctx.build_sql(cube_query).unwrap();
    let view_sql = ctx.build_sql(view_query).unwrap();
    assert_eq!(
        count_status_filter(&view_sql),
        count_status_filter(&cube_sql),
        "exclude(status) must drop the query filter from the leaf CTE through a \
         view just as on the cube; the filter leaked into the exclude leaf.\n\
         cube SQL:\n{}\nview SQL:\n{}",
        cube_sql,
        view_sql,
    );

    if let Some(result) = ctx.try_execute_pg(view_query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// CORE-606 (segment variant): `filter: exclude` targeting a segment must drop
// the query-level segment from the leaf CTE through a view, just as on the cube.
// The segment renders as `"orders".status = 'completed'`; it belongs only in the
// total_amount leaf, never in the amount_exclude_segment leaf.
#[tokio::test(flavor = "multi_thread")]
async fn test_view_exclude_drops_segment_from_leaf() {
    let ctx = create_context();

    let cube_query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_exclude_segment
        dimensions:
          - orders.category
        segments:
          - orders.completed_orders
        order:
          - id: orders.category
    "#};

    let view_query = indoc! {r#"
        measures:
          - orders_ms_view.total_amount
          - orders_ms_view.amount_exclude_segment
        dimensions:
          - orders_ms_view.category
        segments:
          - orders_ms_view.completed_orders
        order:
          - id: orders_ms_view.category
    "#};

    let count_segment = |sql: &str| sql.matches("\"orders\".status = 'completed'").count();
    let cube_sql = ctx.build_sql(cube_query).unwrap();
    let view_sql = ctx.build_sql(view_query).unwrap();
    assert_eq!(
        count_segment(&view_sql),
        count_segment(&cube_sql),
        "exclude(segment) must drop the query segment from the leaf CTE through a \
         view just as on the cube; the segment leaked into the exclude leaf.\n\
         cube SQL:\n{}\nview SQL:\n{}",
        cube_sql,
        view_sql,
    );

    if let Some(result) = ctx.try_execute_pg(view_query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// CORE-606 (keep_only dimension variant): `filter: keep_only: [status]` must
// keep the status filter and drop the category filter from the leaf CTE through
// a view exactly as on the cube. Regression path would drop status too (its
// view member name never matched the base `orders.status`), collapsing the leaf
// to a grand total.
#[tokio::test(flavor = "multi_thread")]
async fn test_view_keep_only_dim_filter_at_leaf() {
    let ctx = create_context();

    let cube_query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_keep_only_status
        dimensions:
          - orders.status
        filters:
          - dimension: orders.status
            operator: equals
            values: [completed]
          - dimension: orders.category
            operator: equals
            values: [books]
    "#};

    let view_query = indoc! {r#"
        measures:
          - orders_ms_view.total_amount
          - orders_ms_view.amount_keep_only_status
        dimensions:
          - orders_ms_view.status
        filters:
          - dimension: orders_ms_view.status
            operator: equals
            values: [completed]
          - dimension: orders_ms_view.category
            operator: equals
            values: [books]
    "#};

    let count_status = |sql: &str| sql.matches("\"orders\".status =").count();
    let count_category = |sql: &str| sql.matches("\"orders\".category =").count();
    let cube_sql = ctx.build_sql(cube_query).unwrap();
    let view_sql = ctx.build_sql(view_query).unwrap();
    assert_eq!(
        count_status(&view_sql),
        count_status(&cube_sql),
        "keep_only(status) must keep the status filter in the leaf CTE through a view.\n\
         cube SQL:\n{}\nview SQL:\n{}",
        cube_sql,
        view_sql,
    );
    assert_eq!(
        count_category(&view_sql),
        count_category(&cube_sql),
        "keep_only(status) must drop the category filter from the leaf CTE through a view.\n\
         cube SQL:\n{}\nview SQL:\n{}",
        cube_sql,
        view_sql,
    );

    if let Some(result) = ctx.try_execute_pg(view_query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// CORE-606 (keep_only segment variant): `filter: keep_only: [segment]` must keep
// the segment and drop the category filter from the leaf CTE through a view.
#[tokio::test(flavor = "multi_thread")]
async fn test_view_keep_only_segment_at_leaf() {
    let ctx = create_context();

    let cube_query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_keep_only_segment
        dimensions:
          - orders.category
        segments:
          - orders.completed_orders
        filters:
          - dimension: orders.category
            operator: equals
            values: [books]
        order:
          - id: orders.category
    "#};

    let view_query = indoc! {r#"
        measures:
          - orders_ms_view.total_amount
          - orders_ms_view.amount_keep_only_segment
        dimensions:
          - orders_ms_view.category
        segments:
          - orders_ms_view.completed_orders
        filters:
          - dimension: orders_ms_view.category
            operator: equals
            values: [books]
        order:
          - id: orders_ms_view.category
    "#};

    let count_segment = |sql: &str| sql.matches("\"orders\".status = 'completed'").count();
    let count_category = |sql: &str| sql.matches("\"orders\".category =").count();
    let cube_sql = ctx.build_sql(cube_query).unwrap();
    let view_sql = ctx.build_sql(view_query).unwrap();
    assert_eq!(
        count_segment(&view_sql),
        count_segment(&cube_sql),
        "keep_only(segment) must keep the segment in the leaf CTE through a view.\n\
         cube SQL:\n{}\nview SQL:\n{}",
        cube_sql,
        view_sql,
    );
    assert_eq!(
        count_category(&view_sql),
        count_category(&cube_sql),
        "keep_only(segment) must drop the category filter from the leaf CTE through a view.\n\
         cube SQL:\n{}\nview SQL:\n{}",
        cube_sql,
        view_sql,
    );

    if let Some(result) = ctx.try_execute_pg(view_query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_with_time_shift() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders_ms_view.amount_prev_month
        time_dimensions:
          - dimension: orders_ms_view.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_with_calculated() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders_ms_view.total_amount
          - orders_ms_view.mom_growth
        time_dimensions:
          - dimension: orders_ms_view.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_with_filter() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders_ms_view.total_amount
        time_dimensions:
          - dimension: orders_ms_view.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        filters:
          - dimension: orders_ms_view.status
            operator: equals
            values:
              - completed
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
