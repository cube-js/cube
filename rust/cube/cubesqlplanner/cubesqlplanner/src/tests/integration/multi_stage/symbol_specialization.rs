//! A multi-stage stage or a subquery dimension filters differently from the
//! query around it, so its `case` pruning and FILTER_PARAMS activity must follow
//! its own filters.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::{table_rows, TestContext};
use indoc::indoc;

const SEED: &str = "integration_multi_stage_tables.sql";

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_stage.yaml");
    TestContext::new(schema).unwrap()
}

// Rows of a result without the named column, sorted, so a query grouped by a
// case over `status` compares with the same query grouped by `status`.
fn rows_without_column(result: &str, column: &str) -> Vec<Vec<String>> {
    let header: Vec<String> = result
        .lines()
        .next()
        .unwrap_or_default()
        .split('|')
        .map(|cell| cell.trim().to_string())
        .collect();
    let skip = header.iter().position(|name| name == column);
    let mut rows: Vec<Vec<String>> = table_rows(result)
        .into_iter()
        .map(|row| {
            row.into_iter()
                .enumerate()
                .filter(|(i, _)| Some(*i) != skip)
                .map(|(_, cell)| cell)
                .collect()
        })
        .collect();
    rows.sort();
    rows
}

// Runs `query` and `equivalent` and compares their rows with `column` and
// `equivalent_column` left out.
async fn assert_same_rows(
    ctx: &TestContext,
    query: &str,
    column: &str,
    equivalent: &str,
    equivalent_column: &str,
) {
    let (Some(result), Some(expected)) = (
        ctx.try_execute_pg(query, SEED).await,
        ctx.try_execute_pg(equivalent, SEED).await,
    ) else {
        return;
    };
    assert_eq!(
        rows_without_column(&result, column),
        rows_without_column(&expected, equivalent_column),
        "\nresult:\n{}\nexpected:\n{}",
        result,
        expected
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_case_dim_under_keep_only_dropping_its_filter() {
    let ctx = create_context();

    // `keep_only: category` drops the status filter inside the stage.
    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_keep_only_category
        dimensions:
          - orders.status_case_label
        filters:
          - member: orders.status
            operator: equals
            values:
              - completed
    "#};
    let equivalent = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_keep_only_category
        dimensions:
          - orders.status
        filters:
          - member: orders.status
            operator: equals
            values:
              - completed
    "#};

    assert_same_rows(
        &ctx,
        query,
        "orders__status_case_label",
        equivalent,
        "orders__status",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_case_dim_under_nested_stage_dropping_its_filter() {
    let ctx = create_context();

    // The wrapper keeps the status filter; the stage it reads drops it.
    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_exclude_status_wrapped
        dimensions:
          - orders.status_case_label
        filters:
          - member: orders.status
            operator: equals
            values:
              - completed
    "#};
    let equivalent = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_exclude_status_wrapped
        dimensions:
          - orders.status
        filters:
          - member: orders.status
            operator: equals
            values:
              - completed
    "#};

    assert_same_rows(
        &ctx,
        query,
        "orders__status_case_label",
        equivalent,
        "orders__status",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_case_measure_in_stage_dropping_its_switch_filter() {
    let ctx = create_context();

    // The stage drops the status filter, so the case measure inside it weighs
    // every status, exactly as it does with no filter at all.
    let query = indoc! {r#"
        measures:
          - orders.status_weighted_amount_exclude_status
        filters:
          - member: orders.status
            operator: equals
            values:
              - completed
    "#};
    let equivalent = indoc! {r#"
        measures:
          - orders.status_weighted_amount
    "#};

    assert_same_rows(&ctx, query, "", equivalent, "").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_case_dim_filter_pruned_by_a_filter_the_stage_drops() {
    let ctx = create_context();

    // The query's status filter reduces the case filter to always true. The
    // stage drops the status filter and keeps the case filter, which must still
    // select only completed orders there.
    let query = indoc! {r#"
        measures:
          - orders.amount_exclude_status
        filters:
          - member: orders.status
            operator: equals
            values:
              - completed
          - member: orders.status_case_label
            operator: equals
            values:
              - done
    "#};
    let equivalent = indoc! {r#"
        measures:
          - orders.amount_exclude_status
        filters:
          - member: orders.status_case_label
            operator: equals
            values:
              - done
    "#};

    assert_same_rows(&ctx, query, "", equivalent, "").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_case_dim_under_stage_adding_a_filter() {
    let ctx = create_context();

    // The stage filters by status where the query does not, so it may prune the
    // case further than the query, never less.
    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_only_completed
        dimensions:
          - orders.status_case_label
    "#};
    let equivalent = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_only_completed
        dimensions:
          - orders.status
    "#};

    assert_same_rows(
        &ctx,
        query,
        "orders__status_case_label",
        equivalent,
        "orders__status",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_case_dim_under_rolling_window_over_stage_dropping_its_filter() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_amount_exclude_status
        dimensions:
          - orders.status_case_label
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        filters:
          - member: orders.status
            operator: equals
            values:
              - completed
    "#};
    let equivalent = indoc! {r#"
        measures:
          - orders.rolling_amount_exclude_status
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        filters:
          - member: orders.status
            operator: equals
            values:
              - completed
    "#};

    assert_same_rows(
        &ctx,
        query,
        "orders__status_case_label",
        equivalent,
        "orders__status",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_case_measure_under_fixed_stage_inside_a_filtering_stage() {
    let ctx = create_context();

    // `mode: fixed` drops the status filter the wrapper adds, so the case
    // measure inside weighs every status, as it does with no filter at all.
    let query = indoc! {r#"
        measures:
          - orders.status_weighted_amount_fixed_under_completed
    "#};
    let equivalent = indoc! {r#"
        measures:
          - orders.status_weighted_amount
    "#};

    assert_same_rows(&ctx, query, "", equivalent, "").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_plain_measure_reading_a_case_in_stage_dropping_its_filter() {
    let ctx = create_context();

    // The stage drops the status filter, so the plain measure it reads keeps
    // every branch of the case its own filter compares.
    let query = indoc! {r#"
        measures:
          - orders.done_amount_exclude_status
        filters:
          - member: orders.status
            operator: equals
            values:
              - completed
    "#};
    let equivalent = indoc! {r#"
        measures:
          - orders.done_amount
    "#};

    assert_same_rows(&ctx, query, "", equivalent, "").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_single_branch_multi_stage_case_dim_through_a_view() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders_ms_view.total_amount
        dimensions:
          - orders_ms_view.status_case_label_multi_stage
        filters:
          - member: orders_ms_view.status
            operator: equals
            values:
              - completed
        order:
          - id: orders_ms_view.status_case_label_multi_stage
    "#};

    ctx.build_sql(query).unwrap();
    assert_rows(
        &ctx,
        query,
        vec![
            vec!["BOOKS", "600.00"],
            vec!["CLOTHING", "300.00"],
            vec!["ELECTRONICS", "500.00"],
        ],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_single_branch_multi_stage_case_dim() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
        dimensions:
          - orders.status_case_label_multi_stage
        filters:
          - member: orders.status
            operator: equals
            values:
              - completed
        order:
          - id: orders.status_case_label_multi_stage
    "#};

    ctx.build_sql(query).unwrap();
    assert_rows(
        &ctx,
        query,
        vec![
            vec!["BOOKS", "600.00"],
            vec!["CLOTHING", "300.00"],
            vec!["ELECTRONICS", "500.00"],
        ],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "a stage dropping the filter needs back the multi-stage dimension the query planned as a regular one"]
async fn test_single_branch_multi_stage_case_dim_under_stage_dropping_its_filter() {
    let ctx = create_context();

    // The query's filter leaves the dimension one branch and so a regular
    // dimension; the stage drops that filter and needs every branch back.
    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_exclude_status
        dimensions:
          - orders.status_case_label_multi_stage
        filters:
          - member: orders.status
            operator: equals
            values:
              - completed
        order:
          - id: orders.status_case_label_multi_stage
    "#};

    // Only completed orders carry a category label, so the stage reads the
    // same rows per label as the query does.
    assert_rows(
        &ctx,
        query,
        vec![
            vec!["BOOKS", "600.00", "600.00"],
            vec!["CLOTHING", "300.00", "300.00"],
            vec!["ELECTRONICS", "500.00", "500.00"],
        ],
    )
    .await;
}

// A subquery dimension is planned as a query of its own, with its own filters.
fn subquery_dimension_context() -> TestContext {
    let schema = MockSchema::from_yaml(indoc! {"
        cubes:
            - name: sq_customers
              sql: \"SELECT * FROM ms_customers\"
              joins:
                  - name: sq_orders
                    relationship: one_to_many
                    sql: \"{sq_customers}.id = {sq_orders.customer_id}\"
              dimensions:
                  - name: id
                    type: number
                    sql: id
                    primary_key: true
                  - name: name
                    type: string
                    sql: name
                  - name: done_amount
                    type: number
                    sql: \"{sq_orders.done_amount}\"
                    sub_query: true
                  - name: done_amount_propagated
                    type: number
                    sql: \"{sq_orders.done_amount}\"
                    sub_query: true
                    propagate_filters_to_sub_query: true
            - name: sq_orders
              sql: \"SELECT * FROM ms_orders\"
              dimensions:
                  - name: id
                    type: number
                    sql: id
                    primary_key: true
                  - name: customer_id
                    type: number
                    sql: customer_id
                  - name: status
                    type: string
                    sql: status
                  - name: status_case_label
                    type: string
                    case:
                        switch: \"{CUBE.status}\"
                        when:
                            - value: completed
                              sql: \"'done'\"
                        else:
                            sql: \"'other'\"
              measures:
                  - name: total_amount
                    type: sum
                    sql: amount
                  - name: done_amount
                    type: sum
                    sql: amount
                    filters:
                        - sql: \"{CUBE.status_case_label} = 'done'\"
    "})
    .unwrap();
    TestContext::new(schema).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subquery_dimension_reading_a_case_the_query_prunes() {
    let ctx = subquery_dimension_context();

    // The subquery does not filter by status, so its measure counts only the
    // completed orders of each customer, whatever the query filters by.
    let query = indoc! {r#"
        measures:
          - sq_orders.total_amount
        dimensions:
          - sq_customers.name
          - sq_customers.done_amount
        filters:
          - member: sq_orders.status
            operator: equals
            values:
              - pending
        order:
          - id: sq_customers.name
    "#};

    ctx.build_sql(query).unwrap();
    assert_rows(
        &ctx,
        query,
        vec![
            vec!["Alice", "300.00", "150.00"],
            vec!["Bob", "500.00", "200.00"],
            vec!["Charlie", "600.00", "300.00"],
        ],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subquery_dimension_pruning_a_case_by_the_filters_it_takes() {
    let ctx = subquery_dimension_context();

    // The subquery takes the status filter, so its case keeps only the `other`
    // branch and no order of any customer counts as done.
    let query = indoc! {r#"
        measures:
          - sq_orders.total_amount
        dimensions:
          - sq_customers.name
          - sq_customers.done_amount_propagated
        filters:
          - member: sq_orders.status
            operator: equals
            values:
              - pending
        order:
          - id: sq_customers.name
    "#};

    ctx.build_sql(query).unwrap();
    assert_rows(
        &ctx,
        query,
        vec![
            vec!["Alice", "NULL", "150.00"],
            vec!["Bob", "NULL", "200.00"],
            vec!["Charlie", "NULL", "300.00"],
        ],
    )
    .await;
}

// The cube pushes the status filter into its own sql, and one measure carries a
// binding whose column reads a joined cube, so where that binding renders the
// join must reach `fp_customers`.
fn filter_params_context() -> TestContext {
    let schema = MockSchema::from_yaml(indoc! {"
        cubes:
            - name: fp_orders
              sql: \"SELECT * FROM ms_orders WHERE {FILTER_PARAMS_COLUMN:fp_orders.status:status}\"
              joins:
                  - name: fp_customers
                    relationship: many_to_one
                    sql: \"{fp_orders}.customer_id = {fp_customers.id}\"
              dimensions:
                  - name: id
                    type: number
                    sql: id
                    primary_key: true
                  - name: status
                    type: string
                    sql: status
              measures:
                  - name: total_amount
                    type: sum
                    sql: amount
                  - name: amount_exclude_status
                    type: sum
                    sql: \"{CUBE.total_amount}\"
                    multi_stage: true
                    filter:
                        exclude:
                            - fp_orders.status
                  - name: amount_only_completed
                    type: sum
                    sql: \"{CUBE.total_amount}\"
                    multi_stage: true
                    filter:
                        include:
                            - member: fp_orders.status
                              operator: equals
                              values: [completed]
                  - name: amount_city_bound
                    type: sum
                    sql: amount
                    filters:
                        - sql: \"{FILTER_PARAMS:fp_orders.status:[fp_customers.city] <> %0}\"
                  - name: amount_city_bound_only_completed
                    type: sum
                    sql: \"{CUBE.amount_city_bound}\"
                    multi_stage: true
                    filter:
                        include:
                            - member: fp_orders.status
                              operator: equals
                              values: [completed]
                  - name: amount_city_bound_exclude_status
                    type: sum
                    sql: \"{CUBE.amount_city_bound}\"
                    multi_stage: true
                    filter:
                        exclude:
                            - fp_orders.status
            - name: fp_customers
              sql: \"SELECT * FROM ms_customers\"
              dimensions:
                  - name: id
                    type: number
                    sql: id
                    primary_key: true
                  - name: city
                    type: string
                    sql: city
    "})
    .unwrap();
    TestContext::new(schema).unwrap()
}

async fn assert_rows(ctx: &TestContext, query: &str, expected: Vec<Vec<&str>>) {
    let Some(result) = ctx.try_execute_pg(query, SEED).await else {
        return;
    };
    assert_eq!(table_rows(&result), expected, "\nresult:\n{}", result);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_filter_params_binding_off_in_stage_dropping_its_filter() {
    let ctx = filter_params_context();

    // The stage drops the status filter, so its scan must not push it down.
    let query = indoc! {r#"
        measures:
          - fp_orders.total_amount
          - fp_orders.amount_exclude_status
        filters:
          - member: fp_orders.status
            operator: equals
            values:
              - completed
    "#};

    ctx.build_sql(query).unwrap();
    assert_rows(&ctx, query, vec![vec!["1400.00", "2250.00"]]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_filter_params_binding_on_in_stage_adding_its_filter() {
    let ctx = filter_params_context();

    // Only the stage filters by status, so only its scan pushes the filter down.
    let query = indoc! {r#"
        measures:
          - fp_orders.total_amount
          - fp_orders.amount_only_completed
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert!(
        sql.contains("WHERE (status ="),
        "the stage's scan must push its status filter down\nsql: {}",
        sql
    );
    assert!(
        sql.contains("WHERE 1 = 1"),
        "the query's own scan has no status filter to push down\nsql: {}",
        sql
    );
    assert_rows(&ctx, query, vec![vec!["2250.00", "1400.00"]]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_filter_params_binding_joins_its_column_cube_in_stage_adding_its_filter() {
    let ctx = filter_params_context();

    // The binding renders only inside the stage, and its column reads
    // `fp_customers`, so the stage's join must reach that cube.
    let query = indoc! {r#"
        measures:
          - fp_orders.amount_city_bound_only_completed
    "#};

    ctx.build_sql(query).unwrap();
    assert_rows(&ctx, query, vec![vec!["1400.00"]]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_filter_params_binding_off_in_stage_dropping_its_filter_with_joined_column() {
    let ctx = filter_params_context();

    // The query filters by status, the stage does not: the binding renders
    // nothing there, whatever cube its column reads.
    let query = indoc! {r#"
        measures:
          - fp_orders.amount_city_bound
          - fp_orders.amount_city_bound_exclude_status
        filters:
          - member: fp_orders.status
            operator: equals
            values:
              - completed
    "#};

    ctx.build_sql(query).unwrap();
    assert_rows(&ctx, query, vec![vec!["1400.00", "2250.00"]]).await;
}
