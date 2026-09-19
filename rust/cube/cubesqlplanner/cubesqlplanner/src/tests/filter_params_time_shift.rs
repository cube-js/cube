use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

// A cube whose `sql` pushes a time dimension down through FILTER_PARAMS, where
// that dimension is derived from the time dimension of another cube and a
// multi-stage measure shifts it.
fn schema() -> MockSchema {
    MockSchema::from_yaml(indoc! {"
        cubes:
            - name: fps_returns
              sql: \"SELECT * FROM fps_returns WHERE {FILTER_PARAMS_COLUMN:fps_orders.return_day:DATE_TRUNC('day', created_at)}\"
              dimensions:
                  - name: id
                    type: number
                    sql: id
                    primary_key: true
                  - name: order_id
                    type: number
                    sql: order_id
                  - name: created_at
                    type: time
                    sql: created_at
              measures:
                  - name: count
                    type: count

            - name: fps_orders
              sql: \"SELECT * FROM fps_orders\"
              joins:
                  - name: fps_returns
                    relationship: one_to_many
                    sql: \"{fps_orders}.id = {fps_returns.order_id}\"
              dimensions:
                  - name: id
                    type: number
                    sql: id
                    primary_key: true
                  - name: return_day
                    type: time
                    sql: \"DATE_TRUNC('day', {fps_returns.created_at})\"
              measures:
                  - name: total
                    type: sum
                    sql: amount

                  - name: total_prev_month
                    type: number
                    sql: \"{CUBE.total}\"
                    multi_stage: true
                    time_shift:
                        - interval: \"1 month\"
                          type: prior
                          timeDimension: fps_orders.return_day
    "})
    .unwrap()
}

// The shifted stage reads the dimension offset by the interval, so the
// predicate pushed into the cube's sql has to be offset the same way. Leaving
// it bare would filter the source rows by unshifted bounds while the stage
// groups by shifted values.
#[test]
fn pushed_down_column_is_offset_in_the_shifted_stage() {
    let ctx = TestContext::new(schema()).unwrap();

    let (sql, _) = ctx
        .build_sql_and_params(indoc! {"
            measures:
              - fps_orders.total
              - fps_orders.total_prev_month
            time_dimensions:
              - dimension: fps_orders.return_day
                granularity: month
                dateRange:
                  - \"2024-01-01\"
                  - \"2024-03-31\"
        "})
        .unwrap();

    assert!(
        sql.contains("(DATE_TRUNC('day', created_at) + interval '1 month')"),
        "the shifted stage must push down the offset column\nsql: {}",
        sql
    );
    assert!(
        sql.contains("(DATE_TRUNC('day', created_at) >="),
        "the unshifted stage must push down the bare column\nsql: {}",
        sql
    );
}
