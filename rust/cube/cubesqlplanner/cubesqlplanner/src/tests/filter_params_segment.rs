use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

// A cube whose `sql` pushes a segment down through FILTER_PARAMS. The pushed
// predicate is stated as the binding's column, the way a dimension binding
// states its column: the segment's own `sql` prefixes the columns with the cube,
// and no cube is in scope inside the sql that builds it.
fn schema() -> MockSchema {
    MockSchema::from_yaml(indoc! {"
        cubes:
            - name: orders
              sql: \"SELECT * FROM orders WHERE {FILTER_PARAMS_COLUMN:orders.completed:status = 'completed'}\"
              dimensions:
                  - name: id
                    type: number
                    sql: id
                    primary_key: true
                  - name: status
                    type: string
                    sql: status
                  - name: created_at
                    type: time
                    sql: created_at
              measures:
                  - name: count
                    type: count
              segments:
                  - name: completed
                    sql: \"{CUBE}.status = 'completed'\"
                  - name: recent
                    sql: \"{CUBE}.created_at > '2024-01-01'\"

        views:
            - name: orders_view
              cubes:
                  - join_path: orders
                    includes:
                        - count
                        - completed
    "})
    .unwrap()
}

#[test]
fn segment_in_query_pushes_its_filter_params_column_into_the_cube_sql() {
    let ctx = TestContext::new(schema()).unwrap();

    let (sql, _) = ctx
        .build_sql_and_params(indoc! {"
            measures:
              - orders.count
            segments:
              - orders.completed
        "})
        .unwrap();

    assert!(
        sql.contains("SELECT * FROM orders WHERE (status = 'completed')"),
        "the segment's column must render inside the cube's sql\nsql: {}",
        sql
    );
    assert!(
        !sql.contains("1 = 1"),
        "the binding must not fall back to always-true\nsql: {}",
        sql
    );
}

#[test]
fn segment_absent_from_query_leaves_the_binding_always_true() {
    let ctx = TestContext::new(schema()).unwrap();

    let (sql, _) = ctx
        .build_sql_and_params(indoc! {"
            measures:
              - orders.count
        "})
        .unwrap();

    assert!(
        sql.contains("SELECT * FROM orders WHERE 1 = 1"),
        "an unselected segment must not restrict the cube's sql\nsql: {}",
        sql
    );
}

#[test]
fn another_segment_in_query_does_not_activate_the_binding() {
    let ctx = TestContext::new(schema()).unwrap();

    let (sql, _) = ctx
        .build_sql_and_params(indoc! {"
            measures:
              - orders.count
            segments:
              - orders.recent
        "})
        .unwrap();

    assert!(
        sql.contains("SELECT * FROM orders WHERE 1 = 1"),
        "only the segment the binding names may activate it\nsql: {}",
        sql
    );
}

#[test]
fn segment_selected_through_a_view_activates_the_cube_binding() {
    let ctx = TestContext::new(schema()).unwrap();

    let (sql, _) = ctx
        .build_sql_and_params(indoc! {"
            measures:
              - orders_view.count
            segments:
              - orders_view.completed
        "})
        .unwrap();

    assert!(
        sql.contains("SELECT * FROM orders WHERE (status = 'completed')"),
        "a view re-exports the cube's segment, so the cube's binding still applies\nsql: {}",
        sql
    );
}

// A segment whose sql is a bare member reference resolves to that dimension.
// The dimension's own binding states a column to compare a value against, which
// is not a predicate, so selecting the segment must not activate it.
#[test]
fn segment_referencing_a_dimension_does_not_activate_that_dimensions_binding() {
    let schema = MockSchema::from_yaml(indoc! {"
        cubes:
            - name: orders
              sql: \"SELECT * FROM orders WHERE {FILTER_PARAMS_COLUMN:orders.status:LOWER(status)}\"
              dimensions:
                  - name: id
                    type: number
                    sql: id
                    primary_key: true
                  - name: status
                    type: string
                    sql: \"{CUBE}.status\"
              measures:
                  - name: count
                    type: count
              segments:
                  - name: bare_status
                    sql: \"{CUBE.status}\"
    "})
    .unwrap();
    let ctx = TestContext::new(schema).unwrap();

    let (sql, _) = ctx
        .build_sql_and_params(indoc! {"
            measures:
              - orders.count
            segments:
              - orders.bare_status
        "})
        .unwrap();

    assert!(
        sql.contains("SELECT * FROM orders WHERE 1 = 1"),
        "a dimension binding must not be activated by a segment\nsql: {}",
        sql
    );
}
