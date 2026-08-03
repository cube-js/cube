use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

// The measure filter's only content is a `FILTER_PARAMS` binding whose column is
// a callback, and the time dimension is referenced from inside that callback
// alone — nothing else in the filter's SQL mentions it.
fn schema() -> MockSchema {
    MockSchema::from_yaml(indoc! {"
        cubes:
            - name: commission
              sql: \"SELECT * FROM commission\"
              dimensions:
                  - name: id
                    type: number
                    sql: id
                    primary_key: true
                  - name: partner
                    type: string
                    sql: partner
                  - name: pricing_duration
                    type: string
                    sql: pricing_duration
                  - name: reconciliation_date
                    type: time
                    sql: reconciliation_date
              measures:
                  - name: daily_mrr
                    type: sum
                    sql: \"{CUBE}.total\"
                    filters:
                        - sql: \"{CUBE.pricing_duration} = 'DAILY'\"
                        - sql: \"{FILTER_PARAMS:commission.reconciliation_date:[CUBE.reconciliation_date] >= %0 AND [CUBE.reconciliation_date] < %1}\"
    "})
    .unwrap()
}

const QUERY: &str = indoc! {"
    measures:
      - commission.daily_mrr
    dimensions:
      - commission.partner
    time_dimensions:
      - dimension: commission.reconciliation_date
        granularity: month
        dateRange:
          - \"2025-07-01\"
          - \"2026-06-30\"
"};

#[test]
fn measure_filter_callback_column_renders_the_referenced_member() {
    let ctx = TestContext::new(schema()).unwrap();

    let (sql, _) = ctx.build_sql_and_params(QUERY).unwrap();

    // Both halves of the callback's own predicate, with its member reference
    // resolved to the dimension's SQL. The query's own time filter renders `<=`
    // against the same column, so the strict `<` is what pins this to the
    // callback.
    assert!(
        sql.contains(
            "\"commission\".reconciliation_date >= $1 AND \"commission\".reconciliation_date < $2"
        ),
        "the filter param predicate must carry the referenced column\nsql: {}",
        sql
    );
    assert!(
        !sql.contains("{arg:"),
        "no dependency placeholder may survive into the SQL\nsql: {}",
        sql
    );
}

// A cube's `sql` builds the table the query reads from, so no member is in scope
// inside it. Resolving such a reference is what used to recurse between the cube
// table and the dimension until the stack ran out.
#[test]
fn cube_sql_referencing_a_member_is_rejected() {
    let schema = MockSchema::from_yaml(indoc! {"
        cubes:
            - name: commission
              sql: \"SELECT * FROM commission WHERE {CUBE.reconciliation_date} IS NOT NULL\"
              dimensions:
                  - name: id
                    type: number
                    sql: id
                    primary_key: true
                  - name: reconciliation_date
                    type: time
                    sql: reconciliation_date
              measures:
                  - name: count
                    type: count
    "})
    .unwrap();

    let err = TestContext::new(schema)
        .and_then(|ctx| {
            ctx.build_sql_and_params(indoc! {"
                measures:
                  - commission.count
            "})
        })
        .expect_err("a member reference in a cube's sql must be reported");

    assert!(
        err.message
            .contains("references member `commission.reconciliation_date`"),
        "unexpected error: {}",
        err.message
    );
}
