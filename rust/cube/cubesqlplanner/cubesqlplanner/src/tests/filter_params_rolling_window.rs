use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const SEED: &str = "filter_params_rolling_window_tables.sql";

// A cube narrowing its own scan through FILTER_PARAMS on its time dimension,
// with a rolling measure whose window reaches back before the reporting period.
// One row per day carrying amount 1, so a rolling sum reads as the number of
// days the window covered.
fn schema(binding: Option<&str>) -> MockSchema {
    let fact_sql = match binding {
        Some(binding) => format!("SELECT * FROM fprw_sales WHERE {{{}}}", binding),
        None => "SELECT * FROM fprw_sales".to_string(),
    };
    MockSchema::from_yaml(&format!(
        r#"
cubes:
    - name: fprw_sales
      sql: "{}"
      dimensions:
          - name: id
            sql: id
            type: number
            primary_key: true
          - name: day_d
            sql: day_d
            type: time
      measures:
          - name: amount
            type: sum
            sql: amount

          - name: amount_trailing_30d
            type: sum
            sql: amount
            rolling_window:
                trailing: 30 day
                offset: end
"#,
        fact_sql
    ))
    .unwrap()
}

const COLUMN: &str = "FILTER_PARAMS_COLUMN:fprw_sales.day_d:day_d";
const CALLBACK: &str = "FILTER_PARAMS:fprw_sales.day_d:day_d >= (%0)::timestamptz \
                        AND day_d <= (%1)::timestamptz";

// The predicate the cube's sql carries.
fn fact_scan_predicate(sql: &str) -> String {
    const SCAN: &str = "FROM fprw_sales WHERE ";
    let after = &sql[sql.find(SCAN).expect("the cube's sql is scanned") + SCAN.len()..];
    let mut depth = 0i32;
    let mut end = after.len();
    for (i, ch) in after.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                if depth == 0 {
                    end = i;
                    break;
                }
                depth -= 1;
            }
            _ => {}
        }
    }
    after[..end].trim().to_string()
}

const QUERY: &str = indoc! {r#"
    measures:
      - fprw_sales.amount
      - fprw_sales.amount_trailing_30d
    time_dimensions:
      - dimension: fprw_sales.day_d
        granularity: day
        dateRange:
          - "2024-03-01"
          - "2024-03-07"
    order:
      - id: fprw_sales.day_d
"#};

// A rolling window reads a period wider than the one reported, and a binding
// passing its column carries that same widening — the stage's own bounds and
// the scan's agree.
#[test]
fn a_column_binding_widens_with_the_window() {
    let ctx = TestContext::new(schema(Some(COLUMN))).unwrap();
    let sql = ctx.build_sql(QUERY).unwrap();
    let predicate = fact_scan_predicate(&sql);

    assert!(
        predicate.contains("interval '30 day'"),
        "the scan reaches back as far as the window does\npredicate: {}",
        predicate
    );
}

// Characterisation, not a statement of intent: a callback binding is handed
// the filter's own values, and a rolling filter's values are the reported
// bounds followed by the window's own parts. What the callback receives is
// therefore the reported period, so a model narrowing its scan through a
// callback loses the tail of every window. Recorded here so the disagreement
// is visible; the fix is not settled by this test.
#[test]
fn a_callback_binding_is_handed_the_reported_period_only() {
    let ctx = TestContext::new(schema(Some(CALLBACK))).unwrap();
    let sql = ctx.build_sql(QUERY).unwrap();
    let predicate = fact_scan_predicate(&sql);

    assert!(
        !predicate.contains("interval '30 day'"),
        "the callback is handed nothing of the window\npredicate: {}",
        predicate
    );
}

// The same in numbers: a 30-day trailing window over one row per day answers
// 30 for every day of the reporting period. A binding that cut the tail off
// would answer less on the first days.
#[tokio::test(flavor = "multi_thread")]
async fn a_column_binding_leaves_the_window_whole() {
    let with_pushdown = TestContext::new(schema(Some(COLUMN))).unwrap();
    let without = TestContext::new(schema(None)).unwrap();

    let Some(pushed_down) = with_pushdown.try_execute_pg(QUERY, SEED).await else {
        return;
    };
    let full_scan = without
        .try_execute_pg(QUERY, SEED)
        .await
        .expect("the plain model runs wherever the pushdown one does");

    assert_eq!(pushed_down, full_scan);
    insta::assert_snapshot!(pushed_down);
}

// Characterisation: the callback form answers less than the same model without
// the binding, which is the disagreement above in numbers.
#[tokio::test(flavor = "multi_thread")]
async fn a_callback_binding_answers_less_than_a_full_scan() {
    let with_pushdown = TestContext::new(schema(Some(CALLBACK))).unwrap();
    let without = TestContext::new(schema(None)).unwrap();

    let Some(pushed_down) = with_pushdown.try_execute_pg(QUERY, SEED).await else {
        return;
    };
    let full_scan = without
        .try_execute_pg(QUERY, SEED)
        .await
        .expect("the plain model runs wherever the pushdown one does");

    assert_ne!(pushed_down, full_scan);
    insta::assert_snapshot!(pushed_down);
}
