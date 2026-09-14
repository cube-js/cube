use crate::cube_bridge::base_query_options::FilterValue;
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

          - name: amount_cumulative
            type: sum
            sql: amount
            rolling_window:
                trailing: unbounded

          - name: amount_trailing_30d
            type: sum
            sql: amount
            rolling_window:
                trailing: 30 day
                offset: end

          - name: amount_month_to_date
            type: sum
            sql: amount
            rolling_window:
                type: to_date
                granularity: month
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

/// The values the placeholders of `predicate` stand for, in the order they
/// appear. A parameter the predicate does not name says nothing about what the
/// scan reads.
fn predicate_values(predicate: &str, params: &[FilterValue]) -> Vec<String> {
    predicate
        .split('$')
        .skip(1)
        .filter_map(|tail| {
            tail.chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>()
                .parse::<usize>()
                .ok()
        })
        // Placeholders are 1-based in the rendered SQL.
        .filter_map(|position| params.get(position.checked_sub(1)?))
        .filter_map(|value| value.to_param_string())
        .collect()
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

// A callback binding is handed the band the stage reads rather than the period
// reported, so the scan it writes reaches as far back as the window sums. The
// band arrives as dates, already shifted, since a callback's SQL is opaque and
// nothing can wrap an interval around it.
#[test]
fn a_callback_binding_reaches_back_as_far_as_the_window() {
    let ctx = TestContext::new(schema(Some(CALLBACK))).unwrap();
    let (sql, params) = ctx.build_sql_and_params(QUERY).unwrap();
    let predicate = fact_scan_predicate(&sql);
    let bounds = predicate_values(&predicate, &params);

    assert!(
        bounds.iter().any(|bound| bound.starts_with("2024-01-31")),
        "the scan stops short of the 30 days the window sums: {bounds:?}\npredicate: {predicate}"
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

// The same in numbers for the callback form: a 30-day trailing window over one
// row per day answers 30 for every day of the reporting period, binding or not.
#[tokio::test(flavor = "multi_thread")]
async fn a_callback_binding_leaves_the_window_whole() {
    let with_pushdown = TestContext::new(schema(Some(CALLBACK))).unwrap();
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

// A `to_date` window reads from the start of its period, which for a range
// opening mid-month is well before the reported period. Both stages -- the
// cube's own scan and the window's -- have to reach that far.
const TO_DATE_QUERY: &str = indoc! {r#"
    measures:
      - fprw_sales.amount_month_to_date
    time_dimensions:
      - dimension: fprw_sales.day_d
        granularity: day
        dateRange:
          - "2024-03-15"
          - "2024-03-21"
    order:
      - id: fprw_sales.day_d
"#};

// A bound an engine can evaluate before reading the table is what eliminates
// partitions; one read back off the series with a scalar sub-select is opaque
// to that, and a table declared with a mandatory partition filter rejects the
// query outright.
#[test]
fn a_column_binding_under_a_to_date_window_pushes_literal_bounds() {
    let ctx = TestContext::new(schema(Some(COLUMN))).unwrap();
    let sql = ctx.build_sql(TO_DATE_QUERY).unwrap();
    let predicate = fact_scan_predicate(&sql);

    assert!(
        !predicate.contains("time_series"),
        "the scan is bounded by a sub-select over the series\npredicate: {}",
        predicate
    );
}

// A to_date window reads from the start of its own period rather than from the
// start of the range reported, and the callback is handed that start — so the
// scan it writes covers what the window sums.
#[test]
fn a_callback_binding_under_a_to_date_window_reaches_the_period_start() {
    let ctx = TestContext::new(schema(Some(CALLBACK))).unwrap();
    let (sql, params) = ctx.build_sql_and_params(TO_DATE_QUERY).unwrap();
    let predicate = fact_scan_predicate(&sql);
    let bounds = predicate_values(&predicate, &params);

    assert!(
        bounds.iter().any(|bound| bound.starts_with("2024-03-01")),
        "the scan stops short of the period the window sums: {bounds:?}\npredicate: {predicate}"
    );
}

// The same in numbers: month-to-date over one row per day answers the day of
// the month. A binding cutting the scan to the reported period answers the day
// of that period instead.
#[tokio::test(flavor = "multi_thread")]
async fn a_callback_binding_leaves_a_to_date_window_whole() {
    let with_pushdown = TestContext::new(schema(Some(CALLBACK))).unwrap();
    let without = TestContext::new(schema(None)).unwrap();

    let Some(pushed_down) = with_pushdown.try_execute_pg(TO_DATE_QUERY, SEED).await else {
        return;
    };
    let full_scan = without
        .try_execute_pg(TO_DATE_QUERY, SEED)
        .await
        .expect("the plain model runs wherever the pushdown one does");

    assert_eq!(pushed_down, full_scan);
    insta::assert_snapshot!(pushed_down);
}

// A cumulative window reaches back without limit, so nothing narrows the lower
// end — that is what the measure asks for. The upper end is still the series'
// own, and a column binding states it: the scan stops at the reporting period
// rather than reading past it.
const CUMULATIVE_QUERY: &str = indoc! {r#"
    measures:
      - fprw_sales.amount_cumulative
    time_dimensions:
      - dimension: fprw_sales.day_d
        granularity: day
        dateRange:
          - "2024-03-01"
          - "2024-03-07"
"#};

#[test]
fn a_column_binding_keeps_the_upper_bound_of_a_cumulative_window() {
    let ctx = TestContext::new(schema(Some(COLUMN))).unwrap();
    let (sql, params) = ctx.build_sql_and_params(CUMULATIVE_QUERY).unwrap();
    let predicate = fact_scan_predicate(&sql);

    assert!(
        !predicate.contains(">="),
        "a cumulative window has no lower bound to state\npredicate: {predicate}"
    );
    assert_eq!(
        predicate_values(&predicate, &params),
        vec!["2024-03-07T23:59:59.999"],
        "the upper bound is the series' own, as a literal\npredicate: {predicate}"
    );
}

// A callback takes both bounds, so a band with only one is a band it cannot be
// given: it states nothing rather than a bound it was never handed.
#[test]
fn a_callback_binding_states_nothing_for_a_cumulative_window() {
    let ctx = TestContext::new(schema(Some(CALLBACK))).unwrap();
    let sql = ctx.build_sql(CUMULATIVE_QUERY).unwrap();

    assert_eq!(fact_scan_predicate(&sql), "(1 = 1)", "{sql}");
}
