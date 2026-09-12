//! A rolling window whose period is bounded by a calendar cube reads a band no
//! interval math reproduces — the period's start is a row of the calendar, so
//! the planner writes it as a sub-select over the series rather than as a
//! literal. Worse for the fact table: the time dimension is the calendar's
//! column, so that predicate restricts the calendar and the fact is reached
//! through the join with no date bound of its own.
//!
//! A `FILTER_PARAMS` binding passing a column is what puts the bound back on
//! the fact's own column. It restates the stage's own predicate against that
//! column, so it needs nothing of the calendar the planner does not already
//! have, and states exactly — not approximately — what the window reads.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const SEED: &str = "integration_calendar_tables.sql";

/// The calendar fixture with `calendar_orders` narrowing its own scan through
/// the given binding.
fn schema(binding: Option<&str>) -> MockSchema {
    const PLAIN: &str = "sql: \"SELECT * FROM cal_orders\"";
    let yaml = std::fs::read_to_string(format!(
        "{}/src/test_fixtures/schemas/yaml_files/common/integration_calendar.yaml",
        env!("CARGO_MANIFEST_DIR")
    ))
    .expect("the calendar fixture is readable");
    assert!(yaml.contains(PLAIN), "the fact cube's sql moved");
    let yaml = match binding {
        Some(binding) => yaml.replace(
            PLAIN,
            &format!("sql: \"SELECT * FROM cal_orders WHERE {{{}}}\"", binding),
        ),
        None => yaml,
    };
    MockSchema::from_yaml(&yaml).unwrap()
}

/// The fact's own date column, which the calendar joins to.
const COLUMN: &str = "FILTER_PARAMS_COLUMN:custom_calendar.date_val:created_at";

const QUERY: &str = indoc! {r#"
    measures:
      - calendar_orders.count_month_to_date
    time_dimensions:
      - dimension: custom_calendar.date_val
        granularity: day
        dateRange:
          - "2024-02-29"
          - "2024-03-09"
    order:
      - id: custom_calendar.date_val
"#};

/// The predicate the fact's own sql carries, if any.
fn fact_scan_predicate(sql: &str) -> Option<String> {
    const SCAN: &str = "FROM cal_orders WHERE ";
    let start = sql.find(SCAN)? + SCAN.len();
    let after = &sql[start..];
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
    Some(after[..end].trim().to_string())
}

// Without a binding the fact table carries no date predicate at all: the
// window's bound is written over the calendar's column, and the fact is reached
// through the join. An engine requiring a filter on the partition column
// rejects such a query outright.
#[test]
fn a_calendar_window_leaves_the_fact_unrestricted() {
    let ctx = TestContext::new(schema(None)).unwrap();
    let sql = ctx.build_sql(QUERY).unwrap();

    assert!(
        !sql.contains("FROM cal_orders WHERE"),
        "expected the fact scan to carry no predicate:\n{sql}"
    );
}

// Passing the fact's column restates the window's own bound against it — the
// period's start still read off the series, since nothing else knows it, but
// now bounding the table that needed bounding.
#[test]
fn a_column_binding_bounds_the_fact_by_the_period_the_window_reads() {
    let ctx = TestContext::new(schema(Some(COLUMN))).unwrap();
    let sql = ctx.build_sql(QUERY).unwrap();
    let predicate = fact_scan_predicate(&sql).expect("the fact scan carries a predicate");

    assert!(
        predicate.contains("created_at"),
        "the bound is over the fact's own column\npredicate: {predicate}"
    );
    assert!(
        predicate.contains("min(\"date_period_start_month\")"),
        "the bound reaches back to the period the window counts from\npredicate: {predicate}"
    );
}

// The restated bound is the window's own, so it drops nothing: the numbers are
// the ones the unrestricted scan answers.
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

// --- a rolling window over a shifted calendar dimension ---

/// The fixture with a measure that is both: a `to_date` window over the
/// calendar, read a fiscal year earlier.
fn shifted_schema(binding: Option<&str>) -> MockSchema {
    const PLAIN: &str = "sql: \"SELECT * FROM cal_orders\"";
    const ANCHOR: &str = "      - name: count_week_to_date";
    let yaml = std::fs::read_to_string(format!(
        "{}/src/test_fixtures/schemas/yaml_files/common/integration_calendar.yaml",
        env!("CARGO_MANIFEST_DIR")
    ))
    .expect("the calendar fixture is readable");
    assert!(yaml.contains(ANCHOR), "the fixture's measures moved");
    let added = "      - name: count_month_to_date_prev_year\n\
                 \x20       type: number\n\
                 \x20       multi_stage: true\n\
                 \x20       sql: \"{count_month_to_date}\"\n\
                 \x20       time_shift:\n\
                 \x20         - name: one_year\n\n";
    let yaml = yaml.replace(ANCHOR, &format!("{added}{ANCHOR}"));
    let yaml = match binding {
        Some(binding) => yaml.replace(
            PLAIN,
            &format!("sql: \"SELECT * FROM cal_orders WHERE {{{}}}\"", binding),
        ),
        None => yaml,
    };
    MockSchema::from_yaml(&yaml).unwrap()
}

const SHIFTED_QUERY: &str = indoc! {r#"
    measures:
      - calendar_orders.count_month_to_date_prev_year
    time_dimensions:
      - dimension: custom_calendar.date_val
        granularity: day
        dateRange:
          - "2024-02-29"
          - "2024-03-09"
    order:
      - id: custom_calendar.date_val
"#};

// Nothing addresses the shift, so nothing states the band the stage reads and
// the scan stays open — the shift contract, unchanged by the window on top.
#[test]
fn a_shifted_window_without_a_binding_for_its_shift_leaves_the_scan_open() {
    let ctx = TestContext::new(shifted_schema(Some(COLUMN))).unwrap();
    let sql = ctx.build_sql(SHIFTED_QUERY).unwrap();

    assert_eq!(
        fact_scan_predicate(&sql).as_deref(),
        Some("(1 = 1)"),
        "{sql}"
    );
}

// Addressing the shift is what the model is told to do, and a stage that is
// also a rolling window's base scan must not refuse it: its filter is the
// window's own, and carries the same two bounds a date range does.
#[test]
fn a_shifted_window_accepts_a_binding_addressing_its_shift() {
    const CALLBACK: &str =
        "FILTER_PARAMS:custom_calendar.date_val@one_year:created_at >= (%0)::timestamptz \
         AND created_at <= (%1)::timestamptz";
    let ctx = TestContext::new(shifted_schema(Some(CALLBACK))).unwrap();
    let sql = ctx.build_sql(SHIFTED_QUERY).unwrap();

    // The band itself is still not derivable — a calendar's period start is a
    // row, and the shift moves it further — so the binding states nothing and
    // the scan stays open. It is accepted rather than refused, which is what
    // leaves the model somewhere to stand.
    assert_eq!(
        fact_scan_predicate(&sql).as_deref(),
        Some("(1 = 1)"),
        "{sql}"
    );
}

/// The fixture with a plain trailing window alongside the calendar ones.
fn mixed_schema() -> MockSchema {
    const ANCHOR: &str = "      - name: count_week_to_date";
    let yaml = std::fs::read_to_string(format!(
        "{}/src/test_fixtures/schemas/yaml_files/common/integration_calendar.yaml",
        env!("CARGO_MANIFEST_DIR")
    ))
    .expect("the calendar fixture is readable");
    assert!(yaml.contains(ANCHOR), "the fixture's measures moved");
    let added = "      - name: count_trailing_7d\n\
                 \x20       type: count\n\
                 \x20       rolling_window:\n\
                 \x20         trailing: 7 day\n\n";
    MockSchema::from_yaml(&yaml.replace(ANCHOR, &format!("{added}{ANCHOR}"))).unwrap()
}

// A query mixing a plain trailing window with a `to_date` one counting off the
// calendar shares one series between them, and the calendar's periods travel
// on it. The plain window's scan is bounded by literals derived from interval
// math, so those bounds have to still cover the series the calendar drives —
// otherwise the window silently loses the rows past them.
#[tokio::test(flavor = "multi_thread")]
async fn a_window_sharing_a_calendar_series_keeps_its_rows() {
    let ctx = TestContext::new(mixed_schema()).unwrap();

    let alone = indoc! {r#"
        measures:
          - calendar_orders.count_trailing_7d
        time_dimensions:
          - dimension: custom_calendar.date_val
            granularity: day
            dateRange:
              - "2024-02-29"
              - "2024-03-09"
        order:
          - id: custom_calendar.date_val
    "#};
    let mixed = indoc! {r#"
        measures:
          - calendar_orders.count_trailing_7d
          - calendar_orders.count_month_to_date
        time_dimensions:
          - dimension: custom_calendar.date_val
            granularity: day
            dateRange:
              - "2024-02-29"
              - "2024-03-09"
        order:
          - id: custom_calendar.date_val
    "#};

    let Some(alone) = ctx.try_execute_pg(alone, SEED).await else {
        return;
    };
    let mixed = ctx
        .try_execute_pg(mixed, SEED)
        .await
        .expect("the mixed query runs wherever the plain one does");

    // The trailing counts are the same column in both, so every row of the
    // plain answer must appear in the mixed one.
    for line in alone.lines().skip(2) {
        let (day, count) = line.split_once('|').expect("a data row");
        let day = day.trim();
        let count = count.trim();
        let mixed_row = mixed
            .lines()
            .find(|l| l.trim_start().starts_with(day))
            .unwrap_or_else(|| panic!("{day} missing from the mixed answer:\n{mixed}"));
        assert!(
            mixed_row.contains(count),
            "{day}: trailing count {count} changed when the calendar window joined:\n{mixed_row}"
        );
    }
}
