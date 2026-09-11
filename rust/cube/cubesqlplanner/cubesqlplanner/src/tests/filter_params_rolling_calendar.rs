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
