use crate::cube_bridge::base_query_options::FilterValue;
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

// A fact whose `sql` pushes a joined CALENDAR cube's date down through
// FILTER_PARAMS, with multi-stage measures that shift that date by NAME rather
// than by interval - the shape a retail 4-5-4 calendar forces, where "one
// fiscal year back" is a mapping column on the calendar and not an interval any
// arithmetic can express.
//
// Contrast with filter_params_time_shift.rs, which covers the interval form.
// The two are handled by the same call site but only one of them can produce a
// shift: see `base_filter.rs`, where the shift handed to
// `to_sql_for_filter_params` is `…get_for_symbol(sym).and_then(|s| s.interval.as_ref())`.
// A named calendar shift carries `interval: None`, so that resolves to `None`
// and the pushed-down column is rendered bare.
fn schema() -> MockSchema {
    MockSchema::from_yaml(indoc! {"
        cubes:
            - name: fpc_calendar
              calendar: true
              sql: \"SELECT * FROM fpc_calendar\"
              dimensions:
                  - name: calendar_d
                    type: time
                    sql: calendar_d
                    primary_key: true
                    time_shift:
                        - name: prior_fiscal_year
                          sql: \"{CUBE}.next_fiscal_year_d\"
                        - name: prior_two_fiscal_year
                          sql: \"{CUBE}.next_two_fiscal_year_d\"

            - name: fpc_margin
              sql: \"SELECT * FROM fpc_margin WHERE {FILTER_PARAMS_COLUMN:fpc_calendar.calendar_d:week_end_d}\"
              joins:
                  - name: fpc_calendar
                    relationship: many_to_one
                    sql: \"{fpc_margin}.week_end_d = {fpc_calendar.calendar_d}\"
              dimensions:
                  - name: id
                    type: number
                    sql: id
                    primary_key: true
                  - name: week_end_d
                    type: time
                    sql: week_end_d
              measures:
                  - name: net_sales
                    type: sum
                    sql: net_sales_a

                  - name: net_sales_ly
                    type: number
                    sql: \"{CUBE.net_sales}\"
                    multi_stage: true
                    time_shift:
                        - name: prior_fiscal_year

                  - name: net_sales_ly2
                    type: number
                    sql: \"{CUBE.net_sales}\"
                    multi_stage: true
                    time_shift:
                        - name: prior_two_fiscal_year
    "})
    .unwrap()
}

fn shifted_stages() -> (String, Vec<FilterValue>) {
    let ctx = TestContext::new(schema()).unwrap();

    ctx.build_sql_and_params(indoc! {"
            measures:
              - fpc_margin.net_sales_ly
              - fpc_margin.net_sales_ly2
            time_dimensions:
              - dimension: fpc_calendar.calendar_d
                dateRange:
                  - \"2026-06-20\"
                  - \"2026-06-20\"
        "})
        .unwrap()
}

fn shifted_stages_sql() -> String {
    shifted_stages().0
}

// Each named shift builds its own stage, and each stage rescans the fact. The
// binding stays attributed to `fpc_calendar.calendar_d` - the shift substitutes
// the column only where the stage's own predicate renders - so FILTER_PARAMS
// keeps matching and the fact scan is never left unfiltered.
#[test]
fn pushed_down_column_reaches_every_named_shift_stage() {
    let sql = shifted_stages_sql();

    assert_eq!(
        sql.matches("FROM fpc_margin WHERE (week_end_d >=").count(),
        2,
        "both named-shift stages must push the column down\nsql: {}",
        sql
    );
    assert!(
        !sql.contains("FROM fpc_margin WHERE (1 = 1)"),
        "no stage may be left scanning the fact unfiltered\nsql: {}",
        sql
    );
}

// The stage predicate is what carries the shift: each stage compares the
// calendar's mapping column, not `calendar_d`.
#[test]
fn stage_predicate_uses_the_named_mapping_column() {
    let sql = shifted_stages_sql();

    assert!(
        sql.contains("next_fiscal_year_d"),
        "the prior-fiscal-year stage must filter on its mapping column\nsql: {}",
        sql
    );
    assert!(
        sql.contains("next_two_fiscal_year_d"),
        "the prior-two-fiscal-year stage must filter on its mapping column\nsql: {}",
        sql
    );
}

// The gap this file exists to pin down.
//
// `filter_params_time_shift.rs` asserts that an INTERVAL shift offsets the
// pushed-down column, so the rows a shifted stage scans line up with the bounds
// that stage groups by. A NAMED calendar shift gets no such treatment: the
// column is rendered bare and bound to the UNSHIFTED reporting bounds, in every
// stage.
//
// That is a contradiction inside each stage. `cte_0` joins the calendar on
// `week_end_d = next_fiscal_year_d`, so it reads PRIOR-year fact rows - while
// the pushed-down predicate restricts the same scan to the REPORTING week. The
// stage is empty unless the model widens the pushed-down band by hand to cover
// the shifted periods, and once it does, every stage scans every band.
#[test]
fn named_shift_binds_the_pushed_down_column_to_unshifted_bounds() {
    let (sql, params) = shifted_stages();

    assert!(
        !sql.contains("week_end_d + interval"),
        "a named calendar shift currently cannot offset the pushed-down column; \
         if this now passes, base_filter.rs learned to carry non-interval shifts \
         and this test should be inverted\nsql: {}",
        sql
    );

    // Eight bounds: a pushed-down pair and a stage pair per shifted stage. Every
    // one of them is the reporting day, so nothing distinguishes the scan of the
    // prior-fiscal-year stage from the scan of the two-year one.
    let bounds: Vec<String> = params
        .iter()
        .map(|p| match p {
            FilterValue::Str(s) => s.clone(),
            other => panic!("unexpected bound {:?}", other),
        })
        .collect();
    assert_eq!(bounds.len(), 8, "sql: {}", sql);
    assert!(
        bounds.iter().all(|b| b.starts_with("2026-06-20")),
        "every bound stays on the reporting day - none is shifted back a fiscal \
         year or two\nbounds: {:?}\nsql: {}",
        bounds,
        sql
    );
}
