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
// There the pushed-down column is offset by the same interval as the stage
// predicate, so the rows a shifted stage scans line up with the bounds it
// groups by. A calendar shift has no such offset: `prior_fiscal_year` resolves
// to `next_fiscal_year_d` on the calendar cube, which is data rather than
// arithmetic and is not in scope inside the fact's own `sql`.
//
// So a STRING column is rejected - it would otherwise be bound to the
// unshifted reporting bounds and empty the stage - while a CALLBACK column is
// accepted, because it is handed the query's own bounds and can widen the
// pushed-down range itself.
fn schema(margin_sql: &str) -> MockSchema {
    MockSchema::from_yaml(&format!(
        indoc! {"
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
                          sql: \"{{CUBE}}.next_fiscal_year_d\"
                        - name: prior_two_fiscal_year
                          sql: \"{{CUBE}}.next_two_fiscal_year_d\"

            - name: fpc_margin
              sql: \"{margin_sql}\"
              joins:
                  - name: fpc_calendar
                    relationship: many_to_one
                    sql: \"{{fpc_margin}}.week_end_d = {{fpc_calendar.calendar_d}}\"
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
                    sql: \"{{CUBE.net_sales}}\"
                    multi_stage: true
                    time_shift:
                        - name: prior_fiscal_year

                  - name: net_sales_ly2
                    type: number
                    sql: \"{{CUBE.net_sales}}\"
                    multi_stage: true
                    time_shift:
                        - name: prior_two_fiscal_year
    "},
        margin_sql = margin_sql
    ))
    .unwrap()
}

const STRING_COLUMN: &str =
    "SELECT * FROM fpc_margin WHERE {FILTER_PARAMS_COLUMN:fpc_calendar.calendar_d:week_end_d}";

// The band a model writes by hand once it knows the shifted periods it has to
// cover - the callback form, which receives the query's own bounds as %0/%1.
const CALLBACK_COLUMN: &str = concat!(
    "SELECT * FROM fpc_margin WHERE ",
    "{FILTER_PARAMS:fpc_calendar.calendar_d:",
    "(week_end_d >= %0 AND week_end_d <= %1) OR ",
    "(week_end_d >= %0 - interval '371 day' AND week_end_d <= %1 - interval '364 day')}"
);

fn build(margin_sql: &str) -> Result<String, cubenativeutils::CubeError> {
    let ctx = TestContext::new(schema(margin_sql)).unwrap();

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
        .map(|(sql, _)| sql)
}

// Before this was rejected, the column rendered bare and was bound to the
// unshifted reporting bounds in every stage. Each stage joins the calendar on
// its own mapping column, so the pushed-down predicate contradicted the stage
// around it and the stage came back empty - the same failure CORE-543 fixed for
// interval shifts, reached by a different route.
#[test]
fn string_column_is_rejected_under_a_named_calendar_shift() {
    let err = build(STRING_COLUMN).expect_err("a string column cannot carry a calendar shift");
    let message = err.to_string();

    assert!(
        message.contains("fpc_calendar.calendar_d") && message.contains("prior_fiscal_year"),
        "the error must name the binding and the shift it cannot carry\nerror: {}",
        message
    );
    assert!(
        message.contains("callback"),
        "the error must point at the form that can carry it\nerror: {}",
        message
    );
}

// A callback column is handed the query's bounds and decides the range itself,
// so it is left alone. This is what a model widened by hand relies on, and it
// must keep working.
#[test]
fn callback_column_is_pushed_into_every_shifted_stage() {
    let sql = build(CALLBACK_COLUMN).expect("a callback column carries its own band");

    assert_eq!(
        sql.matches("week_end_d >=").count(),
        4,
        "both bands must render in both shifted stages\nsql: {}",
        sql
    );
    assert!(
        !sql.contains("FROM fpc_margin WHERE (1 = 1)"),
        "no stage may be left scanning the fact unfiltered\nsql: {}",
        sql
    );
}

// The stage predicate is what carries the shift: each stage compares the
// calendar's own mapping column, not `calendar_d`.
#[test]
fn stage_predicate_uses_the_named_mapping_column() {
    let sql = build(CALLBACK_COLUMN).unwrap();

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

// A calendar cube may declare a shift as a plain interval, with no mapping
// `sql`. That shape is arithmetic, so the column carries it rather than being
// rejected - and the sign has to match what `CalendarTimeShiftSqlNode` renders:
// the calendar map holds the declaration as written and inverts at render,
// unlike `TimeShiftState`, which is stored already inverted.
fn interval_declared_schema() -> MockSchema {
    MockSchema::from_yaml(indoc! {"
        cubes:
            - name: fpi_calendar
              calendar: true
              sql: \"SELECT * FROM fpi_calendar\"
              dimensions:
                  - name: calendar_d
                    type: time
                    sql: calendar_d
                    primary_key: true
                    time_shift:
                        - name: prior_year
                          interval: \"1 year\"
                          type: prior

            - name: fpi_margin
              sql: \"SELECT * FROM fpi_margin WHERE {FILTER_PARAMS_COLUMN:fpi_calendar.calendar_d:week_end_d}\"
              joins:
                  - name: fpi_calendar
                    relationship: many_to_one
                    sql: \"{fpi_margin}.week_end_d = {fpi_calendar.calendar_d}\"
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
                        - name: prior_year
    "})
    .unwrap()
}

#[test]
fn interval_declared_calendar_shift_offsets_the_column() {
    let ctx = TestContext::new(interval_declared_schema()).unwrap();

    let (sql, _) = ctx
        .build_sql_and_params(indoc! {"
            measures:
              - fpi_margin.net_sales_ly
            time_dimensions:
              - dimension: fpi_calendar.calendar_d
                dateRange:
                  - \"2026-06-20\"
                  - \"2026-06-20\"
        "})
        .expect("an interval-declared calendar shift is arithmetic the column can carry");

    assert!(
        sql.contains("(week_end_d + interval '-1 year')"),
        "the column must be offset by the inverted declaration, the same way \
         CalendarTimeShiftSqlNode renders the dimension\nsql: {}",
        sql
    );
}

// A calendar cube whose shifted time dimension is NOT its primary key. The
// shift is registered under the calendar's PK (`calendar_time_shift_for_*`
// returns `time_shift_pk_full_name`), while FILTER_PARAMS binds the shifted
// dimension itself - so a lookup on the bound symbol's own name misses and the
// column would render bare against unshifted bounds.
fn non_pk_binding_schema() -> MockSchema {
    MockSchema::from_yaml(indoc! {"
        cubes:
            - name: fpk_calendar
              calendar: true
              sql: \"SELECT * FROM fpk_calendar\"
              dimensions:
                  - name: date_key
                    type: time
                    sql: date_key
                    primary_key: true
                  - name: retail_d
                    type: time
                    sql: retail_d
                    time_shift:
                        - name: prior_fiscal_year
                          sql: \"{CUBE}.prior_retail_d\"

            - name: fpk_margin
              sql: \"SELECT * FROM fpk_margin WHERE {FILTER_PARAMS_COLUMN:fpk_calendar.retail_d:week_end_d}\"
              joins:
                  - name: fpk_calendar
                    relationship: many_to_one
                    sql: \"{fpk_margin}.week_end_d = {fpk_calendar.date_key}\"
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
    "})
    .unwrap()
}

#[test]
fn shift_is_found_when_the_binding_is_not_the_calendar_pk() {
    let ctx = TestContext::new(non_pk_binding_schema()).unwrap();

    let result = ctx.build_sql_and_params(indoc! {"
            measures:
              - fpk_margin.net_sales_ly
            time_dimensions:
              - dimension: fpk_calendar.retail_d
                dateRange:
                  - \"2026-06-20\"
                  - \"2026-06-20\"
        "});

    match result {
        Err(err) => assert!(
            err.to_string().contains("prior_fiscal_year"),
            "the shift must be found through the calendar PK, not silently missed\nerror: {}",
            err
        ),
        Ok((sql, _)) => panic!(
            "the binding's shift was missed and the column rendered bare\nsql: {}",
            sql
        ),
    }
}
