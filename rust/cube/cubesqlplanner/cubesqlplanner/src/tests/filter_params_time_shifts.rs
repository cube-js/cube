use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const SEED: &str = "filter_params_time_shifts_tables.sql";

// A fact cube whose `sql` narrows its scan through FILTER_PARAMS on the time
// dimension of a fiscal calendar. The calendar's year is 364 days, so its
// shifts are a mapping held in its own table rather than arithmetic on a date:
// day n of the calendar carries amount n, the fiscal year before it n - 364,
// two years before n - 728, and a nominal `1 year` interval lands on n - 365.
//
// `report_d` is the label queries group and filter by, `d` the primary key the
// fact table joins to and the shifts move.
const SCHEMA: &str = r#"
cubes:
    - name: fpts_calendar
      calendar: true
      sql: "SELECT * FROM fpts_calendar"
      dimensions:
          - name: d
            sql: "{CUBE}.d"
            type: time
            primary_key: true
            time_shift: &shifts
                - name: prev_fy
                  sql: "{CUBE}.d_prev_fy"
                - name: prev_two_fy
                  sql: "{CUBE}.d_prev_two_fy"
                - name: prev_fy_by_interval
                  interval: 364 day
                  type: prior

          - name: report_d
            sql: "{CUBE}.d"
            type: time
            time_shift: *shifts

          - name: plain_d
            sql: "{CUBE}.d"
            type: time

          - name: fy_name
            sql: "{CUBE}.fy_name"
            type: string

      measures:
          - name: count
            type: count

    - name: fpts_sales
      sql: "__FACT_SQL__"
      joins:
          - name: fpts_calendar
            sql: "{CUBE}.day_d = {fpts_calendar.d}"
            relationship: many_to_one
      dimensions:
          - name: id
            sql: id
            type: number
            primary_key: true
          - name: day_d
            sql: day_d
            type: time
          - name: fy_name
            sql: fy_name
            type: string
      measures:
          - name: amount
            type: sum
            sql: amount

          - name: amount_prev_fy
            type: number
            multi_stage: true
            sql: "{amount}"
            time_shift:
                - name: prev_fy

          - name: amount_prev_two_fy
            type: number
            multi_stage: true
            sql: "{amount}"
            time_shift:
                - name: prev_two_fy

          - name: amount_prev_fy_by_interval
            type: number
            multi_stage: true
            sql: "{amount}"
            time_shift:
                - interval: 364 day
                  type: prior

          - name: amount_prev_nominal_year
            type: number
            multi_stage: true
            sql: "{amount}"
            time_shift:
                - interval: 1 year
                  type: prior
"#;

const BASE: &str = "FILTER_PARAMS_COLUMN:fpts_calendar.report_d:day_d";
const PREV_FY: &str = "FILTER_PARAMS:fpts_calendar.report_d@prev_fy:\
                       day_d >= (%0)::timestamptz - interval '364 day' \
                       AND day_d <= (%1)::timestamptz - interval '364 day'";
const PREV_TWO_FY: &str = "FILTER_PARAMS:fpts_calendar.report_d@prev_two_fy:\
                           day_d >= (%0)::timestamptz - interval '728 day' \
                           AND day_d <= (%1)::timestamptz - interval '728 day'";
const PREV_FY_BY_INTERVAL: &str = "FILTER_PARAMS:fpts_calendar.report_d@prev_fy_by_interval:\
                                   day_d >= (%0)::timestamptz - interval '364 day' \
                                   AND day_d <= (%1)::timestamptz - interval '364 day'";

fn schema(bindings: &[&str]) -> MockSchema {
    let fact_sql = format!(
        "SELECT * FROM fpts_sales WHERE {{FILTER_GROUP|{}}}",
        bindings.join("|")
    );
    MockSchema::from_yaml(&SCHEMA.replace("__FACT_SQL__", &fact_sql)).unwrap()
}

// The same model with a rollup over the fact cube. A build query carries no
// user filters, so every binding in the group has nothing to restate.
fn schema_with_rollup(bindings: &[&str]) -> MockSchema {
    let fact_sql = format!(
        "SELECT * FROM fpts_sales WHERE {{FILTER_GROUP|{}}}",
        bindings.join("|")
    );
    let yaml = SCHEMA.replace("__FACT_SQL__", &fact_sql).replace(
        "      measures:\n          - name: amount",
        concat!(
            "      pre_aggregations:\n",
            "          - name: daily\n",
            "            type: rollup\n",
            "            measures:\n",
            "                - amount\n",
            "            time_dimension: day_d\n",
            "            granularity: day\n",
            "      measures:\n",
            "          - name: amount",
        ),
    );
    MockSchema::from_yaml(&yaml).unwrap()
}

fn plain_schema() -> MockSchema {
    MockSchema::from_yaml(&SCHEMA.replace("__FACT_SQL__", "SELECT * FROM fpts_sales")).unwrap()
}

fn full_schema() -> MockSchema {
    schema(&[BASE, PREV_FY, PREV_TWO_FY, PREV_FY_BY_INTERVAL])
}

// The predicate each scan of the fact table carries, in the order the CTEs
// appear. What a stage reads is decided here, so this is where a stage that
// reads the wrong band shows up.
fn fact_scan_predicates(sql: &str) -> Vec<String> {
    const SCAN: &str = "FROM fpts_sales WHERE ";
    let mut predicates = Vec::new();
    let mut rest = sql;
    while let Some(at) = rest.find(SCAN) {
        let after = &rest[at + SCAN.len()..];
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
        predicates.push(after[..end].trim().to_string());
        rest = &after[end..];
    }
    predicates
}

const SHIFTED_QUERY: &str = indoc! {r#"
    measures:
      - fpts_sales.amount
      - fpts_sales.amount_prev_fy
      - fpts_sales.amount_prev_two_fy
    time_dimensions:
      - dimension: fpts_calendar.report_d
        granularity: day
        dateRange:
          - "2024-12-29"
          - "2025-01-04"
    order:
      - id: fpts_calendar.report_d
"#};

// Each stage reads the band its own binding describes: the reporting week
// unshifted, the same week of the previous fiscal year 364 days back, and of
// the one before it 728 days back. No stage carries another's band, so no
// stage scans more than it uses.
#[test]
fn each_stage_binds_the_binding_written_for_its_shift() {
    let ctx = TestContext::new(full_schema()).unwrap();
    let sql = ctx.build_sql(SHIFTED_QUERY).unwrap();
    let predicates = fact_scan_predicates(&sql);

    assert_eq!(predicates.len(), 3, "expected three stages\nsql: {}", sql);

    let unshifted = predicates
        .iter()
        .filter(|p| !p.contains("interval"))
        .collect::<Vec<_>>();
    assert_eq!(
        unshifted.len(),
        1,
        "exactly one stage reads the reporting band\npredicates: {:?}",
        predicates
    );
    assert!(
        unshifted[0].contains("day_d >=") && unshifted[0].contains("day_d <="),
        "the unshifted stage still narrows its scan\npredicates: {:?}",
        predicates
    );

    for band in ["364 day", "728 day"] {
        let matching = predicates
            .iter()
            .filter(|p| p.contains(band))
            .collect::<Vec<_>>();
        assert_eq!(
            matching.len(),
            1,
            "exactly one stage reads the band {} back\npredicates: {:?}",
            band,
            predicates
        );
    }
}

// Without a binding for the shift the stage applies, nothing is restated and
// the scan stays open. The plain binding must not step in: its bounds describe
// the reporting period, which is not where the shifted stage reads.
#[test]
fn plain_binding_does_not_narrow_a_calendar_shifted_stage() {
    let ctx = TestContext::new(schema(&[BASE])).unwrap();
    let sql = ctx.build_sql(SHIFTED_QUERY).unwrap();
    let predicates = fact_scan_predicates(&sql);

    assert_eq!(predicates.len(), 3, "expected three stages\nsql: {}", sql);
    assert_eq!(
        predicates.iter().filter(|p| p.contains("day_d")).count(),
        1,
        "only the unshifted stage may narrow its scan\npredicates: {:?}",
        predicates
    );
    // Parenthesised: the binding was reached and dropped, where a group whose
    // member nothing filters collapses to a bare `1 = 1` before any binding is
    // consulted.
    assert_eq!(
        predicates.iter().filter(|p| *p == "(1 = 1)").count(),
        2,
        "both shifted stages scan unrestricted\npredicates: {:?}",
        predicates
    );
}

// A binding is picked by the shift it names, not by where it stands in the
// group: every binding names the same member, so position would otherwise
// decide.
#[test]
fn binding_order_within_the_group_does_not_matter() {
    let straight = TestContext::new(full_schema()).unwrap();
    let reversed =
        TestContext::new(schema(&[PREV_FY_BY_INTERVAL, PREV_TWO_FY, PREV_FY, BASE])).unwrap();

    let mut straight = fact_scan_predicates(&straight.build_sql(SHIFTED_QUERY).unwrap());
    let mut reversed = fact_scan_predicates(&reversed.build_sql(SHIFTED_QUERY).unwrap());
    straight.sort();
    reversed.sort();

    assert_eq!(straight, reversed);
}

// A shift the calendar declares as an interval rather than as a column is
// still the calendar's own mapping — reached through the shifted join, not by
// offsetting the fact column — so it is addressed by the name the calendar
// gives it, whether the measure asks for it by that name or by the interval.
#[test]
fn a_calendar_interval_declaration_is_addressed_by_its_declared_name() {
    let ctx = TestContext::new(full_schema()).unwrap();

    let sql = ctx
        .build_sql(indoc! {r#"
            measures:
              - fpts_sales.amount_prev_fy_by_interval
            time_dimensions:
              - dimension: fpts_calendar.report_d
                granularity: day
                dateRange:
                  - "2024-12-29"
                  - "2025-01-04"
        "#})
        .unwrap();

    let predicates = fact_scan_predicates(&sql);
    assert_eq!(predicates.len(), 1, "sql: {}", sql);
    assert!(
        predicates[0].contains("364 day"),
        "the stage reads the band its calendar declaration maps to\npredicate: {}",
        predicates[0]
    );
}

// An interval the calendar does not declare shifts its primary key by plain
// arithmetic, which the calendar gives no name — so no binding can address it
// and the stage scans unrestricted rather than reading the reporting band.
#[test]
fn an_undeclared_interval_leaves_the_stage_unrestricted() {
    let ctx = TestContext::new(full_schema()).unwrap();

    let sql = ctx
        .build_sql(indoc! {r#"
            measures:
              - fpts_sales.amount_prev_nominal_year
            time_dimensions:
              - dimension: fpts_calendar.report_d
                granularity: day
                dateRange:
                  - "2024-12-29"
                  - "2025-01-04"
        "#})
        .unwrap();

    assert_eq!(fact_scan_predicates(&sql), vec!["(1 = 1)".to_string()]);
}

// Nothing filters the member the group binds, so the group has no predicate to
// restate and collapses — the way a pre-aggregation build query, which carries
// no user filters, reaches it.
#[test]
fn a_group_binding_an_unfiltered_member_collapses() {
    let ctx = TestContext::new(full_schema()).unwrap();

    let sql = ctx
        .build_sql(indoc! {r#"
            measures:
              - fpts_sales.amount
            dimensions:
              - fpts_sales.fy_name
        "#})
        .unwrap();

    assert_eq!(fact_scan_predicates(&sql), vec!["1 = 1".to_string()]);
}

// Extracting the group's subtree keeps the query's own structure, so an `or`
// over the bound member reaches the binding as an `or`.
#[test]
fn an_or_filter_over_the_bound_member_reaches_the_binding() {
    let ctx = TestContext::new(full_schema()).unwrap();

    let sql = ctx
        .build_sql(indoc! {r#"
            measures:
              - fpts_sales.amount
            dimensions:
              - fpts_sales.fy_name
            filters:
              - or:
                  - member: fpts_calendar.report_d
                    operator: inDateRange
                    values:
                      - "2024-12-29"
                      - "2025-01-04"
                  - member: fpts_calendar.report_d
                    operator: inDateRange
                    values:
                      - "2023-12-31"
                      - "2024-01-06"
        "#})
        .unwrap();

    let predicates = fact_scan_predicates(&sql);
    assert_eq!(predicates.len(), 1, "sql: {}", sql);
    assert!(
        predicates[0].contains(" OR "),
        "the group keeps the query's own structure\npredicate: {}",
        predicates[0]
    );
    assert_eq!(
        predicates[0].matches("day_d >=").count(),
        2,
        "both branches of the or narrow the scan\npredicate: {}",
        predicates[0]
    );
}

// A calendar shift moves the primary key the fact table joins to, so every row
// the stage reads comes from the shifted period — not only the rows of the
// dimension the shift was asked for. A binding on any member of that calendar
// therefore states nothing in a shifted stage, whether or not the member is a
// date.
#[test]
fn a_calendar_shift_silences_bindings_on_the_calendars_other_members() {
    let ctx = TestContext::new(schema(&[
        BASE,
        PREV_FY,
        "FILTER_PARAMS_COLUMN:fpts_calendar.fy_name:fy_name",
    ]))
    .unwrap();

    let sql = ctx
        .build_sql(indoc! {r#"
            measures:
              - fpts_sales.amount
              - fpts_sales.amount_prev_fy
            time_dimensions:
              - dimension: fpts_calendar.report_d
                granularity: day
                dateRange:
                  - "2024-12-29"
                  - "2025-01-04"
            filters:
              - member: fpts_calendar.fy_name
                operator: equals
                values:
                  - FY3
        "#})
        .unwrap();

    let predicates = fact_scan_predicates(&sql);
    assert_eq!(predicates.len(), 2, "expected two stages\nsql: {}", sql);
    assert_eq!(
        predicates.iter().filter(|p| p.contains("fy_name")).count(),
        1,
        "only the unshifted stage states the fiscal year\npredicates: {:?}",
        predicates
    );
}

// Which shift names exist is the calendar's business, not the bound member's:
// a calendar shift moves the primary key the whole cube joins through, so a
// member that declares no shift of its own is still shifted by one. Reading the
// names off the bound member instead would turn such a binding into an error on
// every query against the model, shifted or not.
#[test]
fn a_binding_on_a_calendar_member_declaring_no_shift_is_not_an_error() {
    let ctx = TestContext::new(schema(&[
        BASE,
        PREV_FY,
        "FILTER_PARAMS:fpts_calendar.plain_d@prev_fy:\
         day_d >= (%0)::timestamptz - interval '364 day' \
         AND day_d <= (%1)::timestamptz - interval '364 day'",
    ]))
    .unwrap();

    let sql = ctx
        .build_sql(indoc! {r#"
            measures:
              - fpts_sales.amount
            time_dimensions:
              - dimension: fpts_calendar.report_d
                granularity: day
                dateRange:
                  - "2024-12-29"
                  - "2025-01-04"
            filters:
              - member: fpts_calendar.plain_d
                operator: inDateRange
                values:
                  - "2024-12-29"
                  - "2025-01-04"
        "#})
        .unwrap();

    let predicates = fact_scan_predicates(&sql);
    assert_eq!(predicates.len(), 1, "expected one stage\nsql: {}", sql);
    // The bound member the query reports by still narrows the scan; the one
    // holding only a shift binding states nothing here, and says so rather than
    // failing the query.
    assert!(
        predicates[0].contains("day_d >=") && predicates[0].contains("1 = 1"),
        "predicate: {}",
        predicates[0]
    );
}

// A column passed as a string is a column, and a calendar-shifted member is not
// that column offset by anything the planner can write.
#[test]
fn a_string_column_cannot_address_a_time_shift() {
    let ctx = TestContext::new(schema(&[
        BASE,
        "FILTER_PARAMS_COLUMN:fpts_calendar.report_d@prev_fy:day_d",
    ]))
    .unwrap();

    let err = ctx.build_sql(SHIFTED_QUERY).unwrap_err();
    assert!(
        err.message.contains("passes a column"),
        "unexpected error: {}",
        err.message
    );
}

#[test]
fn a_binding_naming_an_undeclared_shift_is_rejected() {
    let ctx = TestContext::new(schema(&[
        BASE,
        "FILTER_PARAMS:fpts_calendar.report_d@no_such_shift:day_d >= (%0)::timestamptz",
    ]))
    .unwrap();

    let err = ctx.build_sql(SHIFTED_QUERY).unwrap_err();
    assert!(
        err.message.contains("does not declare"),
        "unexpected error: {}",
        err.message
    );
}

// A shifted band has two ends, and an operator that supplies one describes no
// band for the binding to restate.
#[test]
fn a_shift_binding_needs_a_range_filter() {
    let ctx = TestContext::new(full_schema()).unwrap();

    let err = ctx
        .build_sql(indoc! {r#"
            measures:
              - fpts_sales.amount_prev_fy
            dimensions:
              - fpts_sales.fy_name
            filters:
              - member: fpts_calendar.report_d
                operator: beforeDate
                values:
                  - "2025-01-04"
        "#})
        .unwrap_err();

    assert!(
        err.message.contains("needs a date-range filter"),
        "unexpected error: {}",
        err.message
    );
}

// A measure asking for a named shift the calendar does not declare is a
// modelling mistake, and reads as one.
#[test]
fn an_undeclared_measure_shift_is_reported_to_the_model_author() {
    let schema = MockSchema::from_yaml(
        &SCHEMA
            .replace("__FACT_SQL__", "SELECT * FROM fpts_sales")
            .replace(
                "- name: prev_two_fy\n                  sql:",
                "- name: renamed_prev_two_fy\n                  sql:",
            ),
    )
    .unwrap();
    let ctx = TestContext::new(schema).unwrap();

    let err = ctx
        .build_sql(indoc! {r#"
            measures:
              - fpts_sales.amount_prev_two_fy
            time_dimensions:
              - dimension: fpts_calendar.report_d
                granularity: day
                dateRange:
                  - "2024-12-29"
                  - "2025-01-04"
        "#})
        .unwrap_err();

    assert!(
        err.message
            .contains("Time shift with name prev_two_fy not found"),
        "unexpected error: {}",
        err.message
    );
}

// FILTER_PARAMS only narrows a scan, so a model carrying it has to answer the
// same numbers as one that scans everything. Run over the unshifted measure and
// both calendar-shifted ones at once, so a band a stage failed to read shows up
// as a number rather than as a shape in the SQL.
#[tokio::test(flavor = "multi_thread")]
async fn pushdown_does_not_change_the_answer() {
    let with_pushdown = TestContext::new(full_schema()).unwrap();
    let without = TestContext::new(plain_schema()).unwrap();

    let query = indoc! {r#"
        measures:
          - fpts_sales.amount
          - fpts_sales.amount_prev_fy
          - fpts_sales.amount_prev_two_fy
          - fpts_sales.amount_prev_fy_by_interval
          - fpts_sales.amount_prev_nominal_year
        time_dimensions:
          - dimension: fpts_calendar.report_d
            granularity: day
            dateRange:
              - "2024-12-29"
              - "2025-01-04"
        order:
          - id: fpts_calendar.report_d
    "#};

    let Some(pushed_down) = with_pushdown.try_execute_pg(query, SEED).await else {
        return;
    };
    let full_scan = without
        .try_execute_pg(query, SEED)
        .await
        .expect("the plain model runs wherever the pushdown one does");

    assert_eq!(pushed_down, full_scan);
    insta::assert_snapshot!(pushed_down);
}

// Building a pre-aggregation reaches the cube's sql with no user filters at
// all, and has to come out with a scan of everything rather than an error or an
// empty band.
#[tokio::test(flavor = "multi_thread")]
async fn a_pre_aggregation_over_the_model_builds_and_serves() {
    let ctx = TestContext::new(schema_with_rollup(&[BASE, PREV_FY, PREV_TWO_FY])).unwrap();

    let query = indoc! {r#"
        measures:
          - fpts_sales.amount
        time_dimensions:
          - dimension: fpts_sales.day_d
            granularity: day
            dateRange:
              - "2024-12-29"
              - "2025-01-04"
        order:
          - id: fpts_sales.day_d
    "#};

    let (_, usages) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();
    assert!(!usages.is_empty(), "the query is served from the rollup");

    let Some(result) = ctx.try_execute_pg(query, SEED).await else {
        return;
    };
    insta::assert_snapshot!(result);
}

// A model that binds only the plain column gets no pushdown in its shifted
// stages, and has to keep answering correctly: the plain binding stepping in
// there would leave every shifted measure null.
#[tokio::test(flavor = "multi_thread")]
async fn a_model_without_a_binding_for_its_shift_still_answers() {
    let base_only = TestContext::new(schema(&[BASE])).unwrap();
    let without = TestContext::new(plain_schema()).unwrap();

    let query = indoc! {r#"
        measures:
          - fpts_sales.amount
          - fpts_sales.amount_prev_fy
          - fpts_sales.amount_prev_two_fy
        time_dimensions:
          - dimension: fpts_calendar.report_d
            granularity: day
            dateRange:
              - "2024-12-29"
              - "2025-01-04"
        order:
          - id: fpts_calendar.report_d
    "#};

    let Some(base_only) = base_only.try_execute_pg(query, SEED).await else {
        return;
    };
    let full_scan = without
        .try_execute_pg(query, SEED)
        .await
        .expect("the plain model runs wherever the pushdown one does");

    assert_eq!(base_only, full_scan);
    assert!(
        !base_only.contains("NULL"),
        "the shifted stages found their rows\n{}",
        base_only
    );
}

// The mirror of the fixture above, and the shape a real model takes.
//
// Which way the calendar's own column has to point depends on where the shift
// lands. Above, the join is written as a member reference, so the shift rewrites
// the join key and the column holds the *earlier* date. Here the join names the
// raw column, so the member is left to render in the projection and the filter
// only — and the column has to hold the *later* date, reached by a reverse
// lookup, for the same reporting label to read the earlier rows.
const CUSTOMER_SCHEMA: &str = r#"
cubes:
    - name: cst_calendar
      calendar: true
      sql: "SELECT *, d + interval '364 day' AS d_next_fy, d + interval '728 day' AS d_next_two_fy FROM fpts_calendar"
      dimensions:
          - name: calendar_d
            sql: "{CUBE}.d"
            type: time
            primary_key: true
            time_shift:
                - name: ly
                  sql: "{CUBE}.d_next_fy"
                - name: ly2
                  sql: "{CUBE}.d_next_two_fy"
      measures:
          - name: count
            type: count

    - name: cst_fact
      sql: "__FACT_SQL__"
      joins:
          - name: cst_calendar
            sql: "{CUBE}.day_d = {cst_calendar}.d"
            relationship: many_to_one
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

          - name: amount_ly
            type: number
            multi_stage: true
            sql: "{amount}"
            time_shift:
                - name: ly

          - name: amount_ly2
            type: number
            multi_stage: true
            sql: "{amount}"
            time_shift:
                - name: ly2
"#;

const CUSTOMER_BINDINGS: &str = "FILTER_GROUP|\
                                 FILTER_PARAMS_COLUMN:cst_calendar.calendar_d:day_d|\
                                 FILTER_PARAMS:cst_calendar.calendar_d@ly:\
                                 day_d >= (%0)::timestamptz - interval '364 day' \
                                 AND day_d <= (%1)::timestamptz - interval '364 day'|\
                                 FILTER_PARAMS:cst_calendar.calendar_d@ly2:\
                                 day_d >= (%0)::timestamptz - interval '728 day' \
                                 AND day_d <= (%1)::timestamptz - interval '728 day'";

fn customer_schema(bindings: Option<&str>) -> MockSchema {
    let fact_sql = match bindings {
        Some(bindings) => format!("SELECT * FROM fpts_sales WHERE {{{}}}", bindings),
        None => "SELECT * FROM fpts_sales".to_string(),
    };
    MockSchema::from_yaml(&CUSTOMER_SCHEMA.replace("__FACT_SQL__", &fact_sql)).unwrap()
}

const CUSTOMER_QUERY: &str = indoc! {r#"
    measures:
      - cst_fact.amount
      - cst_fact.amount_ly
      - cst_fact.amount_ly2
    time_dimensions:
      - dimension: cst_calendar.calendar_d
        granularity: day
        dateRange:
          - "2024-12-29"
          - "2025-01-04"
    order:
      - id: cst_calendar.calendar_d
"#};

// One band per stage here too: the rule keys on the cube whose calendar is
// shifted, not on where in the SQL that shift comes out.
#[test]
fn each_stage_binds_its_own_shift_with_the_shift_in_the_projection() {
    let ctx = TestContext::new(customer_schema(Some(CUSTOMER_BINDINGS))).unwrap();
    let sql = ctx.build_sql(CUSTOMER_QUERY).unwrap();
    let predicates = fact_scan_predicates(&sql);

    assert_eq!(predicates.len(), 3, "expected three stages\nsql: {}", sql);
    assert_eq!(
        predicates
            .iter()
            .filter(|p| !p.contains("interval"))
            .count(),
        1,
        "one stage reads the reporting band\npredicates: {:?}",
        predicates
    );
    for band in ["364 day", "728 day"] {
        assert_eq!(
            predicates.iter().filter(|p| p.contains(band)).count(),
            1,
            "exactly one stage reads the band {} back\npredicates: {:?}",
            band,
            predicates
        );
    }
    // The shift comes out in the projection and the filter; the join is left
    // alone, since it names the column rather than the member.
    assert!(
        sql.contains(r#"ON "cst_fact".day_d = "cst_calendar".d"#),
        "the join key is not shifted\nsql: {}",
        sql
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn pushdown_does_not_change_the_answer_with_the_shift_in_the_projection() {
    let with_pushdown = TestContext::new(customer_schema(Some(CUSTOMER_BINDINGS))).unwrap();
    let without = TestContext::new(customer_schema(None)).unwrap();

    let Some(pushed_down) = with_pushdown.try_execute_pg(CUSTOMER_QUERY, SEED).await else {
        return;
    };
    let full_scan = without
        .try_execute_pg(CUSTOMER_QUERY, SEED)
        .await
        .expect("the plain model runs wherever the pushdown one does");

    assert_eq!(pushed_down, full_scan);
    insta::assert_snapshot!(pushed_down);
}
