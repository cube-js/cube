import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// A fact whose `sql` pushes a joined CALENDAR cube's date down through
// FILTER_PARAMS, with multi_stage measures that shift that date by NAME.
//
// Named shifts are what a retail 4-5-4 calendar forces: "one fiscal year back"
// is a mapping column on the calendar, not an interval any arithmetic can
// express, so the shift is declared as `time_shift: [{ name, sql }]` on the
// calendar dimension and referenced by name from the measure.
//
// Compare `multi-stage-time-shift-filter-params.test.ts` (CORE-543 / #11030),
// which covers the INTERVAL form: there the pushed-down column is offset by the
// same interval as the stage predicate, so a shifted stage scans the rows it
// groups by. A named shift gets no such treatment — see the last test here.
const CALENDAR_SHIFT_MODEL = `
cubes:
  - name: fpc_calendar
    calendar: true
    sql: >
      SELECT '2026-06-20'::date AS calendar_d,
             '2025-06-21'::date AS next_fiscal_year_d,
             '2024-06-22'::date AS next_two_fiscal_year_d
    dimensions:
      - name: calendar_d
        sql: calendar_d
        type: time
        primary_key: true
        time_shift:
          - name: prior_fiscal_year
            sql: "{CUBE}.next_fiscal_year_d"
          - name: prior_two_fiscal_year
            sql: "{CUBE}.next_two_fiscal_year_d"

  - name: fpc_margin
    sql: >
      SELECT * FROM fpc_margin
      WHERE {FILTER_PARAMS.fpc_calendar.calendar_d.filter('week_end_d')}
    joins:
      - name: fpc_calendar
        sql: "{CUBE}.week_end_d = {fpc_calendar.calendar_d}"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: week_end_d
        sql: week_end_d
        type: time
    measures:
      - name: net_sales
        sql: net_sales_a
        type: sum
      - name: net_sales_ly
        multi_stage: true
        sql: "{net_sales}"
        type: number
        time_shift:
          - name: prior_fiscal_year
      - name: net_sales_ly2
        multi_stage: true
        sql: "{net_sales}"
        type: number
        time_shift:
          - name: prior_two_fiscal_year
`;

// The same push-down, shifted by an interval instead of by name.
const INTERVAL_SHIFT_MODEL = `
cubes:
  - name: fpi_margin
    sql: >
      SELECT * FROM fpi_margin
      WHERE {FILTER_PARAMS.fpi_margin.week_end_d.filter('week_end_d')}
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: week_end_d
        sql: week_end_d
        type: time
    measures:
      - name: net_sales
        sql: net_sales_a
        type: sum
      - name: net_sales_ly
        multi_stage: true
        sql: "{net_sales}"
        type: number
        time_shift:
          - time_dimension: week_end_d
            interval: 1 year
            type: prior
`;

async function buildSql(model: string, query: any): Promise<[string, unknown[]]> {
  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(model);
  await compiler.compile();

  return new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
    timezone: 'UTC',
    // Named calendar time shifts are planned by Tesseract only.
    useNativeSqlPlanner: true,
    ...query,
  }).buildSqlAndParams();
}

const calendarShiftSql = () => buildSql(CALENDAR_SHIFT_MODEL, {
  measures: ['fpc_margin.net_sales_ly', 'fpc_margin.net_sales_ly2'],
  timeDimensions: [{
    dimension: 'fpc_calendar.calendar_d',
    dateRange: ['2026-06-20', '2026-06-20'],
  }],
});

describe('FILTER_PARAMS under a named calendar time shift', () => {
  // Each named shift builds its own stage, and each stage rescans the fact. The
  // binding stays attributed to `fpc_calendar.calendar_d` — the shift
  // substitutes the column only where the stage's own predicate renders — so
  // FILTER_PARAMS keeps matching and no stage scans the fact unfiltered.
  it('pushes the column into every shifted stage', async () => {
    const [sql] = await calendarShiftSql();

    expect(sql.match(/FROM fpc_margin WHERE \(week_end_d >=/g)).toHaveLength(2);
    expect(sql).not.toContain('FROM fpc_margin WHERE (1 = 1)');
  });

  // The stage predicate is what carries the shift: each stage compares the
  // calendar's mapping column, not `calendar_d`.
  it('filters each stage on its own mapping column', async () => {
    const [sql] = await calendarShiftSql();

    expect(sql).toContain('"fpc_calendar".next_fiscal_year_d >=');
    expect(sql).toContain('"fpc_calendar".next_two_fiscal_year_d >=');
  });

  // The interval form, for contrast: the pushed-down column is offset so the
  // rows the shifted stage scans line up with the bounds it groups by.
  it('offsets the pushed-down column for an interval shift', async () => {
    const [sql] = await buildSql(INTERVAL_SHIFT_MODEL, {
      measures: ['fpi_margin.net_sales', 'fpi_margin.net_sales_ly'],
      timeDimensions: [{
        dimension: 'fpi_margin.week_end_d',
        dateRange: ['2026-06-20', '2026-06-20'],
      }],
    });

    expect(sql).toContain('(week_end_d + interval \'1 year\')');
  });

  // The gap.
  //
  // A named shift never reaches the offsetting branch above. In
  // `base_filter.rs` the shift handed to `to_sql_for_filter_params` is
  // `time_shifts().get_for_symbol(sym).and_then(|s| s.interval.as_ref())`, and a
  // named calendar shift carries `interval: None` — so the column is rendered
  // bare and bound to the UNSHIFTED reporting bounds, in every stage.
  //
  // That contradicts the stage around it. `cte_0` joins the calendar on
  // `week_end_d = next_fiscal_year_d`, so it reads PRIOR-year fact rows, while
  // the pushed-down predicate restricts the same scan to the REPORTING week.
  // The stage comes back empty unless the model widens the pushed-down band by
  // hand to cover the shifted periods — and once it does, every stage scans
  // every band.
  it('binds the pushed-down column to unshifted bounds', async () => {
    const [sql, params] = await calendarShiftSql();

    expect(sql).not.toContain('week_end_d + interval');

    // Eight bounds: a pushed-down pair and a stage pair per shifted stage.
    // Every one of them is the reporting day, so nothing distinguishes the scan
    // of the prior-fiscal-year stage from the scan of the two-year one.
    expect(params).toHaveLength(8);
    expect(params.every((p) => String(p).startsWith('2026-06-20'))).toBe(true);
  });
});
