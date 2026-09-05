import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// A calendar shift declared with `sql` maps through a column on the calendar
// (`prior_fiscal_year` -> `next_fiscal_year_d`) rather than offsetting the date,
// so a string FILTER_PARAMS column is rejected and a callback — handed the
// query's own bounds — is not. Interval-declared shifts are offset onto the
// column instead; that shape is pinned in the planner suite.
const model = (filterParams: string) => `
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
      SELECT * FROM fpc_margin WHERE ${filterParams}
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

const STRING_COLUMN = '{FILTER_PARAMS.fpc_calendar.calendar_d.filter(\'week_end_d\')}';

// The band a model writes by hand once it knows the shifted periods it has to
// cover. 371/364 days back brackets the fiscal prior year. YAML `.filter()`
// bodies are Python, so this is a lambda — the same form the reporting models
// that hit this use.
const CALLBACK_COLUMN = '{FILTER_PARAMS.fpc_calendar.calendar_d.filter('
  + 'lambda x, y: f"(week_end_d >= {x} AND week_end_d <= {y}) '
  + 'OR (week_end_d >= {x}::timestamptz - interval \'371 day\' '
  + 'AND week_end_d <= {y}::timestamptz - interval \'364 day\')")}';

async function buildSql(filterParams: string): Promise<[string, unknown[]]> {
  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(model(filterParams));
  await compiler.compile();

  return new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
    measures: ['fpc_margin.net_sales_ly', 'fpc_margin.net_sales_ly2'],
    timeDimensions: [{
      dimension: 'fpc_calendar.calendar_d',
      dateRange: ['2026-06-20', '2026-06-20'],
    }],
    timezone: 'UTC',
    // Named calendar time shifts are planned by Tesseract only.
    useNativeSqlPlanner: true,
  }).buildSqlAndParams();
}

describe('FILTER_PARAMS under a named calendar time shift', () => {
  // Before this was rejected the column rendered bare and was bound to the
  // unshifted reporting bounds in every stage. Each stage joins the calendar on
  // its own mapping column, so the pushed-down predicate contradicted the stage
  // around it and the stage came back empty — the same failure CORE-543 fixed
  // for interval shifts, reached by a different route.
  it('rejects a string column', async () => {
    await expect(buildSql(STRING_COLUMN)).rejects.toThrow(
      /fpc_calendar\.calendar_d.*prior_fiscal_year.*callback/s
    );
  });

  // A callback column is handed the query's bounds and decides the range
  // itself, so it is left alone. This is what a model widened by hand relies
  // on, and it must keep working.
  it('pushes a callback column into every shifted stage', async () => {
    const [sql] = await buildSql(CALLBACK_COLUMN);

    expect(sql.match(/week_end_d >=/g)).toHaveLength(4);
    expect(sql).not.toContain('FROM fpc_margin WHERE (1 = 1)');
  });

  // The stage predicate is what carries the shift: each stage compares the
  // calendar's own mapping column, not `calendar_d`.
  it('filters each stage on its own mapping column', async () => {
    const [sql] = await buildSql(CALLBACK_COLUMN);

    expect(sql).toContain('next_fiscal_year_d >=');
    expect(sql).toContain('next_two_fiscal_year_d >=');
  });
});
