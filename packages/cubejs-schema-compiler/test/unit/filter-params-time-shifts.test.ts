/* eslint-disable no-template-curly-in-string */
import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareJsCompiler, prepareYamlCompiler } from './PrepareCompiler';

// A fact cube narrowing its own scan through FILTER_PARAMS on the time
// dimension of a fiscal calendar. The calendar maps a date onto the matching
// date of the previous fiscal year through a column of its own, so no interval
// over the fact column reproduces the shift — the model has to say which band a
// shifted stage reads, which is what `time_shifts.<name>` is for.
const PLAIN = '${FILTER_PARAMS.fiscal_calendar.reportD.filter(\'day_d\')}';
const SHIFTED = '${FILTER_PARAMS.fiscal_calendar.reportD.time_shifts.prev_fy.filter('
  + '(from, to) => `day_d >= ${from}::timestamptz - interval \'364 day\''
  + ' AND day_d <= ${to}::timestamptz - interval \'364 day\'`)}';

const group = (...bindings: string[]) => `\${FILTER_GROUP(${bindings
  .map(b => b.slice('${'.length, -1))
  .join(', ')})}`;

const modelWith = (bindings: string) => [
  'cube(\'fiscal_calendar\', {',
  '  sql: `SELECT * FROM fiscal_calendar`,',
  '  calendar: true,',
  '  dimensions: {',
  '    d: {',
  '      sql: `${CUBE}.d`,',
  '      type: `time`,',
  '      primaryKey: true,',
  '      timeShift: [{ name: `prev_fy`, sql: `${CUBE.dPrevFy}` }]',
  '    },',
  '    reportD: {',
  '      sql: `${CUBE}.d`,',
  '      type: `time`,',
  '      timeShift: [{ name: `prev_fy`, sql: `${CUBE.dPrevFy}` }]',
  '    },',
  '    dPrevFy: {',
  '      sql: `${CUBE}.d_prev_fy`,',
  '      type: `time`',
  '    }',
  '  }',
  '});',
  'cube(\'sales\', {',
  `  sql: \`SELECT * FROM sales WHERE ${bindings}\`,`,
  '  joins: {',
  '    fiscal_calendar: {',
  '      sql: `${CUBE}.day_d = ${fiscal_calendar.d}`,',
  '      relationship: `belongsTo`',
  '    }',
  '  },',
  '  dimensions: {',
  '    id: {',
  '      sql: `id`,',
  '      type: `number`,',
  '      primaryKey: true',
  '    },',
  '    dayD: {',
  '      sql: `day_d`,',
  '      type: `time`',
  '    }',
  '  },',
  '  measures: {',
  '    amount: {',
  '      sql: `amount`,',
  '      type: `sum`',
  '    },',
  '    amountPrevFy: {',
  '      type: `number`,',
  '      multiStage: true,',
  '      sql: `${amount}`,',
  '      timeShift: [{ name: `prev_fy` }]',
  '    }',
  '  },',
  '  preAggregations: {',
  '    daily: {',
  '      measures: [CUBE.amount],',
  '      timeDimension: CUBE.dayD,',
  '      granularity: `day`',
  '    }',
  '  }',
  '});',
].join('\n');

async function queryFor(bindings: string, useNativeSqlPlanner: boolean, options = {}) {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(modelWith(bindings));
  await compiler.compile();

  return new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
    measures: ['sales.amount', 'sales.amountPrevFy'],
    timeDimensions: [{
      dimension: 'fiscal_calendar.reportD',
      granularity: 'day',
      dateRange: ['2025-01-01', '2025-01-07'],
    }],
    timezone: 'UTC',
    useNativeSqlPlanner,
    ...options,
  });
}

// The band a shifted stage reads, stated by the binding written for that shift.
const SHIFTED_BAND = /day_d >= \$\d+::timestamptz - interval '364 day'/g;
// The reporting band, stated by the plain binding.
const REPORTING_BAND = /day_d >= \$\d+::timestamptz AND day_d <= \$\d+::timestamptz/g;

describe('FILTER_PARAMS addressing a time shift', () => {
  it('binds the shift its stage applies, and the plain column nowhere else', async () => {
    const query = await queryFor(group(PLAIN, SHIFTED), true);
    const [sql] = query.buildSqlAndParams();

    // One stage reads the reporting week, the other the same week of the
    // previous fiscal year. Neither carries the other's band.
    expect(sql.match(SHIFTED_BAND)).toHaveLength(1);
    expect(sql.match(REPORTING_BAND)).toHaveLength(1);
  });

  // The plain binding narrows to the reporting period, which is not where a
  // calendar-shifted stage reads, so it must not step in there.
  it('leaves a shifted stage unrestricted when no binding addresses its shift', async () => {
    const query = await queryFor(PLAIN, true);
    const [sql] = query.buildSqlAndParams();

    expect(sql.match(REPORTING_BAND)).toHaveLength(1);
    expect(sql).toContain('1 = 1');
  });

  it('picks the binding by the shift it names rather than by its position', async () => {
    const [straight] = (await queryFor(group(PLAIN, SHIFTED), true)).buildSqlAndParams();
    const [reversed] = (await queryFor(group(SHIFTED, PLAIN), true)).buildSqlAndParams();

    expect(reversed).toEqual(straight);
  });

  // The legacy planner has no notion of an addressable shift, so a binding for
  // one states nothing there and the plain binding stands everywhere, shifted
  // stages included. What matters is that a model written for the native
  // planner still compiles and answers: the legacy planner is reached on every
  // request, to match and build pre-aggregations.
  it('compiles under the legacy planner, where a shift binding states nothing', async () => {
    const query = await queryFor(group(PLAIN, SHIFTED), false);
    const [sql] = query.buildSqlAndParams();

    expect(sql.match(REPORTING_BAND)).toHaveLength(2);
    expect(sql).not.toMatch(SHIFTED_BAND);
  });

  it('compiles under the legacy planner with only a shift binding', async () => {
    const query = await queryFor(SHIFTED, false);
    const [sql] = query.buildSqlAndParams();

    expect(sql).not.toMatch(SHIFTED_BAND);
    expect(sql).toContain('1 = 1');
  });

  // Every string under `time_shifts` reads as a shift name, so leaving the name
  // off has to say so rather than fail somewhere inside the compiler.
  describe.each([
    ['legacy planner', false],
    ['native planner', true],
  ])('%s', (_name, useNativeSqlPlanner) => {
    it.each([
      ['coerced without a shift name', '${FILTER_PARAMS.fiscal_calendar.reportD.time_shifts}'],
      ['given filter as the shift name', '${FILTER_PARAMS.fiscal_calendar.reportD.time_shifts.filter(\'day_d\')}'],
    ])('reports time_shifts %s', async (_case, binding) => {
      const query = await queryFor(binding, useNativeSqlPlanner);

      expect(() => query.buildSqlAndParams()).toThrow(/needs the name of a time shift/);
    });
  });

  // A YAML model states the callback as a Python lambda, which is what the
  // documented form uses — and `from` is a keyword there, so the bounds cannot
  // even be named the way the JavaScript form names them.
  it('binds the shift from a YAML model', async () => {
    const model = `
cubes:
  - name: fiscal_calendar
    calendar: true
    sql: "SELECT '2025-01-01'::timestamp AS d, '2024-01-03'::timestamp AS d_prev_fy"
    dimensions:
      - name: d
        sql: "{CUBE}.d"
        type: time
        primary_key: true
        time_shift: &shifts
          - name: prev_fy
            sql: "{CUBE.d_prev_fy}"
      - name: report_d
        sql: "{CUBE}.d"
        type: time
        time_shift: *shifts
      - name: d_prev_fy
        sql: "{CUBE}.d_prev_fy"
        type: time
  - name: sales
    sql: |
      SELECT * FROM sales WHERE {FILTER_GROUP(
        FILTER_PARAMS.fiscal_calendar.report_d.filter('day_d'),
        FILTER_PARAMS.fiscal_calendar.report_d.time_shifts.prev_fy.filter(
          lambda x, y: f"day_d >= {x}::timestamptz - interval '364 day' AND day_d <= {y}::timestamptz - interval '364 day'"
        )
      )}
    joins:
      - name: fiscal_calendar
        sql: "{CUBE}.day_d = {fiscal_calendar.d}"
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
        sql: amount
        type: sum
      - name: amount_prev_fy
        type: number
        multi_stage: true
        sql: "{amount}"
        time_shift:
          - name: prev_fy
`;
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(model);
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['sales.amount', 'sales.amount_prev_fy'],
      timeDimensions: [{
        dimension: 'fiscal_calendar.report_d',
        granularity: 'day',
        dateRange: ['2025-01-01', '2025-01-07'],
      }],
      timezone: 'UTC',
    });
    const [sql] = query.buildSqlAndParams();

    expect(sql.match(SHIFTED_BAND)).toHaveLength(1);
    expect(sql.match(REPORTING_BAND)).toHaveLength(1);
  });

  // A build query carries no user filters, so the group has nothing to state
  // and the scan stays open.
  describe.each([
    ['legacy planner', false],
    ['native planner', true],
  ])('%s', (_name, useNativeSqlPlanner) => {
    it('builds a pre-aggregation over the model', async () => {
      const query = await queryFor(group(PLAIN, SHIFTED), useNativeSqlPlanner, {
        measures: ['sales.amount'],
        timeDimensions: [{
          dimension: 'sales.dayD',
          granularity: 'day',
          dateRange: ['2025-01-01', '2025-01-07'],
        }],
      });
      const [description]: any = query.preAggregations?.preAggregationsDescription();
      const [loadSql] = description.loadSql;

      expect(loadSql).toContain('1 = 1');
      expect(loadSql).not.toMatch(SHIFTED_BAND);
    });
  });
});
