import { PostgresQuery } from '../../src';
import { prepareJsCompiler, prepareYamlCompiler } from './PrepareCompiler';

const cubes = `
cubes:
  - name: calendar
    sql_table: calendar
    dimensions:
      - name: dt_ts
        sql: dt_ts
        type: time
        primary_key: true
      - name: dt_year
        sql: dt_year
        type: string
      - name: dt_year_month
        sql: dt_year_month
        type: string

  - name: subscriptions
    sql_table: subscriptions
    joins:
      - name: calendar
        alias: purchase_cal
        sql: "{CUBE}.purchase_date = {purchase_cal.dt_ts}"
        relationship: many_to_one
      - name: calendar
        alias: cancel_cal
        sql: "{CUBE}.cancel_date = {cancel_cal.dt_ts}"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: cancel_year
        sql: "to_char(cancel_date, 'YYYY')"
        type: string
    measures:
      - name: count
        type: count

  - name: carts
    sql_table: carts
    joins:
      - name: calendar
        sql: "{CUBE}.created_date = {calendar.dt_ts}"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: count
        type: count
`;

const view = (cancellationsOverrides: string) => `
views:
  - name: biz
    cubes:
      - join_path: calendar
        includes: [dt_ts, dt_year, dt_year_month]
      - join_path: subscriptions
        alias: purchases
        prefix: true
        includes: [count]
        dimension_overrides:
          dt_ts: subscriptions.purchase_cal.dt_ts
          dt_year: subscriptions.purchase_cal.dt_year
          dt_year_month: subscriptions.purchase_cal.dt_year_month
      - join_path: subscriptions
        alias: cancellations
        prefix: true
        includes: [count]
        dimension_overrides:
${cancellationsOverrides}
      - join_path: carts
        prefix: true
        includes: [count]
`;

const cancelCalendar = `
          dt_year: subscriptions.cancel_cal.dt_year
          dt_year_month: subscriptions.cancel_cal.dt_year_month`;

describe('View dimension overrides', () => {
  it('attaches the overrides of an include to its measures', async () => {
    const { compiler, cubeEvaluator } = prepareYamlCompiler(cubes + view(cancelCalendar));
    await compiler.compile();

    const { measures } = cubeEvaluator.cubeFromPath('biz');
    expect(measures.purchases_count.dimensionOverrides).toEqual({
      dt_ts: 'subscriptions.purchase_cal.dt_ts',
      dt_year: 'subscriptions.purchase_cal.dt_year',
      dt_year_month: 'subscriptions.purchase_cal.dt_year_month',
    });
    expect(measures.cancellations_count.dimensionOverrides).toEqual({
      dt_year: 'subscriptions.cancel_cal.dt_year',
      dt_year_month: 'subscriptions.cancel_cal.dt_year_month',
    });
    expect(measures.carts_count.dimensionOverrides).toBeUndefined();
  });

  it('passes the overrides of a view measure to the SQL planner', async () => {
    const compilers = prepareYamlCompiler(cubes + view(cancelCalendar));
    await compilers.compiler.compile();

    const [sql] = compilers.createQuery(PostgresQuery, {
      measures: ['biz.cancellations_count'],
      timezone: 'UTC',
    }).buildSqlAndParams();
    expect(sql).toMatch(/FROM\s+subscriptions/);
  });

  it('rejects overrides the query uses under the legacy planner', async () => {
    const compilers = prepareYamlCompiler(cubes + view(cancelCalendar));
    await compilers.compiler.compile();

    expect(() => compilers.createQuery(PostgresQuery, {
      measures: ['biz.cancellations_count'],
      dimensions: ['biz.dt_year_month'],
      timezone: 'UTC',
      useNativeSqlPlanner: false,
    }).buildSqlAndParams()).toThrow(
      /Measure 'biz.cancellations_count' has dimension overrides for dimensions of this query, which require the Tesseract SQL planner/
    );
  });

  it('plans a view measure the query uses no override of under the legacy planner', async () => {
    const compilers = prepareYamlCompiler(cubes + view(cancelCalendar));
    await compilers.compiler.compile();

    const [sql] = compilers.createQuery(PostgresQuery, {
      measures: ['biz.cancellations_count'],
      timezone: 'UTC',
      useNativeSqlPlanner: false,
    }).buildSqlAndParams();
    expect(sql).toMatch(/FROM\s+subscriptions/);
  });

  it('overrides with a dimension of the included cube itself', async () => {
    const { compiler, cubeEvaluator } = prepareYamlCompiler(cubes + view(`
          dt_year: subscriptions.cancel_year`));
    await compiler.compile();

    expect(cubeEvaluator.cubeFromPath('biz').measures.cancellations_count.dimensionOverrides)
      .toEqual({ dt_year: 'subscriptions.cancel_year' });
  });

  it('reads overrides of a JS model', async () => {
    const { compiler, cubeEvaluator } = prepareJsCompiler(`
      cube('calendar', {
        sql_table: 'calendar',
        dimensions: {
          dt_ts: { sql: 'dt_ts', type: 'time', primary_key: true },
          dt_year: { sql: 'dt_year', type: 'string' },
        },
      });

      cube('subscriptions', {
        sql_table: 'subscriptions',
        joins: {
          calendar: { alias: 'cancel_cal', relationship: 'many_to_one', sql: \`\${CUBE}.cancel_date = \${cancel_cal.dt_ts}\` },
        },
        dimensions: { id: { sql: 'id', type: 'number', primary_key: true } },
        measures: { count: { type: 'count' } },
      });

      view('biz', {
        cubes: [
          { join_path: calendar, includes: ['dt_year'] },
          {
            join_path: subscriptions,
            includes: ['count'],
            dimension_overrides: { dt_year: 'subscriptions.cancel_cal.dt_year' },
          },
        ],
      });
    `);
    await compiler.compile();

    expect(cubeEvaluator.cubeFromPath('biz').measures.count.dimensionOverrides)
      .toEqual({ dt_year: 'subscriptions.cancel_cal.dt_year' });
  });

  it.each([
    [
      'a name that is no dimension of the view',
      '          dt_quarter: subscriptions.cancel_cal.dt_year',
      /Dimension override 'dt_quarter' of 'subscriptions' names no dimension of view 'biz'/,
    ],
    [
      'a path outside the join path',
      '          dt_year: calendar.dt_year',
      /Dimension override 'dt_year' of 'subscriptions': 'calendar.dt_year' must start with the join path 'subscriptions'/,
    ],
    [
      'a step that is no join',
      '          dt_year: subscriptions.refund_cal.dt_year',
      /Dimension override 'dt_year' of 'subscriptions': 'subscriptions' has no join 'refund_cal' in 'subscriptions.refund_cal.dt_year'/,
    ],
    [
      'a member that is no dimension',
      '          dt_year: subscriptions.cancel_cal.dt_week',
      /Dimension override 'dt_year' of 'subscriptions': 'subscriptions.cancel_cal.dt_week' names no dimension of 'calendar'/,
    ],
    [
      'a dimension of another type',
      '          dt_year: subscriptions.cancel_cal.dt_ts',
      /Dimension override 'dt_year' of 'subscriptions': 'subscriptions.cancel_cal.dt_ts' is of type 'time' but 'biz.dt_year' is of type 'string'/,
    ],
  ])('rejects %s', async (_name, overrides, error) => {
    const { compiler } = prepareYamlCompiler(cubes + view(overrides));
    await expect(compiler.compile()).rejects.toThrow(error);
  });

  it('rejects overrides on a split include', async () => {
    const { compiler } = prepareYamlCompiler(`${cubes}
views:
  - name: biz
    cubes:
      - join_path: calendar
        includes: [dt_year]
      - join_path: subscriptions
        split: true
        includes: [count]
        dimension_overrides:
          dt_year: subscriptions.cancel_cal.dt_year
`);
    await expect(compiler.compile()).rejects.toThrow(/Dimension overrides of 'subscriptions' can't be used together with split/);
  });
});
