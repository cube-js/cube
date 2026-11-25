import { getEnv } from '@cubejs-backend/shared';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// Dimension overrides are applied by the Tesseract planner; the legacy planner rejects them.
const tesseract = getEnv('nativeSqlPlanner');

(tesseract ? describe : describe.skip)('View dimension overrides', () => {
  jest.setTimeout(200000);

  // 2026 purchases: 3 (2026-01), 4 (2026-02). 2026 cancellations: 1, 3 (both 2026-02).
  // 2026 carts: 1, 2 (both 2026-02).
  const model = (withRollups: boolean) => `
cubes:
  - name: calendar
    sql: >
      SELECT d::date AS dt_ts, to_char(d, 'YYYY') AS dt_year, to_char(d, 'YYYY-MM') AS dt_year_month
      FROM generate_series(DATE '2025-01-01', DATE '2026-12-31', INTERVAL '1 day') d
    dimensions:
      - {name: dt_ts, sql: dt_ts, type: time, primary_key: true}
      - {name: dt_year, sql: dt_year, type: string}
      - {name: dt_year_month, sql: dt_year_month, type: string}

  - name: subscriptions
    sql: >
      SELECT * FROM (VALUES
        (1, '2025-03-10'::date, '2026-02-05'::date),
        (2, '2025-06-01'::date, '2025-09-01'::date),
        (3, '2026-01-15'::date, '2026-02-20'::date),
        (4, '2026-02-01'::date, NULL::date)
      ) AS t(id, purchase_date, cancel_date)
    joins:
      - {name: calendar, alias: purchase_cal, relationship: many_to_one, sql: "{CUBE}.purchase_date = {purchase_cal.dt_ts}"}
      - {name: calendar, alias: cancel_cal, relationship: many_to_one, sql: "{CUBE}.cancel_date = {cancel_cal.dt_ts}"}
    dimensions:
      - {name: id, sql: id, type: number, primary_key: true}
    measures:
      - {name: count, type: count}
      - name: cancellations
        type: count
        filters: [{sql: "{CUBE}.cancel_date IS NOT NULL"}]
      - name: cancellations_cumulative
        type: count
        filters: [{sql: "{CUBE}.cancel_date IS NOT NULL"}]
        rolling_window: {trailing: unbounded}
      - name: cancellations_prev_year
        type: number
        multi_stage: true
        sql: "{CUBE.cancellations}"
        time_shift: [{interval: 1 year, type: prior}]
${withRollups ? `    pre_aggregations:
      - name: counts_by_purchase_month
        measures: [count]
        dimensions: [purchase_cal.dt_year, purchase_cal.dt_year_month]
      - name: cancellations_by_cancel_month
        measures: [cancellations]
        dimensions: [cancel_cal.dt_year, cancel_cal.dt_year_month]
` : ''}
  - name: carts
    sql: >
      SELECT * FROM (VALUES (1, '2026-02-03'::date), (2, '2026-02-10'::date), (3, '2025-09-01'::date))
      AS t(id, created_date)
    joins:
      - {name: calendar, relationship: many_to_one, sql: "{CUBE}.created_date = {calendar.dt_ts}"}
    dimensions:
      - {name: id, sql: id, type: number, primary_key: true}
    measures:
      - {name: count, type: count}
${withRollups ? `    pre_aggregations:
      - name: counts_by_month
        measures: [count]
        dimensions: [calendar.dt_year, calendar.dt_year_month]
` : ''}
views:
  - name: biz
    segments:
      - name: year_2026
        sql: "{CUBE.dt_year} = '2026'"
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
        includes: [count, cancellations, cancellations_cumulative, cancellations_prev_year]
        dimension_overrides:
          dt_ts: subscriptions.cancel_cal.dt_ts
          dt_year: subscriptions.cancel_cal.dt_year
          dt_year_month: subscriptions.cancel_cal.dt_year_month
      - join_path: carts
        prefix: true
        includes: [count]
`;

  const compilers = prepareYamlCompiler(model(false));
  const compilersWithRollups = prepareYamlCompiler(model(true));

  const year2026 = { member: 'biz.dt_year', operator: 'equals', values: ['2026'] };

  it('filters a measure on its own date', async () => dbRunner.runQueryTest({
    measures: ['biz.cancellations_cancellations'],
    filters: [year2026],
    timezone: 'UTC',
  }, [
    { biz__cancellations_cancellations: '2' },
  ], compilers));

  it('groups a measure by its own date', async () => dbRunner.runQueryTest({
    measures: ['biz.purchases_count'],
    dimensions: ['biz.dt_year_month'],
    filters: [year2026],
    order: [{ id: 'biz.dt_year_month' }],
    timezone: 'UTC',
  }, [
    { biz__dt_year_month: '2026-01', biz__purchases_count: '1' },
    { biz__dt_year_month: '2026-02', biz__purchases_count: '1' },
  ], compilers));

  it('puts each measure on its own date in one query', async () => dbRunner.runQueryTest({
    measures: ['biz.purchases_count', 'biz.cancellations_cancellations', 'biz.carts_count'],
    dimensions: ['biz.dt_year_month'],
    filters: [year2026],
    order: [{ id: 'biz.dt_year_month' }],
    timezone: 'UTC',
  }, [
    {
      biz__dt_year_month: '2026-01',
      biz__purchases_count: '1',
      biz__cancellations_cancellations: null,
      biz__carts_count: null,
    },
    {
      biz__dt_year_month: '2026-02',
      biz__purchases_count: '1',
      biz__cancellations_cancellations: '2',
      biz__carts_count: '2',
    },
  ], compilers));

  it('keeps one filter from narrowing two roles of one fact to each other', async () => dbRunner.runQueryTest({
    measures: ['biz.purchases_count', 'biz.cancellations_cancellations'],
    filters: [year2026],
    timezone: 'UTC',
  }, [
    { biz__purchases_count: '2', biz__cancellations_cancellations: '2' },
  ], compilers));

  it('applies a view segment on each measure\'s own date', async () => dbRunner.runQueryTest({
    measures: ['biz.purchases_count', 'biz.cancellations_cancellations', 'biz.carts_count'],
    segments: ['biz.year_2026'],
    timezone: 'UTC',
  }, [
    { biz__purchases_count: '2', biz__cancellations_cancellations: '2', biz__carts_count: '2' },
  ], compilers));

  it('reads one cube measure under two roles', async () => dbRunner.runQueryTest({
    measures: ['biz.purchases_count', 'biz.cancellations_count'],
    dimensions: ['biz.dt_year_month'],
    filters: [year2026],
    order: [{ id: 'biz.dt_year_month' }],
    timezone: 'UTC',
  }, [
    { biz__dt_year_month: '2026-01', biz__purchases_count: '1', biz__cancellations_count: null },
    { biz__dt_year_month: '2026-02', biz__purchases_count: '1', biz__cancellations_count: '2' },
  ], compilers));

  it('groups two roles by a time dimension granularity', async () => dbRunner.runQueryTest({
    measures: ['biz.purchases_count', 'biz.cancellations_cancellations'],
    timeDimensions: [{ dimension: 'biz.dt_ts', granularity: 'month', dateRange: ['2026-01-01', '2026-12-31'] }],
    order: [{ id: 'biz.dt_ts' }],
    timezone: 'UTC',
  }, [
    { biz__dt_ts_month: '2026-01-01T00:00:00.000Z', biz__purchases_count: '1', biz__cancellations_cancellations: null },
    { biz__dt_ts_month: '2026-02-01T00:00:00.000Z', biz__purchases_count: '1', biz__cancellations_cancellations: '2' },
  ], compilers));

  it('reads each leg from the rollup of its own date role', async () => {
    await compilersWithRollups.compiler.compile();
    const query = new PostgresQuery(compilersWithRollups, {
      measures: ['biz.purchases_count', 'biz.cancellations_cancellations', 'biz.carts_count'],
      dimensions: ['biz.dt_year_month'],
      filters: [year2026],
      order: [{ id: 'biz.dt_year_month' }],
      timezone: 'UTC',
      preAggregationsSchema: '',
    });

    const used = query.preAggregations?.preAggregationsDescription().map((d: any) => d.tableName).sort();
    expect(used).toEqual([
      'carts_counts_by_month',
      'subscriptions_cancellations_by_cancel_month',
      'subscriptions_counts_by_purchase_month',
    ]);
    expect(await dbRunner.evaluateQueryWithPreAggregations(query)).toEqual([
      {
        biz__dt_year_month: '2026-01',
        biz__purchases_count: '1',
        biz__cancellations_cancellations: null,
        biz__carts_count: null,
      },
      {
        biz__dt_year_month: '2026-02',
        biz__purchases_count: '1',
        biz__cancellations_cancellations: '2',
        biz__carts_count: '2',
      },
    ]);
  });

  it('rolls a cube measure up on its own date', async () => dbRunner.runQueryTest({
    measures: ['biz.purchases_count', 'biz.cancellations_cancellations_cumulative'],
    timeDimensions: [{ dimension: 'biz.dt_ts', granularity: 'month', dateRange: ['2026-01-01', '2026-03-31'] }],
    order: [{ id: 'biz.dt_ts' }],
    timezone: 'UTC',
  }, [
    { biz__dt_ts_month: '2026-01-01T00:00:00.000Z', biz__purchases_count: '1', biz__cancellations_cancellations_cumulative: '1' },
    { biz__dt_ts_month: '2026-02-01T00:00:00.000Z', biz__purchases_count: '1', biz__cancellations_cancellations_cumulative: '3' },
    { biz__dt_ts_month: '2026-03-01T00:00:00.000Z', biz__purchases_count: null, biz__cancellations_cancellations_cumulative: '3' },
  ], compilers));

  it('shifts a cube measure on its own date', async () => dbRunner.runQueryTest({
    measures: ['biz.carts_count', 'biz.cancellations_cancellations_prev_year'],
    timeDimensions: [{ dimension: 'biz.dt_ts', granularity: 'year', dateRange: ['2026-01-01', '2026-12-31'] }],
    timezone: 'UTC',
  }, [
    { biz__dt_ts_year: '2026-01-01T00:00:00.000Z', biz__carts_count: '2', biz__cancellations_cancellations_prev_year: '1' },
  ], compilers));
});

// One fact blended with itself: flights are departures by their origin and
// arrivals by their destination. Departures: SFO 3, JFK 2, LHR 1; arrivals:
// SFO 2, JFK 2, LHR 2.
(tesseract ? describe : describe.skip)('View dimension overrides of one fact', () => {
  jest.setTimeout(200000);

  const compilers = prepareYamlCompiler(`
cubes:
  - name: airports
    sql: >
      SELECT * FROM (VALUES ('SFO', 'US'), ('JFK', 'US'), ('LHR', 'GB')) AS t(code, country)
    dimensions:
      - {name: code, sql: code, type: string, primary_key: true}
      - {name: country, sql: country, type: string}

  - name: flights
    sql: >
      SELECT * FROM (VALUES
        (1, 'SFO', 'JFK'), (2, 'SFO', 'LHR'), (3, 'JFK', 'SFO'),
        (4, 'LHR', 'SFO'), (5, 'JFK', 'LHR'), (6, 'SFO', 'JFK')
      ) AS t(id, origin, destination)
    joins:
      - {name: airports, alias: origin_airport, relationship: many_to_one, sql: "{CUBE}.origin = {origin_airport.code}"}
      - {name: airports, alias: destination_airport, relationship: many_to_one, sql: "{CUBE}.destination = {destination_airport.code}"}
    dimensions:
      - {name: id, sql: id, type: number, primary_key: true}
      - {name: origin, sql: origin, type: string}
      - {name: destination, sql: destination, type: string}
    measures:
      - {name: count, type: count}

views:
  - name: airport_traffic
    cubes:
      - join_path: airports
        includes: [code, country]
      - join_path: flights
        alias: departures
        prefix: true
        includes: [count]
        dimension_overrides:
          code: flights.origin_airport.code
          country: flights.origin_airport.country
      - join_path: flights
        alias: arrivals
        prefix: true
        includes: [count]
        dimension_overrides:
          code: flights.destination_airport.code
          country: flights.destination_airport.country

  - name: route_ends
    cubes:
      - join_path: flights
        includes:
          - name: origin
            alias: airport
      - join_path: flights
        alias: departures
        prefix: true
        includes: [count]
      - join_path: flights
        alias: arrivals
        prefix: true
        includes: [count]
        dimension_overrides:
          airport: flights.destination
`);

  it('blends departures and arrivals by airport through a joined cube', async () => dbRunner.runQueryTest({
    measures: ['airport_traffic.departures_count', 'airport_traffic.arrivals_count'],
    dimensions: ['airport_traffic.code'],
    filters: [{ member: 'airport_traffic.country', operator: 'equals', values: ['US'] }],
    order: [{ id: 'airport_traffic.code' }],
    timezone: 'UTC',
  }, [
    { airport_traffic__code: 'JFK', airport_traffic__departures_count: '2', airport_traffic__arrivals_count: '2' },
    { airport_traffic__code: 'SFO', airport_traffic__departures_count: '3', airport_traffic__arrivals_count: '2' },
  ], compilers));

  it('blends departures and arrivals by the fact\'s own columns', async () => dbRunner.runQueryTest({
    measures: ['route_ends.departures_count', 'route_ends.arrivals_count'],
    dimensions: ['route_ends.airport'],
    order: [{ id: 'route_ends.airport' }],
    timezone: 'UTC',
  }, [
    { route_ends__airport: 'JFK', route_ends__departures_count: '2', route_ends__arrivals_count: '2' },
    { route_ends__airport: 'LHR', route_ends__departures_count: '1', route_ends__arrivals_count: '2' },
    { route_ends__airport: 'SFO', route_ends__departures_count: '3', route_ends__arrivals_count: '2' },
  ], compilers));
});

// Two facts with no join and no shared dimension cube, related by a view:
// income and expenses, each by its own date and branch.
(tesseract ? describe : describe.skip)('View dimension overrides of unrelated facts', () => {
  jest.setTimeout(200000);

  const compilers = prepareYamlCompiler(`
cubes:
  - name: income
    sql: >
      SELECT * FROM (VALUES
        (1, 'North', '2026-01-05'::timestamp, 100),
        (2, 'North', '2026-01-20'::timestamp, 50),
        (3, 'South', '2026-01-10'::timestamp, 70),
        (4, 'South', '2026-02-03'::timestamp, 30),
        (5, 'North', '2026-02-14'::timestamp, 20)
      ) AS t(id, branch, received_at, amount)
    dimensions:
      - {name: id, sql: id, type: number, primary_key: true}
      - {name: branch, sql: branch, type: string}
      - {name: received_at, sql: received_at, type: time}
    measures:
      - {name: amount, sql: amount, type: sum}

  - name: expenses
    sql: >
      SELECT * FROM (VALUES
        (1, 'North', '2026-01-07'::timestamp, 40),
        (2, 'South', '2026-01-15'::timestamp, 25),
        (3, 'South', '2026-02-20'::timestamp, 60),
        (4, 'East', '2026-02-01'::timestamp, 10)
      ) AS t(id, branch, spent_at, amount)
    dimensions:
      - {name: id, sql: id, type: number, primary_key: true}
      - {name: branch, sql: branch, type: string}
      - {name: spent_at, sql: spent_at, type: time}
    measures:
      - {name: amount, sql: amount, type: sum}

views:
  - name: finance
    measures:
      - name: balance
        type: number
        sql: "COALESCE({CUBE.income}, 0) - COALESCE({CUBE.expenses}, 0)"
    cubes:
      - join_path: income
        includes:
          - {name: received_at, alias: date}
          - branch
          - {name: amount, alias: income}
      - join_path: expenses
        includes:
          - {name: amount, alias: expenses}
        dimension_overrides:
          date: expenses.spent_at
          branch: expenses.branch
`);

  it('blends income and expenses by month and branch', async () => dbRunner.runQueryTest({
    measures: ['finance.income', 'finance.expenses'],
    dimensions: ['finance.branch'],
    timeDimensions: [{ dimension: 'finance.date', granularity: 'month', dateRange: ['2026-01-01', '2026-02-28'] }],
    order: [{ id: 'finance.date' }, { id: 'finance.branch' }],
    timezone: 'UTC',
  }, [
    { finance__branch: 'North', finance__date_month: '2026-01-01T00:00:00.000Z', finance__income: '150', finance__expenses: '40' },
    { finance__branch: 'South', finance__date_month: '2026-01-01T00:00:00.000Z', finance__income: '70', finance__expenses: '25' },
    { finance__branch: 'East', finance__date_month: '2026-02-01T00:00:00.000Z', finance__income: null, finance__expenses: '10' },
    { finance__branch: 'North', finance__date_month: '2026-02-01T00:00:00.000Z', finance__income: '20', finance__expenses: null },
    { finance__branch: 'South', finance__date_month: '2026-02-01T00:00:00.000Z', finance__income: '30', finance__expenses: '60' },
  ], compilers));

  it('computes a view measure over both facts', async () => dbRunner.runQueryTest({
    measures: ['finance.balance'],
    dimensions: ['finance.branch'],
    order: [{ id: 'finance.branch' }],
    timezone: 'UTC',
  }, [
    { finance__branch: 'East', finance__balance: '-10' },
    { finance__branch: 'North', finance__balance: '130' },
    { finance__branch: 'South', finance__balance: '15' },
  ], compilers));

  it('blends income and expenses of one branch by month', async () => dbRunner.runQueryTest({
    measures: ['finance.income', 'finance.expenses'],
    timeDimensions: [{ dimension: 'finance.date', granularity: 'month' }],
    filters: [{ member: 'finance.branch', operator: 'equals', values: ['South'] }],
    order: [{ id: 'finance.date' }],
    timezone: 'UTC',
  }, [
    { finance__date_month: '2026-01-01T00:00:00.000Z', finance__income: '70', finance__expenses: '25' },
    { finance__date_month: '2026-02-01T00:00:00.000Z', finance__income: '30', finance__expenses: '60' },
  ], compilers));
});
