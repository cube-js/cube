import { PostgresQuery } from '../../src';
import { prepareJsCompiler, prepareYamlCompiler } from './PrepareCompiler';

const cubes = `
cubes:
  - name: dates
    sql_table: dates
    dimensions:
      - name: date
        sql: date
        type: time
        primary_key: true
      - name: is_holiday
        sql: is_holiday
        type: boolean
    measures:
      - name: count
        type: count
    segments:
      - name: holidays
        sql: "{CUBE}.is_holiday"

  - name: other_dates
    sql_table: other_dates
    dimensions:
      - name: date
        sql: date
        type: time
        primary_key: true

  - name: orders
    sql_table: orders
    joins:
      - name: dates
        alias: event_day
        sql: "{CUBE}.event_date = {event_day.date}"
        relationship: many_to_one
      - name: dates
        alias: ship_day
        sql: "{CUBE}.ship_date = {ship_day.date}"
        relationship: many_to_one
      - name: other_dates
        sql: "{CUBE}.event_date = {other_dates.date}"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: order_count
        type: count

  - name: refunds
    sql_table: refunds
    joins:
      - name: dates
        alias: event_day
        sql: "{CUBE}.event_date = {event_day.date}"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: refund_count
        type: count

  - name: returns
    sql_table: returns
    joins:
      - name: dates
        sql: "{CUBE}.returned_on = {dates.date}"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: return_count
        type: count
`;

const withView = (entry: string) => `${cubes}
views:
  - name: activity
    cubes:
      - join_path: orders
        includes:
          - order_count
      - join_path: refunds
        includes:
          - refund_count
      - join_path: returns
        includes:
          - return_count
${entry}
`;

const sharedEventDay = `
      - join_paths:
          - orders.event_day
          - refunds.event_day
          - returns.dates
        alias: event_day
        includes:
          - date
          - is_holiday
`;

async function compile(model: string) {
  const compilers = prepareYamlCompiler(model);
  await compilers.compile();
  return compilers;
}

describe('Views sharing dimensions through join_paths', () => {
  it('includes the dimensions every path reaches, reading the first path', async () => {
    const compilers = await compile(withView(sharedEventDay));
    const view = compilers.cubeEvaluator.cubeFromPath('activity');

    expect(Object.keys(view.dimensions).sort()).toEqual(['date', 'is_holiday']);
    expect((view.dimensions.date as any).sharedPaths).toEqual([
      'orders.event_day.date',
      'refunds.event_day.date',
      'returns.dates.date',
    ]);
    expect(compilers.cubeEvaluator.evaluateReferences(
      'activity',
      view.dimensions.date.sql as any,
      { collectJoinHints: true }
    )).toBe('orders.event_day.date');
  });

  it('keeps the shared paths out of the join map', async () => {
    const compilers = await compile(withView(sharedEventDay));
    expect(compilers.cubeEvaluator.cubeFromPath('activity').joinMap).toEqual([]);
  });

  it('names the members of an entry by its alias', async () => {
    const compilers = await compile(withView(sharedEventDay.replace('alias: event_day', 'alias: day\n        prefix: true')));
    expect(Object.keys(compilers.cubeEvaluator.cubeFromPath('activity').dimensions).sort()).toEqual([
      'day_date',
      'day_is_holiday',
    ]);
  });

  it('defaults the alias to the last segment the paths share', async () => {
    const compilers = await compile(withView(`
      - join_paths:
          - orders.event_day
          - refunds.event_day
        prefix: true
        includes:
          - date
`));
    expect(Object.keys(compilers.cubeEvaluator.cubeFromPath('activity').dimensions)).toEqual(['event_day_date']);
  });

  it('includes every dimension, and nothing else, for "*"', async () => {
    const compilers = await compile(withView(`
      - join_paths:
          - orders.event_day
          - refunds.event_day
        includes: "*"
`));
    const view = compilers.cubeEvaluator.cubeFromPath('activity');
    expect(Object.keys(view.dimensions).sort()).toEqual(['date', 'is_holiday']);
    expect(Object.keys(view.measures).sort()).toEqual(['order_count', 'refund_count', 'return_count']);
    expect(Object.keys(view.segments || {})).toEqual([]);
  });

  it('treats a single path like join_path', async () => {
    const compilers = await compile(withView(`
      - join_paths:
          - orders.event_day
        includes:
          - date
`));
    const view = compilers.cubeEvaluator.cubeFromPath('activity');
    expect((view.dimensions.date as any).sharedPaths).toBeUndefined();
  });

  it('accepts join_paths in the JS model', async () => {
    const compilers = prepareJsCompiler(`
      cube('dates', {
        sql_table: 'dates',
        dimensions: { date: { sql: 'date', type: 'time', primary_key: true } },
      });
      cube('orders', {
        sql_table: 'orders',
        joins: [{ name: 'dates', alias: 'event_day', sql: \`\${CUBE}.event_date = \${event_day.date}\`, relationship: 'many_to_one' }],
        dimensions: { id: { sql: 'id', type: 'number', primary_key: true } },
        measures: { order_count: { type: 'count' } },
      });
      cube('refunds', {
        sql_table: 'refunds',
        joins: [{ name: 'dates', alias: 'event_day', sql: \`\${CUBE}.event_date = \${event_day.date}\`, relationship: 'many_to_one' }],
        dimensions: { id: { sql: 'id', type: 'number', primary_key: true } },
        measures: { refund_count: { type: 'count' } },
      });
      view('activity', {
        cubes: [
          { join_path: orders, includes: ['order_count'] },
          { join_path: refunds, includes: ['refund_count'] },
          { join_paths: [orders.event_day, refunds.event_day], includes: ['date'] },
        ],
      });
    `);
    await compilers.compile();
    expect((compilers.cubeEvaluator.cubeFromPath('activity').dimensions.date as any).sharedPaths).toEqual([
      'orders.event_day.date',
      'refunds.event_day.date',
    ]);
  });

  it('reads each fact through its own path', async () => {
    const compilers = await compile(withView(sharedEventDay));
    const sql = compilers.createQuery(PostgresQuery, {
      measures: ['activity.order_count', 'activity.refund_count', 'activity.return_count'],
      timeDimensions: [{ dimension: 'activity.date', granularity: 'day' }],
      timezone: 'UTC',
    }).buildSqlAndParams()[0];

    expect(sql).toContain('AS "orders__event_day" ON "orders".event_date = "orders__event_day".date');
    expect(sql).toContain('AS "refunds__event_day" ON "refunds".event_date = "refunds__event_day".date');
    expect(sql).toContain('AS "dates" ON "returns".returned_on = "dates".date');
  });
});

describe('join_paths limits', () => {
  it('requires the Tesseract planner', async () => {
    const compilers = await compile(withView(sharedEventDay.replace(/- orders\.event_day\n\s+- refunds\.event_day\n\s+/, '')));
    expect(() => compilers.createQuery(PostgresQuery, {
      measures: ['activity.return_count'],
      dimensions: ['activity.is_holiday'],
      timezone: 'UTC',
      useNativeSqlPlanner: false,
    }).buildSqlAndParams()).not.toThrow();

    const shared = await compile(withView(sharedEventDay));
    expect(() => shared.createQuery(PostgresQuery, {
      measures: ['activity.return_count'],
      dimensions: ['activity.is_holiday'],
      timezone: 'UTC',
      useNativeSqlPlanner: false,
    }).buildSqlAndParams()).toThrow(/shared through join_paths, which require the Tesseract SQL planner/);
  });

  it('rejects paths reaching different cubes', async () => {
    const compilers = prepareYamlCompiler(withView(`
      - join_paths:
          - orders.other_dates
          - refunds.event_day
        alias: day
        includes:
          - date
`));
    await expect(compilers.compile()).rejects.toThrow(/reaching different cubes \('other_dates', 'dates'\)/);
  });

  it('rejects two paths from one cube', async () => {
    const compilers = prepareYamlCompiler(withView(`
      - join_paths:
          - orders.event_day
          - orders.ship_day
        alias: day
        includes:
          - date
`));
    await expect(compilers.compile()).rejects.toThrow(/with more than one path from 'orders'/);
  });

  it('requires an alias when the paths end differently', async () => {
    const compilers = prepareYamlCompiler(withView(`
      - join_paths:
          - orders.event_day
          - returns.dates
        includes:
          - date
`));
    await expect(compilers.compile()).rejects.toThrow(/ending in different names. Add an alias/);
  });

  it('rejects measures and segments', async () => {
    const compilers = prepareYamlCompiler(withView(`
      - join_paths:
          - orders.event_day
          - refunds.event_day
        includes:
          - date
          - count
          - holidays
`));
    const error = await compilers.compile().then(() => null, (e: Error) => e);
    expect(error?.message).toMatch(/includes 'count' of 'dates' through join_paths .* includes dimensions only: include measures/);
    expect(error?.message).toMatch(/includes 'holidays' of 'dates' through join_paths .* include segments/);
    expect(error?.message).not.toMatch(/is included in 'activity' but not defined/);
  });

  it('rejects join_path and join_paths together', async () => {
    const compilers = prepareYamlCompiler(withView(`
      - join_path: orders.event_day
        join_paths:
          - orders.event_day
          - refunds.event_day
        includes:
          - date
`));
    await expect(compilers.compile()).rejects.toThrow(/either join_path or join_paths, not both/);
  });

  it('rejects split', async () => {
    const compilers = prepareYamlCompiler(withView(`
      - join_paths:
          - orders.event_day
          - refunds.event_day
        split: true
        includes:
          - date
`));
    await expect(compilers.compile()).rejects.toThrow(/split together with join_paths is not supported/);
  });
});
