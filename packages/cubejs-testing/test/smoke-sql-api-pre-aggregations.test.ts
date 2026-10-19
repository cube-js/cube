// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, describe, expect, jest, test } from '@jest/globals';
import fetch from 'node-fetch';
import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_API_TOKEN,
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
  JEST_BEFORE_ALL_DEFAULT_TIMEOUT,
  stopIfStarted,
} from './smoke-tests';

type CubeSqlResult = {
  rows: unknown[][];
  usedPreAggregationIds: string[];
};

// With SQL push down to Cube Store off, a rollup match must be read from Cube Store and a
// pushed-down query must run on DuckDB: sending source SQL to Cube Store fails with
// "Inline table orders was not found".
describe('SQL API over pre-aggregations of the tutorial project', () => {
  jest.setTimeout(60 * 5 * 1000);
  let birdbox: BirdBox;

  beforeAll(async () => {
    birdbox = await getBirdbox(
      'duckdb',
      {
        CUBEJS_DB_TYPE: 'duckdb',
        ...DEFAULT_CONFIG,
        CUBEJS_PG_SQL_PORT: '5656',
        CUBESQL_SQL_PUSH_DOWN: 'true',
        CUBESQL_SQL_PUSH_DOWN_PRE_AGGREGATIONS: 'false',
      },
      {
        schemaDir: 'sql-api-pre-aggregations/schema',
        cubejsConfig: 'sql-api-pre-aggregations/cube.py',
      }
    );
  }, JEST_BEFORE_ALL_DEFAULT_TIMEOUT);

  afterAll(async () => {
    await stopIfStarted('birdbox', birdbox);
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

  async function cubeSql(query: string, options: { cache?: string, timezone?: string } = {}): Promise<CubeSqlResult> {
    const response = await fetch(`${birdbox.configuration.apiUrl}/cubesql`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: DEFAULT_API_TOKEN,
      },
      body: JSON.stringify({ query, ...options }),
    });
    const text = await response.text();
    if (!response.ok && !text.trim().startsWith('{')) {
      throw new Error(`${query}: HTTP ${response.status}: ${text.slice(0, 500)}`);
    }

    const lines = text
      .split('\n')
      .filter((line) => line.trim())
      .map((line) => JSON.parse(line));

    const error = lines.find((line) => line.error);
    if (error) {
      throw new Error(`${query}: ${error.error}`);
    }

    const [head, ...chunks] = lines;
    return {
      rows: chunks.flatMap((chunk) => chunk.data),
      usedPreAggregationIds: Object.values<{ preAggregationId: string }>(head.usedPreAggregations || {})
        .map((preAggregation) => preAggregation.preAggregationId),
    };
  }

  const total = (rows: unknown[][], column: number) => rows.reduce((sum, row) => sum + Number(row[column]), 0);

  const ROLLUP = 'base_orders.orders_by_month';
  const ORDERS = 40000;
  const MONTHS = 166;
  const COUNT_QUERY = 'SELECT MEASURE(orders.count) AS orders_count FROM orders LIMIT 5000';

  // First to touch Cube Store, so the requests race the build of the cold rollup
  test('the same query from the agent and Explore at once, before the rollup is built', async () => {
    const results = await Promise.all([
      cubeSql(COUNT_QUERY),
      cubeSql(COUNT_QUERY, { cache: 'stale-while-revalidate', timezone: 'America/Los_Angeles' }),
      cubeSql(COUNT_QUERY, { cache: 'must-revalidate', timezone: 'America/Los_Angeles' }),
    ]);

    for (const result of results) {
      expect(result.rows).toEqual([[`${ORDERS}`]]);
      expect(result.usedPreAggregationIds).toEqual([ROLLUP]);
    }
  });

  test('measure query is served from the rollup', async () => {
    const result = await cubeSql(COUNT_QUERY);

    expect(result.rows).toEqual([[`${ORDERS}`]]);
    expect(result.usedPreAggregationIds).toEqual([ROLLUP]);
  });

  test.each([
    ['stale-while-revalidate', 'America/Los_Angeles'],
    ['must-revalidate', 'Europe/London'],
    ['stale-if-slow', undefined],
    ['no-cache', 'UTC'],
  ])('measure query with cache %s and time zone %s is served from the rollup', async (cache, timezone) => {
    const result = await cubeSql(COUNT_QUERY, { cache, timezone });

    expect(result.rows).toEqual([[`${ORDERS}`]]);
    expect(result.usedPreAggregationIds).toEqual([ROLLUP]);
  });

  test('post-processed measures are served from the rollup', async () => {
    const result = await cubeSql(
      'SELECT MEASURE(orders.count) + 1 AS c, CASE WHEN MEASURE(orders.completed_count) > 10 THEN \'many\' ELSE \'few\' END AS completed FROM orders',
      { cache: 'stale-while-revalidate', timezone: 'UTC' }
    );

    expect(result.rows).toEqual([[`${ORDERS + 1}`, 'many']]);
    expect(result.usedPreAggregationIds).toEqual([ROLLUP]);
  });

  test('monthly query is served from the rollup', async () => {
    const result = await cubeSql(
      'SELECT DATE_TRUNC(\'month\', created_at) AS month, MEASURE(orders.count) AS c FROM orders GROUP BY 1 ORDER BY 1',
      { cache: 'stale-while-revalidate', timezone: 'UTC' }
    );

    expect(result.rows.length).toBe(MONTHS);
    expect(total(result.rows, 1)).toBe(ORDERS);
    expect(result.usedPreAggregationIds).toEqual([ROLLUP]);
  });

  test('pushed down query runs on the data source', async () => {
    const result = await cubeSql(
      'SELECT CAST(DATE_TRUNC(\'month\', created_at) AS DATE) AS month, MEASURE(orders.count) AS c, SUM(MEASURE(orders.count)) OVER (ORDER BY CAST(DATE_TRUNC(\'month\', created_at) AS DATE)) AS running FROM orders GROUP BY 1 ORDER BY 1',
      { cache: 'stale-while-revalidate', timezone: 'UTC' }
    );

    expect(result.rows.length).toBe(MONTHS);
    expect(Number(result.rows[MONTHS - 1][2])).toBe(ORDERS);
    expect(result.usedPreAggregationIds).toEqual([]);
  });

  // Dashboard queries an AI agent wrote over the same project, over members the rollup
  // does not hold
  test('orders by status', async () => {
    const result = await cubeSql(
      'SELECT status, ROUND(MEASURE(line_items_sum_price)::numeric / MEASURE(count), 2) as avg_order_value, MEASURE(count) as order_count, MEASURE(completed_percentage) as completion_rate FROM orders GROUP BY status ORDER BY order_count DESC'
    );

    expect(result.rows.map(([status, , count]) => [status, count])).toEqual([
      ['completed', '20003'],
      ['shipped', '10034'],
      ['processing', '6631'],
      ['returned', '3332'],
    ]);
    expect(result.usedPreAggregationIds).toEqual([]);
  });

  test('monthly revenue until the current month', async () => {
    const result = await cubeSql(
      'SELECT DATE_TRUNC(\'month\', created_at) as month, MEASURE(count) as order_count, MEASURE(line_items_sum_price) as total_revenue FROM orders WHERE created_at < DATE_TRUNC(\'month\', CURRENT_DATE) GROUP BY month ORDER BY month'
    );

    expect(result.rows.length).toBeGreaterThan(0);
    expect(total(result.rows, 1)).toBeLessThanOrEqual(ORDERS);
    expect(result.usedPreAggregationIds).toEqual([]);
  });
});
