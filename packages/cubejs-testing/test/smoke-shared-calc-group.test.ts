import { StartedTestContainer } from 'testcontainers';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import cubejs, { CubeApi, Query } from '@cubejs-client/core';
import { PostgresDBRunner } from '@cubejs-backend/testing-shared';
import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_API_TOKEN,
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
  JEST_BEFORE_ALL_DEFAULT_TIMEOUT,
} from './smoke-tests';

// End-to-end pre-aggregation coverage for rolling-window metrics exposed
// through multi-stage `case` entrypoint measures dispatched by a shared
// `type: switch` dimension (calc group) across two joined fact cubes.
// Unlike the schema-compiler integration spec (which builds rollups in
// Postgres), rollups here are stored and queried in Cube Store, so the
// multi-stage plans are executed by the Cube Store engine like in
// production.
describe('shared calc group pre-aggregations in Cube Store', () => {
  jest.setTimeout(60 * 5 * 1000);
  let db: StartedTestContainer;
  let birdbox: BirdBox;
  let client: CubeApi;

  beforeAll(async () => {
    db = await PostgresDBRunner.startContainer({});
    birdbox = await getBirdbox(
      'postgres',
      {
        ...DEFAULT_CONFIG,
        CUBEJS_DB_HOST: db.getHost(),
        CUBEJS_DB_PORT: `${db.getMappedPort(5432)}`,
        CUBEJS_DB_NAME: 'test',
        CUBEJS_DB_USER: 'test',
        CUBEJS_DB_PASS: 'test',
        CUBEJS_ROLLUP_ONLY: 'true',
        CUBEJS_REFRESH_WORKER: 'false',
        CUBEJS_TESSERACT_SQL_PLANNER: 'true',
      },
      {
        schemaDir: 'shared-calc-group/schema',
        cubejsConfig: 'shared-calc-group/cube.js',
      },
    );
    client = cubejs(async () => DEFAULT_API_TOKEN, {
      apiUrl: birdbox.configuration.apiUrl,
    });
  }, JEST_BEFORE_ALL_DEFAULT_TIMEOUT);

  afterAll(async () => {
    await birdbox.stop();
    await db.stop();
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

  const REPRO_FILTERS: Query['filters'] = [
    {
      member: 'performance_view.account',
      operator: 'equals',
      values: ['A1'],
    },
    {
      member: 'performance_view.rolling_window',
      operator: 'equals',
      values: ['R3'],
    },
  ];

  function usedPreAggregations(resultSet: any): string[] {
    return Object.keys(
      resultSet.serialize().loadResponse.results[0].usedPreAggregations || {}
    );
  }

  test('single-cube rolling measure is served from the rollup', async () => {
    const query: Query = {
      measures: ['performance_view.rolling_amount'],
      dimensions: ['performance_view.product'],
      filters: REPRO_FILTERS,
      order: {
        'performance_view.product': 'asc',
      },
    };
    const result = await client.load(query);
    expect(usedPreAggregations(result).some(t => t.includes('perf_rolling'))).toBe(true);
    expect(result.rawData().map((r: any) => r['performance_view.product'])).toEqual(['P1', 'P2']);
  });

  test('cross-cube rolling measures are served from both rollups', async () => {
    const query: Query = {
      measures: [
        'performance_view.rolling_amount',
        'performance_view.rolling_share_change',
      ],
      dimensions: ['performance_view.product'],
      filters: REPRO_FILTERS,
      order: {
        'performance_view.product': 'asc',
      },
    };
    const result = await client.load(query);
    const tables = usedPreAggregations(result);
    expect(tables.some(t => t.includes('perf_rolling'))).toBe(true);
    expect(tables.some(t => t.includes('perf_share'))).toBe(true);
    expect(result.rawData().map((r: any) => r['performance_view.product'])).toEqual(['P1', 'P2']);
  });

  test('full multi-stage query executes in Cube Store', async () => {
    const query: Query = {
      measures: [
        'performance_view.rolling_amount',
        'performance_view.rolling_amount_change',
        'performance_view.rolling_share_change',
      ],
      dimensions: ['performance_view.product'],
      filters: REPRO_FILTERS,
      order: {
        'performance_view.product': 'asc',
      },
    };
    const result = await client.load(query);
    const tables = usedPreAggregations(result);
    expect(tables.some(t => t.includes('perf_rolling'))).toBe(true);
    expect(tables.some(t => t.includes('perf_share'))).toBe(true);
    expect(result.rawData().map((r: any) => r['performance_view.product'])).toEqual(['P1', 'P2']);
  });

  // Mirrors the production query shape: rolling amount + growth percentage
  // (an extra multi-stage layer over the same rolling leaves) + cross-cube
  // share change. The deep FullKeyAggregate plan this produces is the shape
  // that can overflow Cube Store's serialized-plan decode recursion limit
  // ("Error decoding expr as protobuf: ... recursion limit reached").
  test('deep multi-stage query with growth percentage executes in Cube Store', async () => {
    const query: Query = {
      measures: [
        'performance_view.rolling_amount',
        'performance_view.rolling_amount_change',
        'performance_view.rolling_amount_growth_pct',
        'performance_view.rolling_share_change',
      ],
      dimensions: ['performance_view.product'],
      filters: REPRO_FILTERS,
      order: {
        'performance_view.product': 'asc',
      },
    };
    const result = await client.load(query);
    const tables = usedPreAggregations(result);
    expect(tables.some(t => t.includes('perf_rolling'))).toBe(true);
    expect(tables.some(t => t.includes('perf_share'))).toBe(true);
    expect(result.rawData().map((r: any) => r['performance_view.product'])).toEqual(['P1', 'P2']);
  });
});
