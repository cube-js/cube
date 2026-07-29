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

  // Every reference to a rollup in a multi-stage plan is keyed separately
  // (`__usage_N` suffix), so dedupe to the distinct rollup tables used.
  function usedPreAggregations(resultSet: any): string[] {
    const keys = Object.keys(
      resultSet.serialize().loadResponse.results[0].usedPreAggregations || {}
    );
    return [...new Set(keys.map(t => t.replace(/__usage_\d+$/, '')))].sort();
  }

  const SALES_ROLLUP = 'dev_pre_aggregations.sales_perf_rolling';
  const SHARE_ROLLUP = 'dev_pre_aggregations.share_metrics_perf_share';

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
    expect(usedPreAggregations(result)).toEqual([SALES_ROLLUP]);
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
    expect(usedPreAggregations(result)).toEqual([SALES_ROLLUP, SHARE_ROLLUP]);
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
    expect(usedPreAggregations(result)).toEqual([SALES_ROLLUP, SHARE_ROLLUP]);
    expect(result.rawData().map((r: any) => r['performance_view.product'])).toEqual(['P1', 'P2']);
  });

  // A month-granularity time dimension over a bounded range — the shape a
  // dashboard charting these rolling metrics per month sends. Both rollups
  // are built at `granularity: month` on the same date and the planner picks
  // them (verified in the schema-compiler Postgres suite), but Cube Store
  // cannot execute the resulting plan:
  //
  //   Internal: Optimizer rule 'optimize_projections' failed
  //   Schema error: No field named fk_aggregate.sales__r3_amount. Valid
  //   fields are fk_aggregate.performance_view__date_month,
  //   fk_aggregate.performance_view__product,
  //   fk_aggregate.rolling_window_dim__rolling_window.
  //
  // The rolling-window CTE groups by (date, product, calc group) and its
  // consumer projects a strict subset of those group-by columns — it drops
  // the filter-pinned calc-group column but keeps the aggregate — and the
  // Cube Store projection optimizer then prunes the aggregate out of the
  // CTE's schema. The same SQL runs correctly on Postgres, and a plain
  // rolling measure (no `case` entrypoint, so no calc-group column in the
  // CTE) at month granularity is served from the same rollup by Cube Store
  // without trouble.
  //
  // Cube Store itself was fixed in #11410, and the Rust suite proves the shape
  // runs: cubesqlplanner tests/integration/cubestore/switch_rolling.rs executes
  // it against a live Cube Store and snapshots correct rows. What still blocks
  // these two specs is the image split: birdbox pulls the published
  // cubejs/cubestore:${BIRDBOX_CUBESTORE_VERSION:-latest}, while the Cube server
  // is built from this working tree, so the planner fix is under test here but
  // the Cube Store fix is not.
  //
  // FIXME: unskip once a Cube Store release containing #11410 is `latest`.
  const MONTH_TIME_DIMENSION: Query['timeDimensions'] = [
    {
      dimension: 'performance_view.date',
      granularity: 'month',
      dateRange: ['2017-04-01', '2017-06-30'],
    },
  ];

  const MONTH_ORDER: Query['order'] = {
    'performance_view.product': 'asc',
    'performance_view.date': 'asc',
  };

  // R3 (trailing 3 month) per month for account A1: P1 sells
  // 10/20/30/40/50/60 from January to June, P2 sells 5 every month.
  const EXPECTED_MONTHLY_AMOUNTS = [
    ['P1', '2017-04-01T00:00:00.000', 90],
    ['P1', '2017-05-01T00:00:00.000', 120],
    ['P1', '2017-06-01T00:00:00.000', 150],
    ['P2', '2017-04-01T00:00:00.000', 15],
    ['P2', '2017-05-01T00:00:00.000', 15],
    ['P2', '2017-06-01T00:00:00.000', 15],
  ];

  function monthlyAmounts(result: any) {
    return result.rawData().map((r: any) => [
      r['performance_view.product'],
      r['performance_view.date.month'],
      Number(r['performance_view.rolling_amount']),
    ]);
  }

  test.skip('single-cube rolling measures with a month time dimension are served from the rollup', async () => {
    const query: Query = {
      measures: [
        'performance_view.rolling_amount',
        'performance_view.rolling_amount_change',
      ],
      dimensions: ['performance_view.product'],
      timeDimensions: MONTH_TIME_DIMENSION,
      filters: REPRO_FILTERS,
      order: MONTH_ORDER,
    };
    const result = await client.load(query);
    expect(usedPreAggregations(result)).toEqual([SALES_ROLLUP]);
    expect(monthlyAmounts(result)).toEqual(EXPECTED_MONTHLY_AMOUNTS);
  });

  test.skip('cross-cube rolling measures with a month time dimension are served from both rollups', async () => {
    const query: Query = {
      measures: [
        'performance_view.rolling_amount',
        'performance_view.rolling_amount_change',
        'performance_view.rolling_share_change',
      ],
      dimensions: ['performance_view.product'],
      timeDimensions: MONTH_TIME_DIMENSION,
      filters: REPRO_FILTERS,
      order: MONTH_ORDER,
    };
    const result = await client.load(query);
    expect(usedPreAggregations(result)).toEqual([SALES_ROLLUP, SHARE_ROLLUP]);
    expect(monthlyAmounts(result)).toEqual(EXPECTED_MONTHLY_AMOUNTS);
  });

  // Mirrors the production query shape: rolling amount + growth percentage
  // (an extra multi-stage layer over the same rolling leaves) + cross-cube
  // share change. Before the trivial-subquery collapse optimizer in the
  // Tesseract physical plan builder, the deep FullKeyAggregate plan this
  // produces overflowed Cube Store's serialized-plan decode recursion limit
  // and the query failed with "Error during planning: Error decoding expr
  // as protobuf: ... recursion limit reached".
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
    expect(usedPreAggregations(result)).toEqual([SALES_ROLLUP, SHARE_ROLLUP]);
    expect(result.rawData().map((r: any) => r['performance_view.product'])).toEqual(['P1', 'P2']);
  });
});
