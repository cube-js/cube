import { GenericContainer, StartedTestContainer } from 'testcontainers';
import cubejs, { CubeApi, Query } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, describe, expect, jest, test } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_API_TOKEN,
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
  JEST_BEFORE_ALL_DEFAULT_TIMEOUT,
  stopIfStarted,
} from './smoke-tests';

// https://github.com/cube-js/cube/issues/11647
//
// A rolling `count_distinct_approx` measure stored in a Cube Store rollup is
// read back as `merge(sketch)` when the query has a date range but no
// granularity. Tesseract never wraps it in `cardinality(...)`, so the API
// returns the raw HLL sketch bytes, and a `number` measure that divides by it
// fails in Cube Store with
// `Cannot coerce arithmetic expression Decimal128(28, 5) / Binary`.
// The legacy planner returns the cardinality (35) for the same query.
//
// The fixture has 7 x 5 = 35 distinct `id_a-entity_id` keys in every hour.
describe('rolling count_distinct_approx over a Cube Store rollup', () => {
  jest.setTimeout(60 * 5 * 1000);

  let db: StartedTestContainer;
  let birdbox: BirdBox;
  let client: CubeApi;

  beforeAll(async () => {
    db = await new GenericContainer('hbontempo/postgres-hll:16-v2.18')
      .withEnvironment({
        POSTGRES_USER: 'test',
        POSTGRES_DB: 'test',
        POSTGRES_PASSWORD: 'test',
      })
      .withExposedPorts(5432)
      .withStartupTimeout(30 * 1000)
      .start();

    // The port opens before Postgres finishes its first start, so retry.
    for (let attempt = 0; ; attempt++) {
      const { exitCode, output } = await db.exec([
        'psql', '-U', 'test', '-d', 'test', '-c', 'CREATE EXTENSION IF NOT EXISTS hll',
      ]);
      if (exitCode === 0) {
        break;
      }
      if (attempt > 30) {
        throw new Error(`Unable to create hll extension: ${output}`);
      }
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }

    birdbox = await getBirdbox(
      'postgres',
      {
        ...DEFAULT_CONFIG,
        CUBEJS_DB_TYPE: 'postgres',
        CUBEJS_DB_HOST: db.getHost(),
        CUBEJS_DB_PORT: db.getMappedPort(5432).toString(),
        CUBEJS_DB_NAME: 'test',
        CUBEJS_DB_USER: 'test',
        CUBEJS_DB_PASS: 'test',
        CUBEJS_TESSERACT_SQL_PLANNER: 'true',
      },
      {
        schemaDir: 'hll-rolling/schema',
      }
    );
    client = cubejs(async () => DEFAULT_API_TOKEN, {
      apiUrl: birdbox.configuration.apiUrl,
    });
  }, JEST_BEFORE_ALL_DEFAULT_TIMEOUT);

  afterAll(async () => {
    await stopIfStarted('birdbox', birdbox);
    await stopIfStarted('db', db);
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

  const noGranularity: Query['timeDimensions'] = [{
    dimension: 'events.event_time',
    dateRange: ['2026-01-01T01:00:00.000', '2026-01-01T01:59:59.999'],
  }];

  test('returns the cardinality, not the sketch, without granularity', async () => {
    const result = await client.load({
      measures: ['events.count_trailing_1_hr'],
      timeDimensions: noGranularity,
    });

    // @ts-ignore
    expect(Object.keys(result.loadResponse.results[0].usedPreAggregations || {}))
      .toEqual(['dev_pre_aggregations.events_avg_by_minute']);
    expect(result.rawData()).toEqual([{ 'events.count_trailing_1_hr': '35' }]);
  });

  test('a ratio over the rolling approx count works without granularity', async () => {
    const result = await client.load({
      measures: ['events.count_trailing_1_hr', 'events.avg_trailing_1_hr'],
      filters: [{ member: 'events.group_id', operator: 'equals', values: ['1'] }],
      timeDimensions: noGranularity,
    });

    const [row] = result.rawData();
    expect(Number(row['events.count_trailing_1_hr'])).toBeGreaterThan(0);
    expect(Number.isFinite(Number(row['events.avg_trailing_1_hr']))).toBe(true);
  });
});
