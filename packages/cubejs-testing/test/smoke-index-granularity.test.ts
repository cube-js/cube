import fetch from 'node-fetch';
import { StartedTestContainer } from 'testcontainers';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest, test, describe } from '@jest/globals';
import cubejs, { CubeApi, Query } from '@cubejs-client/core';
import { PostgresDBRunner } from '@cubejs-backend/testing-shared';
import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_API_TOKEN,
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
  JEST_BEFORE_ALL_DEFAULT_TIMEOUT,
} from './smoke-tests';

describe('pre-aggregation index with time dimension granularity columns', () => {
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
      },
      {
        schemaDir: 'smoke/schema',
        cubejsConfig: 'smoke/cube.js',
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

  test('query pre-aggregation with granularity-based index', async () => {
    const query: Query = {
      measures: ['OrdersPAIndexGranularity.count', 'OrdersPAIndexGranularity.totalAmount'],
      dimensions: ['OrdersPAIndexGranularity.status'],
      timeDimensions: [{
        dimension: 'OrdersPAIndexGranularity.createdAt',
        granularity: 'day',
        dateRange: ['2024-01-01', '2024-01-03'],
      }],
      order: {
        'OrdersPAIndexGranularity.createdAt': 'asc',
        'OrdersPAIndexGranularity.status': 'asc',
      },
    };
    const result = await client.load(query);
    expect(result.rawData()).toEqual([
      {
        'OrdersPAIndexGranularity.count': '2',
        'OrdersPAIndexGranularity.createdAt': '2024-01-01T00:00:00.000',
        'OrdersPAIndexGranularity.createdAt.day': '2024-01-01T00:00:00.000',
        'OrdersPAIndexGranularity.status': 'new',
        'OrdersPAIndexGranularity.totalAmount': '300',
      },
      {
        'OrdersPAIndexGranularity.count': '2',
        'OrdersPAIndexGranularity.createdAt': '2024-01-02T00:00:00.000',
        'OrdersPAIndexGranularity.createdAt.day': '2024-01-02T00:00:00.000',
        'OrdersPAIndexGranularity.status': 'processed',
        'OrdersPAIndexGranularity.totalAmount': '800',
      },
      {
        'OrdersPAIndexGranularity.count': '1',
        'OrdersPAIndexGranularity.createdAt': '2024-01-03T00:00:00.000',
        'OrdersPAIndexGranularity.createdAt.day': '2024-01-03T00:00:00.000',
        'OrdersPAIndexGranularity.status': 'shipped',
        'OrdersPAIndexGranularity.totalAmount': '600',
      },
    ]);
  });

  test('pre-aggregation partitions include correct index references', async () => {
    const id = 'OrdersPAIndexGranularity.ordersByDay';

    const partitionsResponse = await (await fetch(`${birdbox.configuration.systemUrl}/pre-aggregations/partitions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: {
          preAggregations: [{ id }]
        }
      }),
    })).json();

    const partitions = partitionsResponse.preAggregationPartitions;
    expect(partitions.length).toEqual(1);
    expect(partitions[0].partitions.length).toBeGreaterThan(0);

    const partition = partitions[0].partitions[0];
    expect(partition.indexesSql.length).toEqual(2);

    const [timeIndexSql] = partition.indexesSql[0].sql;
    expect(timeIndexSql).toContain('orders_p_a_index_granularity__created_at_day');
    expect(timeIndexSql).not.toContain('orders_p_a_index_granularity__created_at__day');

    const [timeAndStatusIndexSql] = partition.indexesSql[1].sql;
    expect(timeAndStatusIndexSql).toContain('orders_p_a_index_granularity__created_at_day');
    expect(timeAndStatusIndexSql).toContain('orders_p_a_index_granularity__status');
  });
});
