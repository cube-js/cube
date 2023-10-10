import R from 'ramda';
import { StartedTestContainer } from 'testcontainers';
import { pausePromise } from '@cubejs-backend/shared';
import fetch from 'node-fetch';
import { PostgresDBRunner } from '@cubejs-backend/testing-shared';
import cubejs, { CubejsApi, Query } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import { DEFAULT_API_TOKEN, DEFAULT_CONFIG } from './smoke-tests';

const CubeStoreDriver = require('@cubejs-backend/cubestore-driver');
const PostgresDriver = require('@cubejs-backend/postgres-driver');

async function runScheduledRefresh(client: any) {
  return client.loadMethod(
    () => client.request('run-scheduled-refresh'),
    (response: any) => response,
    {},
    undefined
  );
}

describe('lambda', () => {
  jest.setTimeout(60 * 5 * 1000);

  let db: StartedTestContainer;
  let birdbox: BirdBox;
  let client: CubejsApi;
  let postgres: any;
  let cubestore: any;

  beforeAll(async () => {
    db = await PostgresDBRunner.startContainer({});
    await PostgresDBRunner.loadEcom(db);
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
        CUBEJS_ROLLUP_ONLY: 'true',
        CUBEJS_REFRESH_WORKER: 'false',
      },
      {
        schemaDir: 'lambda/schema',
        cubejsConfig: 'lambda/cube.js',
      }
    );
    client = cubejs(async () => DEFAULT_API_TOKEN, {
      apiUrl: birdbox.configuration.apiUrl,
    });
    postgres = new PostgresDriver({
      host: db.getHost(),
      port: db.getMappedPort(5432),
      database: 'test',
      user: 'test',
      password: 'test',
    });
    // TS compiler is confused: the ctor is the module, but the TS type is inside the module.
    // @ts-ignore
    cubestore = new CubeStoreDriver({
      host: '127.0.0.1',
      user: undefined,
      password: undefined,
      port: 3030,
    });
  });

  afterAll(async () => {
    await birdbox.stop();
    await db.stop();
    await cubestore.release();
  });

  test('query', async () => {
    const query: Query = {
      measures: ['Orders.count'],
      dimensions: ['Orders.status'],
      timeDimensions: [
        {
          dimension: 'Orders.completedAt',
          granularity: 'day'
        }
      ],
      filters: [
        {
          member: 'Orders.status',
          operator: 'equals',
          values: ['shipped']
        }
      ],
      order: {
        'Orders.status': 'asc',
        'Orders.completedAt': 'desc',
      },
      limit: 3
    };
    const response = await client.load(query);

    // @ts-ignore
    expect(Object.keys(response.loadResponse.results[0].usedPreAggregations)).toEqual([
      'dev_pre_aggregations.orders_orders_by_completed_at',
      'dev_pre_aggregations.orders_orders_by_completed_by_day',
      'dev_pre_aggregations.real_time_orders__a_orders_by_completed_by_hour'
    ]);

    // With lambda-view we observe all 'fresh' data, with no partition/buildRange limit.
    expect(response.rawData()).toEqual(
      [
        {
          'Orders.completedAt': '2021-01-07T00:00:00.000',
          'Orders.completedAt.day': '2021-01-07T00:00:00.000',
          'Orders.count': '1',
          'Orders.status': 'shipped',
        },
        {
          'Orders.completedAt': '2021-01-06T00:00:00.000',
          'Orders.completedAt.day': '2021-01-06T00:00:00.000',
          'Orders.count': '2',
          'Orders.status': 'shipped',
        },
        {
          'Orders.completedAt': '2021-01-05T00:00:00.000',
          'Orders.completedAt.day': '2021-01-05T00:00:00.000',
          'Orders.count': '2',
          'Orders.status': 'shipped',
        },
      ]
    );
  });

  test('query month', async () => {
    const query: Query = {
      measures: ['Orders.count'],
      timeDimensions: [
        {
          dimension: 'Orders.completedAt',
          granularity: 'month'
        }
      ],
      order: {
        'Orders.completedAt': 'desc',
      },
      limit: 3
    };
    const response = await client.load(query);

    // @ts-ignore
    expect(Object.keys(response.loadResponse.results[0].usedPreAggregations)).toEqual([
      'dev_pre_aggregations.orders_orders_by_completed_at_month'
    ]);

    // With lambda-view we observe all 'fresh' data, with no partition/buildRange limit.
    expect(response.rawData()).toEqual(
      [
        {
          'Orders.completedAt': '2021-01-01T00:00:00.000',
          'Orders.completedAt.month': '2021-01-01T00:00:00.000',
          'Orders.count': '125',
        },
        {
          'Orders.completedAt': '2020-12-01T00:00:00.000',
          'Orders.completedAt.month': '2020-12-01T00:00:00.000',
          'Orders.count': '808',
        },
        {
          'Orders.completedAt': '2020-11-01T00:00:00.000',
          'Orders.completedAt.month': '2020-11-01T00:00:00.000',
          'Orders.count': '730',
        },
      ]
    );

    // add a row to (2021-01-06T00:00:00.000, shipped)
    // add a row to (2021-12-30T00:00:00.000, shipped)
    // add 2 rows to (_, completed), should not be visible in the results
    await postgres.query(`
      INSERT INTO public.Orders
        (id, user_id, number, status, completed_at, created_at, product_id)
      VALUES
        (1000000, 123, 321, 'shipped', '2021-01-06T09:00:00.000Z', '2021-01-05T09:00:00.000Z', 25),
        (1000001, 123, 321, 'completed', '2021-01-06T09:00:00.000Z', '2021-01-05T09:00:00.000Z', 25),
        (1000002, 123, 321, 'shipped', '2021-12-30T09:00:00.000Z', '2021-12-20T09:00:00.000Z', 25),
        (1000003, 123, 321, 'completed', '2021-12-30T09:00:00.000Z', '2021-12-20T09:00:00.000Z', 25);
    `);

    // wait past refreshKey: { every: '1 second' } to invalidate the cached lambda query
    await pausePromise(2000);

    const response2 = await client.load(query);

    expect(response2.rawData()).toEqual(
      [
        {
          'Orders.completedAt': '2021-12-01T00:00:00.000',
          'Orders.completedAt.month': '2021-12-01T00:00:00.000',
          'Orders.count': '2',
        },
        {
          'Orders.completedAt': '2021-01-01T00:00:00.000',
          'Orders.completedAt.month': '2021-01-01T00:00:00.000',
          'Orders.count': '127',
        },
        {
          'Orders.completedAt': '2020-12-01T00:00:00.000',
          'Orders.completedAt.month': '2020-12-01T00:00:00.000',
          'Orders.count': '808',
        },
      ]
    );
  });

  test('query with 2 dimensions', async () => {
    const response = await client.load({
      measures: ['Orders.count'],
      dimensions: ['Orders.status', 'Orders.userId'],
      timeDimensions: [
        {
          dimension: 'Orders.completedAt',
          dateRange: ['2020-01-01', '2020-12-31'],
          granularity: 'day'
        }
      ],
      filters: [
        {
          member: 'Orders.status',
          operator: 'equals',
          values: ['shipped']
        }
      ],
      order: {
        'Orders.status': 'asc',
        'Orders.completedAt': 'desc',
        'Orders.userId': 'asc',
      },
      limit: 3
    });

    // @ts-ignore
    expect(Object.keys(response.loadResponse.results[0].usedPreAggregations)).toEqual([
      'dev_pre_aggregations.orders_orders_by_completed_at_and_user_id'
    ]);

    // With lambda-view we observe all 'fresh' data, with no partition/buildRange limit.
    expect(response.rawData()).toEqual(
      [
        {
          'Orders.completedAt': '2020-12-31T00:00:00.000',
          'Orders.completedAt.day': '2020-12-31T00:00:00.000',
          'Orders.count': '1',
          'Orders.status': 'shipped',
          'Orders.userId': '31',
        },
        {
          'Orders.completedAt': '2020-12-31T00:00:00.000',
          'Orders.completedAt.day': '2020-12-31T00:00:00.000',
          'Orders.count': '1',
          'Orders.status': 'shipped',
          'Orders.userId': '111',
        },
        {
          'Orders.completedAt': '2020-12-31T00:00:00.000',
          'Orders.completedAt.day': '2020-12-31T00:00:00.000',
          'Orders.count': '1',
          'Orders.status': 'shipped',
          'Orders.userId': '140',
        },
      ]
    );
  });

  test('refresh', async () => {
    await runScheduledRefresh(client);
  });

  it('Pre-aggregations API', async () => {
    const preAggs = await fetch(`${birdbox.configuration.playgroundUrl}/cubejs-system/v1/pre-aggregations`, {
      method: 'GET',
      headers: {
        Authorization: ''
      },
    });
    console.log(await preAggs.json());
    expect(preAggs.status).toBe(200);
  });

  test('Pre-aggregations API partitions', async () => {
    const partitions = await (await fetch(`${birdbox.configuration.systemUrl}/pre-aggregations/partitions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: {
          preAggregations: [
            {
              id: 'Orders.ordersByCompletedAtLambda'
            },
            {
              id: 'Orders.ordersByCompletedAt'
            },
            {
              id: 'Orders.ordersByCompletedByDay'
            }
          ]
        }
      }),
    })).json();
    console.log(JSON.stringify(partitions, null, 2));
    const completedAtPartition = partitions.preAggregationPartitions[1].partitions[0];
    expect(completedAtPartition.loadSql[0]).toMatch(/orders_orders_by_completed_at/);
    const completedByDayPartition = partitions.preAggregationPartitions[2].partitions[0];
    expect(completedByDayPartition.loadSql[0]).toMatch(/orders_orders_by_completed_by_day/);
  });
});
