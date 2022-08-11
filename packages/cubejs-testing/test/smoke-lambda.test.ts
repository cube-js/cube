import R from 'ramda';
import { StartedTestContainer } from 'testcontainers';
import { pausePromise } from '@cubejs-backend/shared';
import { PostgresDBRunner } from '@cubejs-backend/testing-shared';
import cubejs, { CubejsApi, Query } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import { DEFAULT_CONFIG } from './smoke-tests';

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

async function checkCubestoreState(cubestore: any) {
  let rows = await cubestore.query('SELECT table_schema, table_name, build_range_end FROM information_schema.tables ORDER BY table_name', []);
  const table = rows[3];
  rows = R.map(
    // eslint-disable-next-line camelcase
    ({ table_schema, table_name, build_range_end }) => ({ table_schema, table_name: table_name.split('_').slice(0, -3).join('_'), build_range_end }),
    rows
  );
  expect(rows.slice(0, 4)).toEqual([
    {
      table_schema: 'dev_pre_aggregations',
      table_name: 'orders_orders_by_completed_at20200201',
      build_range_end: '2020-05-07T00:00:00.000Z',
    },
    {
      table_schema: 'dev_pre_aggregations',
      table_name: 'orders_orders_by_completed_at20200301',
      build_range_end: '2020-05-07T00:00:00.000Z',
    },
    {
      table_schema: 'dev_pre_aggregations',
      table_name: 'orders_orders_by_completed_at20200401',
      build_range_end: '2020-05-07T00:00:00.000Z',
    },
    {
      table_schema: 'dev_pre_aggregations',
      table_name: 'orders_orders_by_completed_at20200501',
      build_range_end: '2020-05-07T00:00:00.000Z',
    },
  ]);
  expect(table.build_range_end).toEqual('2020-05-07T00:00:00.000Z');
  rows = await cubestore.query(`SELECT * FROM ${table.table_schema}.${table.table_name}`, []);
  expect(rows.length).toEqual(18);
  rows = await cubestore.query(`SELECT * FROM ${table.table_schema}.${table.table_name} WHERE orders__completed_at_day < to_timestamp('${table.build_range_end}')`, []);
  expect(rows.length).toEqual(18);
  rows = await cubestore.query(`SELECT * FROM ${table.table_schema}.${table.table_name} WHERE orders__completed_at_day >= to_timestamp('${table.build_range_end}')`, []);
  expect(rows.length).toEqual(0);
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
        CUBEJS_PLAYGROUND_AUTH_SECRET: 'SECRET',
        CUBEJS_SCHEDULED_REFRESH_DEFAULT: 'true',
        CUBEJS_ROLLUP_ONLY: 'true',
        CUBEJS_REFRESH_WORKER: 'false',
        CUBEJS_PRE_AGGREGATIONS_BUILDER: 'true',
      },
      {
        schemaDir: 'lambda/schema',
        cubejsConfig: 'lambda/cube.js',
      }
    );
    client = cubejs(async () => 'test', {
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
      'dev_pre_aggregations.orders_orders_by_completed_at'
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

    await checkCubestoreState(cubestore);

    // add a row to (2021-01-06T00:00:00.000, shipped)
    // add a row to (2021-12-30T00:00:00.000, shipped)
    // add 2 rows to (_, completed), should not be visible in the results
    await postgres.query(`
      INSERT INTO public.Orders
        (id, user_id, number, status, completed_at, created_at, product_id)
      VALUES
        (1000000, 123, 321, 'shipped', '2021-01-06T09:00:00.000', '2021-01-05T09:00:00.000', 25),
        (1000001, 123, 321, 'completed', '2021-01-06T09:00:00.000', '2021-01-05T09:00:00.000', 25),
        (1000002, 123, 321, 'shipped', '2021-12-30T09:00:00.000', '2021-12-20T09:00:00.000', 25),
        (1000003, 123, 321, 'completed', '2021-12-30T09:00:00.000', '2021-12-20T09:00:00.000', 25);
    `);

    // wait past refreshKey: { every: '1 second' } to invalidate the cached lambda query
    await pausePromise(2000);

    const response2 = await client.load(query);

    expect(response2.rawData()).toEqual(
      [
        {
          'Orders.completedAt': '2021-12-30T00:00:00.000',
          'Orders.completedAt.day': '2021-12-30T00:00:00.000',
          'Orders.count': '1',
          'Orders.status': 'shipped',
        },
        {
          'Orders.completedAt': '2021-01-07T00:00:00.000',
          'Orders.completedAt.day': '2021-01-07T00:00:00.000',
          'Orders.count': '1',
          'Orders.status': 'shipped',
        },
        {
          'Orders.completedAt': '2021-01-06T00:00:00.000',
          'Orders.completedAt.day': '2021-01-06T00:00:00.000',
          'Orders.count': '3',
          'Orders.status': 'shipped',
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

    await checkCubestoreState(cubestore);
  });

  test('refresh', async () => {
    await runScheduledRefresh(client);
    await checkCubestoreState(cubestore);
  });
});
