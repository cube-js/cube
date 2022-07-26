import R from 'ramda';
import { StartedTestContainer } from 'testcontainers';
import { PostgresDBRunner } from '@cubejs-backend/testing-shared';
import cubejs, { CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import { DEFAULT_CONFIG } from './smoke-tests';

const CubeStoreDriver = require('@cubejs-backend/cubestore-driver');

async function checkCubestoreState(cubestore: any) {
  let rows = await cubestore.query('SELECT table_schema, table_name, build_range_end FROM information_schema.tables ORDER BY table_name', []);
  const table = rows[3];
  rows = R.map(
    // eslint-disable-next-line camelcase
    ({ table_schema, table_name, build_range_end }) => ({ table_schema, table_name: table_name.split('_').slice(0, -3).join('_'), build_range_end }),
    rows
  );
  expect(rows).toEqual([
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
    }
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
  //
  let db: StartedTestContainer;
  let birdbox: BirdBox;
  let client: CubejsApi;
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
        CUBEJS_REFRESH_WORKER: 'true',
        CUBEJS_ROLLUP_ONLY: 'true',
      },
      {
        schemaDir: 'lambda/schema',
      }
    );
    client = cubejs(async () => 'test', {
      apiUrl: birdbox.configuration.apiUrl,
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
    const response = await client.load({
      measures: ['Orders.count'],
      dimensions: ['Orders.status'],
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
      },
      limit: 3
    });

    // With lambda-view we observe all 'fresh' data, with no partition/buildRange limit.
    expect(response.rawData()).toEqual(
      [
        {
          'Orders.completedAt': '2020-12-31T00:00:00.000',
          'Orders.completedAt.day': '2020-12-31T00:00:00.000',
          'Orders.count': '11',
          'Orders.status': 'shipped',
        },
        {
          'Orders.completedAt': '2020-12-30T00:00:00.000',
          'Orders.completedAt.day': '2020-12-30T00:00:00.000',
          'Orders.count': '8',
          'Orders.status': 'shipped',
        },
        {
          'Orders.completedAt': '2020-12-29T00:00:00.000',
          'Orders.completedAt.day': '2020-12-29T00:00:00.000',
          'Orders.count': '10',
          'Orders.status': 'shipped',
        },
      ]
    );

    await checkCubestoreState(cubestore);
  });

  test('refresh', async () => {
    await client.runScheduledRefresh();
    await checkCubestoreState(cubestore);
  });
});
