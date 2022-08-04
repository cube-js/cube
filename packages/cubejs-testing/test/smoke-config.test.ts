import { StartedTestContainer } from 'testcontainers';
import { MysqlDBRunner, PostgresDBRunner } from '@cubejs-backend/testing-shared';
import cubejs, { CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import { DEFAULT_CONFIG } from './smoke-tests';

describe('config manual refresh', () => {
  jest.setTimeout(60 * 5 * 1000);
  let db: StartedTestContainer;
  let birdbox: BirdBox;
  let client: CubejsApi;

  beforeAll(async () => {
    db = await PostgresDBRunner.startContainer({});
    birdbox = await getBirdbox(
      'postgres',
      {
        ...DEFAULT_CONFIG,

        CUBEJS_DB_TYPE: 'postgres',
        CUBEJS_DB_HOST: db.getHost(),
        CUBEJS_DB_PORT: `${db.getMappedPort(5432)}`,
        CUBEJS_DB_NAME: 'test',
        CUBEJS_DB_USER: 'test',
        CUBEJS_DB_PASS: 'test',

        CUBEJS_ROLLUP_ONLY: 'true',
        CUBEJS_REFRESH_WORKER: 'false',
        CUBEJS_PRE_AGGREGATIONS_BUILDER: 'true',
      },
      {
        schemaDir: 'preaggregation/schema',
      }
    );
    client = cubejs(async () => 'test', {
      apiUrl: birdbox.configuration.apiUrl,
    });
  });

  afterAll(async () => {
    await birdbox.stop();
    await db.stop();
  });

  test('manual refresh', async () => {
    const response = await client.load({
      order: {
        'Orders.status': 'asc',
      },
      measures: [
        'Orders.count',
      ],
      dimensions: [
        'Orders.status',
      ],
    });
    expect(response.rawData()).toEqual(
      [
        { 'Orders.count': '2', 'Orders.status': 'new' },
        { 'Orders.count': '2', 'Orders.status': 'processed' },
        { 'Orders.count': '1', 'Orders.status': 'shipped' }
      ]
    );
  });
});
