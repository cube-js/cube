import { StartedTestContainer } from 'testcontainers';
import { PostgresDBRunner } from '@cubejs-backend/testing-shared';
import cubejs, { CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import { DEFAULT_CONFIG } from './smoke-tests';

describe('lambda', () => {
  jest.setTimeout(60 * 5 * 1000);
  //
  let db: StartedTestContainer;
  let birdbox: BirdBox;
  let client: CubejsApi;

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
  });

  afterAll(async () => {
    await birdbox.stop();
    await db.stop();
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
  });
});
