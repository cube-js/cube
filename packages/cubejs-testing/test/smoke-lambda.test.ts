import { StartedTestContainer } from 'testcontainers';
import { PostgresDBRunner } from '@cubejs-backend/testing-shared';
import cubejs, { CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import { DEFAULT_CONFIG } from './smoke-tests';

describe('lambda', () => {
  jest.setTimeout(60 * 5 * 1000);
  let db: StartedTestContainer;
  let birdbox: BirdBox;
  let client: CubejsApi;

  beforeAll(async () => {
    birdbox = await getBirdbox(
      'postgres',
      {
        ...DEFAULT_CONFIG,
        CUBEJS_DB_TYPE: 'postgres',
        CUBEJS_DB_HOST: 'demo-db-recipes.cube.dev',
        CUBEJS_DB_PORT: '5432',
        CUBEJS_DB_NAME: 'recipes',
        CUBEJS_DB_USER: 'cube',
        CUBEJS_DB_PASS: '12345',
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
      order: {
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
          'Orders.count': '2',
          'Orders.status': 'completed',
        },
        {
          'Orders.completedAt': '2020-12-31T00:00:00.000',
          'Orders.completedAt.day': '2020-12-31T00:00:00.000',
          'Orders.count': '6',
          'Orders.status': 'processing',
        },
        {
          'Orders.completedAt': '2020-12-30T00:00:00.000',
          'Orders.completedAt.day': '2020-12-30T00:00:00.000',
          'Orders.count': '2',
          'Orders.status': 'completed',
        },
      ]
    );
  });
});
