import { StartedTestContainer } from 'testcontainers';
import { PostgresDBRunner } from '@cubejs-backend/testing-shared';
import cubejs, { CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import { DEFAULT_CONFIG } from './smoke-tests';

describe('multidb', () => {
  jest.setTimeout(60 * 5 * 1000);
  let db: StartedTestContainer;
  let birdbox: BirdBox;
  let client: CubejsApi;

  beforeAll(async () => {
    birdbox = await getBirdbox(
      'multidb',
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

  test('lambda query', async () => {
    const response = await client.load({
      measures: ['Orders.count'],
      dimensions: ['Orders.status'],
      timeDimensions: [
        {
          dimension: 'Orders.completedAt',
          granularity: 'day'
        }
      ],
      limit: 3
    });
    expect(response.rawData()).toMatchSnapshot('query');
  });
});
