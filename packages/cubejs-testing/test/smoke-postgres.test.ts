import cubejs, { CubejsApi, Query } from '@cubejs-client/core';
import { PostgresDBRunner } from '@cubejs-backend/testing-shared';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { StartedTestContainer } from 'testcontainers';
import { BirdBox, getBirdbox } from '../src';
import { DEFAULT_CONFIG } from './smoke-tests';


describe('postgres pa', () => {
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
        CUBEJS_DB_HOST: db.getHost(),
        CUBEJS_DB_PORT: `${db.getMappedPort(5432)}`,
        CUBEJS_DB_NAME: 'test',
        CUBEJS_DB_USER: 'test',
        CUBEJS_DB_PASS: 'test',
        CUBEJS_ROLLUP_ONLY: 'true',
        CUBEJS_REFRESH_WORKER: 'false',
        // CUBEJS_SCHEDULED_REFRESH_DEFAULT: 'true',
        // CUBEJS_PRE_AGGREGATIONS_BUILDER: 'true',
      },
      {
        schemaDir: 'smoke/schema',
        cubejsConfig: 'smoke/cube.js',
      },
    );
    client = cubejs(async () => 'test', {
      apiUrl: birdbox.configuration.apiUrl,
    });
  });

  afterAll(async () => {
    await birdbox.stop();
    await db.stop();
  });

  test('basic pa', async () => {
    const query: Query = {
      measures: ['Orders.totalAmount'],
      dimensions: ['Orders.status'],
      order: {
        'Orders.status': 'asc',
      },
    };
    const result = await client.load(query, {});
    expect(result.rawData()).toEqual([]);
  });
});
