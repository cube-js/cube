import { StartedTestContainer } from 'testcontainers';
import { MysqlDBRunner, PostgresDBRunner } from '@cubejs-backend/testing-shared';
import cubejs, { CubeApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_API_TOKEN,
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
  JEST_BEFORE_ALL_DEFAULT_TIMEOUT,
} from './smoke-tests';

describe('multidb', () => {
  jest.setTimeout(60 * 5 * 1000);
  let db: StartedTestContainer;
  let db2: StartedTestContainer;
  let birdbox: BirdBox;
  let client: CubeApi;

  beforeAll(async () => {
    db = await PostgresDBRunner.startContainer({});
    db2 = await MysqlDBRunner.startContainer({});

    birdbox = await getBirdbox(
      'multidb',
      {
        CUBEJS_DB_TYPE: 'postgres',

        CUBEJS_DB_HOST: db.getHost(),
        CUBEJS_DB_PORT: `${db.getMappedPort(5432)}`,
        CUBEJS_DB_NAME: 'test',
        CUBEJS_DB_USER: 'test',
        CUBEJS_DB_PASS: 'test',

        CUBEJS_DB_HOST2: db2.getHost(),
        CUBEJS_DB_PORT2: `${db2.getMappedPort(3306)}`,
        CUBEJS_DB_NAME2: 'mysql',
        CUBEJS_DB_USER2: 'root',
        CUBEJS_DB_PASS2: 'Test1test',

        ...DEFAULT_CONFIG,
      },
      {
        schemaDir: 'multidb/schema',
        cubejsConfig: 'multidb/cube.js',
      }
    );
    client = cubejs(async () => DEFAULT_API_TOKEN, {
      apiUrl: birdbox.configuration.apiUrl,
    });
  }, JEST_BEFORE_ALL_DEFAULT_TIMEOUT);

  afterAll(async () => {
    await birdbox.stop();
    await db.stop();
    await db2.stop();
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

  test('query', async () => {
    const response = await client.load({
      order: {
        'Products.name': 'asc'
      },
      dimensions: [
        'Products.name',
        'Suppliers.company',
      ],
    });
    expect(response.rawData()).toMatchSnapshot('query');
  });
});
