import cubejs, { CubeApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { MssqlDbRunner } from '@cubejs-backend/testing-shared';
import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_API_TOKEN,
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
  JEST_BEFORE_ALL_DEFAULT_TIMEOUT,
  testQueryMeasure,
} from './smoke-tests';

describe('mssql', () => {
  jest.setTimeout(60 * 5 * 1000);
  let birdbox: BirdBox;
  let client: CubeApi;

  beforeAll(async () => {
    const db = await MssqlDbRunner.startContainer({});
    birdbox = await getBirdbox(
      'mssql',
      {
        CUBEJS_DB_TYPE: 'mssql',

        CUBEJS_DB_HOST: db.getHost(),
        CUBEJS_DB_PORT: `${db.getMappedPort(1433)}`,
        CUBEJS_DB_USER: 'sa',
        CUBEJS_DB_PASS: process.env.TEST_DB_PASSWORD || 'Test1test',

        ...DEFAULT_CONFIG,
      },
      {
        schemaDir: 'mssql/schema',
      }
    );
    client = cubejs(async () => DEFAULT_API_TOKEN, {
      apiUrl: birdbox.configuration.apiUrl,
    });
  }, JEST_BEFORE_ALL_DEFAULT_TIMEOUT);

  afterAll(async () => {
    await birdbox.stop();
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

  test('query measure', () => testQueryMeasure(client));

  test('query dimensions', async () => {
    const response = await client.load({
      measures: [
        'Orders.totalAmount',
      ],
      dimensions: [
        'Orders.status',
      ],
    });

    expect(response.rawData()).toMatchSnapshot('dimensions');
  });
});
