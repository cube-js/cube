import cubejs, { CubeApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { TrinoDBRunner } from '@cubejs-backend/testing-shared';
import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_API_TOKEN,
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
  JEST_BEFORE_ALL_DEFAULT_TIMEOUT,
} from './smoke-tests';

describe('trino', () => {
  jest.setTimeout(60 * 5 * 1000);
  let birdbox: BirdBox;
  let client: CubeApi;

  beforeAll(async () => {
    const db = await TrinoDBRunner.startContainer({});
    birdbox = await getBirdbox(
      'trino',
      {
        CUBEJS_DB_TYPE: 'trino',

        CUBEJS_DB_HOST: db.getHost(),
        CUBEJS_DB_PORT: `${db.getMappedPort(8080)}`,
        CUBEJS_DB_TRINO_CATALOG: 'memory',
        CUBEJS_DB_USER: 'test',

        ...DEFAULT_CONFIG,
      },
      {
        schemaDir: 'presto/schema',
      }
    );
    client = cubejs(async () => DEFAULT_API_TOKEN, {
      apiUrl: birdbox.configuration.apiUrl,
    });
  }, JEST_BEFORE_ALL_DEFAULT_TIMEOUT);

  afterAll(async () => {
    await birdbox.stop();
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

  test('query measure grouped by time dimension with timezone', async () => {
    const response = await client.load({
      measures: [
        'Orders.totalAmount',
      ],
      timeDimensions: [
        {
          dimension: 'Orders.createdAt',
          granularity: 'hour'
        }
      ],
      timezone: 'Europe/Kiev'
    });

    expect(response.rawData()).toMatchSnapshot('measure-group-by');
  });
});
