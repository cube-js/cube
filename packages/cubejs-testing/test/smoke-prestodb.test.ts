import cubejs, { CubeApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { PrestoDbRunner } from '@cubejs-backend/testing-shared';
import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_API_TOKEN,
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
  JEST_BEFORE_ALL_DEFAULT_TIMEOUT,
  testQueryMeasure,
} from './smoke-tests';

describe('prestodb', () => {
  jest.setTimeout(60 * 5 * 1000);
  let birdbox: BirdBox;
  let client: CubeApi;

  beforeAll(async () => {
    const db = await PrestoDbRunner.startContainer({});
    birdbox = await getBirdbox(
      'prestodb',
      {
        CUBEJS_DB_TYPE: 'prestodb',

        CUBEJS_DB_HOST: db.getHost(),
        CUBEJS_DB_PORT: `${db.getMappedPort(8080)}`,
        CUBEJS_DB_PRESTO_CATALOG: 'memory',
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

  test('query dimensions with underscore filter', async () => {
    const response = await client.load({
      filters: [
        {
          member: 'Orders.status',
          operator: 'contains',
          values: ['cancelled_']
        }
      ],
      dimensions: [
        'Orders.status',
      ],
    });

    expect(response.rawData()).toMatchSnapshot('dimensions');
  });

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
