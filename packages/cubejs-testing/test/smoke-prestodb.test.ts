import cubejs, { CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { PrestoDbRunner } from '@cubejs-backend/testing-shared';
import { BirdBox, getBirdbox } from '../src';
import { DEFAULT_CONFIG, testQueryMeasure } from './smoke-tests';

describe('prestodb', () => {
  jest.setTimeout(60 * 5 * 1000);
  let birdbox: BirdBox;
  let client: CubejsApi;

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
    client = cubejs(async () => 'test', {
      apiUrl: birdbox.configuration.apiUrl,
    });
  });

  afterAll(async () => {
    await birdbox.stop();
  });

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
});
