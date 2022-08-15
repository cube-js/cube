import cubejs, { CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, jest, expect } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import { DEFAULT_CONFIG, testQueryMeasure } from './smoke-tests';

describe('firebolt', () => {
  jest.setTimeout(60 * 5 * 1000);
  let birdbox: BirdBox;
  let client: CubejsApi;

  beforeAll(async () => {
    birdbox = await getBirdbox(
      'firebolt',
      {
        CUBEJS_DB_TYPE: 'firebolt',
        ...DEFAULT_CONFIG
      },
      {
        schemaDir: 'firebolt/schema',
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
      measures: ['Orders.count'],
      timeDimensions: [
        {
          dimension: 'Orders.createdAt',
          granularity: 'day',
        },
      ],
    });

    expect(response.rawData()).toMatchSnapshot('dimensions');
  });
});
