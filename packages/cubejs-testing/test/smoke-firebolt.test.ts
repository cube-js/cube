import cubejs, { CubeApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, jest, expect } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_API_TOKEN,
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
  JEST_BEFORE_ALL_DEFAULT_TIMEOUT,
  testQueryMeasure,
} from './smoke-tests';

describe('firebolt', () => {
  jest.setTimeout(60 * 5 * 1000);
  let birdbox: BirdBox;
  let client: CubeApi;

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
