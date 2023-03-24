import fetch from 'node-fetch';
import cubejs, { CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, jest, expect } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import { DEFAULT_API_TOKEN, DEFAULT_CONFIG, testQueryMeasure } from './smoke-tests';

const delay = (t: number) => new Promise(resolve => setTimeout(() => resolve(null), t));

describe('athena', () => {
  jest.setTimeout(60 * 5 * 1000);
  let birdbox: BirdBox;
  let client: CubejsApi;

  beforeAll(async () => {
    birdbox = await getBirdbox(
      'athena',
      {
        CUBEJS_DB_TYPE: 'athena',
        CUBEJS_DB_NAME: 'default',

        ...DEFAULT_CONFIG,
      },
      {
        schemaDir: 'postgresql/schema',
      }
    );
    client = cubejs(async () => DEFAULT_API_TOKEN, {
      apiUrl: birdbox.configuration.apiUrl,
    });
  });

  afterAll(async () => {
    await birdbox.stop();
  });

  test('query measure', () => testQueryMeasure(client));
});
