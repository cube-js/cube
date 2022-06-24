import cubejs, { CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, jest } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import { DEFAULT_CONFIG, testQueryMeasure } from './smoke-tests';

describe('bigquery', () => {
  jest.setTimeout(60 * 5 * 1000);
  let birdbox: BirdBox;
  let client: CubejsApi;

  beforeAll(async () => {
    birdbox = await getBirdbox(
      'bigquery',
      {
        CUBEJS_DB_TYPE: 'bigquery',

        ...DEFAULT_CONFIG,
      },
      {
        schemaDir: 'smoke/schema',
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
});
