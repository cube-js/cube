import cubejs, { CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, jest } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_CONFIG,
  testPreAggregation,
  testQueryMeasure
} from './smoke-tests';

async function setupClient(birdbox: BirdBox) {
  return cubejs(async () => 'test', {
    apiUrl: birdbox.configuration.apiUrl,
  });
}

describe('athena', () => {
  jest.setTimeout(60 * 5 * 1000);
  let birdbox: BirdBox;
  let client: CubejsApi;

  beforeAll(async () => {
    birdbox = await getBirdbox(
      'athena',
      {
        ...DEFAULT_CONFIG,
        CUBEJS_DB_TYPE: 'athena',
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

  test('query measure', async () => {
    await testQueryMeasure(client);
  });
});

describe('athena pa', () => {
  let birdbox: BirdBox;
  let client: CubejsApi;

  beforeAll(async () => {
    birdbox = await getBirdbox(
      'athena',
      {
        ...DEFAULT_CONFIG,
        CUBEJS_DB_TYPE: 'athena',
        CUBEJS_REFRESH_WORKER: 'true',
        CUBEJS_ROLLUP_ONLY: 'true',
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

  test('query measure', async () => {
    await testPreAggregation(client);
  });
});
