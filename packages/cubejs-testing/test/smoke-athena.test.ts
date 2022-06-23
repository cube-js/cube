import cubejs, { CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import { DEFAULT_CONFIG, testQueryMeasure } from './smoke-tests';

async function setupBirdbox(extraEnv: Record<string, string> = {}) {
  return getBirdbox(
    'athena',
    {
      CUBEJS_DB_TYPE: 'athena',
      ...DEFAULT_CONFIG,
      ...extraEnv,
    },
    {
      schemaDir: 'postgresql/schema',
    }
  );
}

async function setupClient(birdbox: BirdBox) {
  return cubejs(async () => 'test', {
    apiUrl: birdbox.configuration.apiUrl,
  });
}

async function runScheduledRefresh(client: any) {
  return client.loadMethod(
    () => client.request('run-scheduled-refresh'),
    (response: any) => response,
  );
}

describe('athena', () => {
  jest.setTimeout(60 * 5 * 1000);

  // test('query measure', async () => {
  //   const birdbox = await setupBirdbox();
  //   try {
  //     const client = await setupClient(birdbox);
  //     await testQueryMeasure(client);
  //   } finally {
  //     await birdbox.stop();
  //   }
  // });

  test('rollup measure', async () => {
    const birdbox = await setupBirdbox({
      CUBEJS_REFRESH_WORKER: 'true',
      CUBEJS_ROLLUP_ONLY: 'true',
    });
    try {
      const client = await setupClient(birdbox);
      // await client.runScheduledRefresh();
      await runScheduledRefresh(client);
      await testQueryMeasure(client);
    } finally {
      await birdbox.stop();
    }
  });
});
