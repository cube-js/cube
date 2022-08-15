import cubejs, { CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import { DEFAULT_CONFIG, TEST_CASES, testQueryMeasure } from './smoke-tests';

// describe('bigquery', () => {
//   jest.setTimeout(60 * 5 * 1000);
//   let birdbox: BirdBox;
//   let client: CubejsApi;
//
//   beforeAll(async () => {
//     birdbox = await getBirdbox(
//       'bigquery',
//       {
//         CUBEJS_DB_TYPE: 'bigquery',
//
//         ...DEFAULT_CONFIG,
//       },
//       {
//         schemaDir: 'postgresql/schema',
//       }
//     );
//     client = cubejs(async () => 'test', {
//       apiUrl: birdbox.configuration.apiUrl,
//     });
//   });
//
//   afterAll(async () => {
//     await birdbox.stop();
//   });
//
//   test('query measure', () => testQueryMeasure(client));
// });

describe('bigqueryPA', () => {
  jest.setTimeout(60 * 5 * 1000);
  let birdbox: BirdBox;
  let client: CubejsApi;

  beforeAll(async () => {
    birdbox = await getBirdbox(
      'bigquery',
      {
        ...DEFAULT_CONFIG,
        CUBEJS_DB_TYPE: 'bigquery',
        CUBEJS_ROLLUP_ONLY: 'true',
        CUBEJS_REFRESH_WORKER: 'false',
      },
      {
        schemaDir: 'smoke/schema',
        cubejsConfig: 'smoke/cube.js',
      },
    );
    client = cubejs(async () => 'test', {
      apiUrl: birdbox.configuration.apiUrl,
    });
  });

  afterAll(async () => {
    await birdbox.stop();
  });

  test('query measure', async () => {
    const testCase = TEST_CASES.basicPA;
    const result = await client.load(testCase.query, testCase.options);
    expect(result.rawData()).toEqual(testCase.rows);
  });
});
