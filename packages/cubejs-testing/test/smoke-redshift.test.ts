import { StartedTestContainer } from 'testcontainers';
import cubejs, { CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';

describe('redshift', () => {
  jest.setTimeout(60 * 5 * 1000);
  let birdbox: BirdBox;
  let client: CubejsApi;

  beforeAll(async () => {
    birdbox = await getBirdbox(
      'redshift',
      {
        CUBEJS_DB_TYPE: 'redshift',

        CUBEJS_DEV_MODE: 'true',
        CUBEJS_WEB_SOCKETS: 'false',
        CUBEJS_EXTERNAL_DEFAULT: 'true',
        CUBEJS_SCHEDULED_REFRESH_DEFAULT: 'false',
        CUBEJS_REFRESH_WORKER: 'false',
        CUBEJS_ROLLUP_ONLY: 'false',
      },
      {
        schemaDir: 'postgresql/schema',
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
    const response = await client.load({
      measures: [
        'Orders.totalAmount',
      ],
    });
    expect(response.rawData()).toMatchSnapshot('query');
  });
});
