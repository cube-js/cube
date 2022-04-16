// eslint-disable-next-line import/no-extraneous-dependencies
import cubejs, { CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import {
  BirdBox,
  getBirdbox,
} from '../src';

export function executeTestCaseFor(type: string) {
  describe(`${type}`, () => {
    jest.setTimeout(60 * 5 * 1000);
    let birdbox: BirdBox;
    let client: CubejsApi;

    beforeAll(async () => {
      birdbox = await getBirdbox(type, {
        CUBEJS_DEV_MODE: 'true',
        CUBEJS_WEB_SOCKETS: 'false',
        CUBEJS_EXTERNAL_DEFAULT: 'true',
        CUBEJS_SCHEDULED_REFRESH_DEFAULT: 'false',
        CUBEJS_REFRESH_WORKER: 'true',
        CUBEJS_ROLLUP_ONLY: 'true',
      });
      client = cubejs(async () => 'test', {
        apiUrl: birdbox.configuration.apiUrl,
      });
    });

    afterAll(async () => {
      await birdbox.stop();
    });

    test('query', async () => {
      const response = await client.load({
        measures: [
          'OrdersPA.amount2',
          'OrdersPA.amount'
        ],
        dimensions: [
          'OrdersPA.id2',
          'OrdersPA.status2',
          'OrdersPA.id',
          'OrdersPA.status'
        ],
      });
      expect(response.rawData()).toMatchSnapshot('query');
    });
  });
}
