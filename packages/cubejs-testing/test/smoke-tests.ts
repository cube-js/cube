// eslint-disable-next-line import/no-extraneous-dependencies
import { expect } from '@jest/globals';
import { CubejsApi } from '@cubejs-client/core';

export const DEFAULT_CONFIG = {
  CUBEJS_DEV_MODE: 'true',
  CUBEJS_WEB_SOCKETS: 'false',
  CUBEJS_EXTERNAL_DEFAULT: 'true',
  CUBEJS_SCHEDULED_REFRESH_DEFAULT: 'false',
  CUBEJS_REFRESH_WORKER: 'false',
  CUBEJS_ROLLUP_ONLY: 'false',
};

export async function testQueryMeasure(client: CubejsApi) {
  const response = await client.load({
    measures: [
      'Orders.totalAmount',
    ],
  });
  expect(response.rawData()).toMatchSnapshot('query');
}
