// eslint-disable-next-line import/no-extraneous-dependencies
import { expect } from '@jest/globals';
import { CubeApi } from '@cubejs-client/core';
import { sign } from 'jsonwebtoken';

export const DEFAULT_CONFIG = {
  CUBEJS_DEV_MODE: 'true',
  CUBEJS_WEB_SOCKETS: 'false',
  CUBEJS_EXTERNAL_DEFAULT: 'true',
  CUBEJS_SCHEDULED_REFRESH_DEFAULT: 'false',
  CUBEJS_REFRESH_WORKER: 'false',
  CUBEJS_ROLLUP_ONLY: 'false',
  CUBEJS_API_SECRET: 'secret',
};

export const DEFAULT_API_TOKEN = sign({}, DEFAULT_CONFIG.CUBEJS_API_SECRET, {
  expiresIn: '2 days'
});

export const JEST_BEFORE_ALL_DEFAULT_TIMEOUT = 2 * 60 * 1000;
export const JEST_AFTER_ALL_DEFAULT_TIMEOUT = 60 * 1000;

export async function testQueryMeasure(client: CubeApi) {
  const response = await client.load({
    measures: [
      'Orders.totalAmount',
    ],
  });
  expect(response.rawData()).toMatchSnapshot('query');
}
