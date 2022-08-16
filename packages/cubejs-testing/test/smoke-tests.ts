// eslint-disable-next-line import/no-extraneous-dependencies
import { expect } from '@jest/globals';
import { CubejsApi, LoadMethodOptions, Query } from '@cubejs-client/core';

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

export type TestCase = {
  query: Query,
  options?: LoadMethodOptions,
  rows: any[],
};

export const TEST_CASES: Record<string, TestCase> = {
  basicPA: {
    query: {
      measures: [
        'OrdersPA.totalAmount',
      ],
      dimensions: [
        'OrdersPA.status',
      ],
      order: {
        'OrdersPA.status': 'asc',
      },
    },
    rows: [
      {
        'OrdersPA.status': 'new',
        'OrdersPA.totalAmount': '300',
      },
      {
        'OrdersPA.status': 'processed',
        'OrdersPA.totalAmount': '800',
      },
      {
        'OrdersPA.status': 'shipped',
        'OrdersPA.totalAmount': '600',
      },
    ]
  }
};
