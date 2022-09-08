// eslint-disable-next-line import/no-extraneous-dependencies
import cubejs, { DeeplyReadonly, Query, CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import WebSocketTransport from '@cubejs-client/ws-transport';
import { BirdBox, BirdboxOptions, Env, getBirdbox } from '../src';
import { DriverTest } from './driverTests/driverTest';

type DriverType = 'postgres';

type TestSuite = {
  options?: BirdboxOptions,
  config?: Partial<Env>,
  type: DriverType
  tests: DriverTest<DeeplyReadonly<Query | Query[]>>[]
};

export function executeTestSuite({ type, tests, config = {}, options }: TestSuite) {
  describe(`${type} driver tests`, () => {
    describe(`using ${type} for the pre-aggregations`, () => {
      jest.setTimeout(60 * 5 * 1000);
      let box: BirdBox;
      let client: CubejsApi;
      let transport: WebSocketTransport;

      beforeAll(async () => {
        box = await getBirdbox(
          type,
          {
            CUBEJS_DEV_MODE: 'true',
            CUBEJS_WEB_SOCKETS: 'true',
            CUBEJS_EXTERNAL_DEFAULT: 'false',
            CUBEJS_SCHEDULED_REFRESH_DEFAULT: 'true',
            CUBEJS_REFRESH_WORKER: 'true',
            CUBEJS_ROLLUP_ONLY: 'false',
            ...config,
          },
          options
        );
        transport = new WebSocketTransport({
          apiUrl: box.configuration.apiUrl,
        });
        client = cubejs(async () => 'test', {
          apiUrl: box.configuration.apiUrl,
          // transport,
        });
      });
      afterAll(async () => {
        await transport.close();
        await box.stop();
      });

      for (const t of tests) {
        // eslint-disable-next-line no-loop-func
        test(t.name, async () => {
          const response = await client.load(t.query);

          expect(response.rawData()).toMatchSnapshot('query');

          if (t.expectArray) {
            for (const expectFn of t.expectArray) {
              expectFn(response);
            }
          }
        });
      }
    });
  });
}
