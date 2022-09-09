// eslint-disable-next-line import/no-extraneous-dependencies
import cubejs, { CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import WebSocketTransport from '@cubejs-client/ws-transport';
import { uniq } from 'ramda';
import { BirdBox, Env, getBirdbox } from '../src';
import { DriverTest } from './driverTests/driverTest';

type SupportedDriverType = 'postgres' | 'questdb' | 'firebolt' | 'bigquery' | 'athena';

type TestSuite = {
  config?: Partial<Env>
  type: SupportedDriverType;
  tests: DriverTest[];
};

export function executeTestSuite({ type, tests, config = {} }: TestSuite) {
  const testSchemas = uniq(tests.flatMap(t => t.schemas));

  const overridedConfig = {
    CUBEJS_DEV_MODE: 'true',
    CUBEJS_WEB_SOCKETS: 'true',
    CUBEJS_EXTERNAL_DEFAULT: 'false',
    CUBEJS_SCHEDULED_REFRESH_DEFAULT: 'true',
    CUBEJS_REFRESH_WORKER: 'true',
    CUBEJS_ROLLUP_ONLY: 'false',
    ...config,
  };
  describe(`${type} driver tests`, () => {
    jest.setTimeout(60 * 5 * 1000);
    let box: BirdBox;
    let client: CubejsApi;
    let transport: WebSocketTransport;

    beforeAll(async () => {
      box = await getBirdbox(
        type,
        overridedConfig,
        { schemas: testSchemas }
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
      const jsonConfig = JSON.stringify(overridedConfig);
      const testNameWithHash = `${t.name}_${jsonConfig}`;
      
      if (t.type === 'basic') {
        // eslint-disable-next-line no-loop-func
        const cbFn = async () => {
          const response = await client.load(t.query);

          expect(response.rawData()).toMatchSnapshot('query');

          if (t.expectArray) {
            for (const expectFn of t.expectArray) {
              expectFn(response);
            }
          }
        };

        if (t.skip) {
          test.skip(testNameWithHash, cbFn);
        } else {
          test(testNameWithHash, cbFn);
        }
      } else if (t.type === 'withError') {
        // eslint-disable-next-line no-loop-func
        const cbFnError = async () => {
          const promise = async () => {
            await client.load(t.query);
          };
          await expect(promise).rejects.toThrow('error');
        };
        if (t.skip) {
          test.skip(testNameWithHash, cbFnError);
        } else {
          test(testNameWithHash, cbFnError);
        }
      }
    }
  });
}
