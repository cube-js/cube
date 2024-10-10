import jwt from 'jsonwebtoken';
// eslint-disable-next-line import/no-extraneous-dependencies
import cubejs, { CubeApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import WebSocketTransport from '@cubejs-client/ws-transport';
import { uniq } from 'ramda';
import { BirdBox, Env, getBirdbox } from '../src';
import { DriverTest } from './driverTests/driverTest';

type SupportedDriverType =
  'postgres' |
  'questdb' |
  'firebolt' |
  'bigquery' |
  'athena' |
  'databricks-jdbc' |
  'vertica';

type TestSuite = {
  config?: Partial<Env>
  type: SupportedDriverType;
  tests: DriverTest[];
};

export function executeTestSuite({ type, tests, config = {} }: TestSuite) {
  const testSchemas = uniq(tests.flatMap(t => t.schemas));

  const apiSecret = 'mysupersecret';

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
    let client: CubeApi;
    let transport: WebSocketTransport;

    beforeAll(async () => {
      box = await getBirdbox(
        type,
        overridedConfig,
        { schemas: testSchemas }
      );
      transport = new WebSocketTransport({
        apiUrl: box.configuration.apiUrl,
        authorization: jwt.sign({}, apiSecret)
      });
      client = cubejs(async () => jwt.sign({}, apiSecret), {
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
          if (t.expectArray) {
            const promiseInstance = promise();
            for (const expectFn of t.expectArray) {
              await promiseInstance.catch((e) => expectFn(e as Error));
            }
          } else {
            await expect(promise).rejects.toMatchSnapshot('error');
          }
        };
        if (t.skip) {
          test.skip(testNameWithHash, cbFnError);
        } else {
          test(testNameWithHash, cbFnError);
        }
      } else if (t.type === 'testFn') {
        if (t.skip) {
          // eslint-disable-next-line no-loop-func
          test.skip(testNameWithHash, () => t.testFn(client));
        } else {
          // eslint-disable-next-line no-loop-func
          test(testNameWithHash, () => t.testFn(client));
        }
      }
    }
  });
}
