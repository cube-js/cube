import { jest, expect, beforeAll, afterAll } from '@jest/globals';
import { BaseDriver } from '@cubejs-backend/base-driver';
import { Readable } from 'stream';
import { Environment } from '../types/Environment';
import {
  getFixtures,
  getCreateQueries,
  getSelectQueries,
  getDriver,
  runEnvironment,
} from '../helpers';

export function testConnection(type: string): void {
  describe(`Raw @cubejs-backend/${type}-driver`, () => {
    jest.setTimeout(60 * 5 * 1000);

    let driver: BaseDriver & {
      stream?: (
        query: string,
        values: string[],
        options: { highWaterMark: number },
      ) => Promise<any>
    };
    let query: string[];
    let env: Environment;

    function execute(name: string, test: () => Promise<void>) {
      const fixtures = getFixtures(type);
      if (fixtures.skip && fixtures.skip.indexOf(name) >= 0) {
        it.skip(name, test);
      } else {
        it(name, test);
      }
    }
  
    beforeAll(async () => {
      env = await runEnvironment(type);
      if (env.data) {
        process.env.CUBEJS_DB_HOST = '127.0.0.1';
        process.env.CUBEJS_DB_PORT = `${env.data.port}`;
      }
      driver = await getDriver(type);
    });
  
    afterAll(async () => {
      await driver.release();
      await env.stop();
    });
  
    execute('must establish a connection', async () => {
      await driver.testConnection();
    });
  
    execute('must creates a data source', async () => {
      query = getCreateQueries(type);
      await Promise.all(query.map(async (q) => {
        await driver.query(q);
      }));
    });

    execute('must select from the data source', async () => {
      query = getSelectQueries(type);
      const response = await Promise.all(
        query.map(async (q) => {
          const res = await driver.query(q);
          return res;
        })
      );
      expect(response).toMatchSnapshot();
    });

    execute('must stream from the data source', async () => {
      query = getSelectQueries(type);
      const response = await Promise.all(
        query.map(async (q) => {
          const res = driver.stream &&
            await driver.stream(q, [], { highWaterMark: 16000 });
          await new Promise((resolve) => {
            console.log(JSON.stringify(res.types, undefined, 2));
            Object.keys(res.types).forEach((k) => {
              console.log('TYPE: ', res.types[k].type);
            });
            const stream: Readable = res.rowStream;
            stream.on('data', (el) => {
              console.log(JSON.stringify(el, undefined, 2));
            });
            stream.on('close', () => {
              res.release();
              resolve(undefined);
            });
          });
          return res;
        })
      );
    });

    execute('must delete the data source', async () => {
      await Promise.all(['ecommerce', 'customers', 'products'].map(async (t) => {
        await driver.dropTable(t);
      }));
    });
  });
}
