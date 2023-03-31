import { jest, expect, beforeAll, afterAll } from '@jest/globals';
import { BaseDriver } from '@cubejs-backend/base-driver';
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
          const stream = driver.stream &&
            await driver.stream(q, [], { highWaterMark: 16000 });
          const { types } = stream;
          const data: unknown[] = [];
          await new Promise((resolve) => {
            const { rowStream } = stream;
            rowStream.on('data', (row: unknown) => {
              data.push(row);
            });
            rowStream.on('end', () => {
              stream.release();
              resolve(undefined);
            });
          });
          return { types, data };
        })
      );
      expect(response).toMatchSnapshot();
    });

    execute('must delete the data source', async () => {
      await Promise.all(['ecommerce', 'customers', 'products'].map(async (t) => {
        await driver.dropTable(t);
      }));
    });
  });
}
