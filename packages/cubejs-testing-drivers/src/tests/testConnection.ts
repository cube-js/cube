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
      env = await runEnvironment(type, 'driver');
      if (env.data) {
        process.env.CUBEJS_DB_HOST = '127.0.0.1';
        process.env.CUBEJS_DB_PORT = `${env.data.port}`;
      }
      driver = (await getDriver(type)).source;
    });
  
    afterAll(async () => {
      await driver.release();
      await env.stop();
    });
  
    execute('must establish a connection', async () => {
      await driver.testConnection();
    });
  
    execute('must creates a data source', async () => {
      query = getCreateQueries(type, 'driver');
      await Promise.all(query.map(async (q) => {
        await driver.query(q);
      }));
    });

    execute('must select from the data source', async () => {
      query = getSelectQueries(type, 'driver');
      const response = await Promise.all(
        query.map(async (q) => {
          const res = await driver.query(q);
          return res;
        })
      );
      expect(response.length).toBe(3);

      response[0].forEach(item => {
        expect(item).toMatchSnapshot({
          category: expect.any(String),
          product_name: expect.any(String),
          sub_category: expect.any(String),
        });
      });
      expect(response[0].length).toBe(28);

      response[1].forEach(item => {
        expect(item).toMatchSnapshot({
          customer_id: expect.any(String),
          customer_name: expect.any(String),
        });
      });
      expect(response[1].length).toBe(41);

      response[2].forEach(item => {
        expect(item).toMatchSnapshot({
          row_id: expect.anything(), // can be String or Number
          order_id: expect.any(String),
          order_date: expect.anything(), // can be String or Date
          customer_id: expect.any(String),
          city: expect.any(String),
          category: expect.any(String),
          sub_category: expect.any(String),
          product_name: expect.any(String),
          sales: expect.anything(), // can be String or Number
          quantity: expect.anything(), // can be String or Number
          discount: expect.anything(), // can be String or Number
          profit: expect.anything(), // can be String or Number
        });
      });
      expect(response[2].length).toBe(44);
    });

    execute('must stream from the data source', async () => {
      query = getSelectQueries(type, 'driver');
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
              resolve({ types, data });
            });
          });
          return { types, data };
        })
      );
      expect(response.length).toBe(3);
      expect(response[0].data.length).toBe(28);
      expect(response[1].data.length).toBe(41);
      expect(response[2].data.length).toBe(44);
    });

    execute('must delete the data source', async () => {
      await Promise.all([
        'ecommerce_driver',
        'customers_driver',
        'products_driver',
      ].map(async (t) => {
        await driver.dropTable(t);
      }));
    });
  });
}
