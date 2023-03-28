import { jest, expect, beforeAll, afterAll } from '@jest/globals';
import { BaseDriver } from '@cubejs-backend/base-driver';
import { Environment } from '../types/Environment';
import {
  getCreateQueries,
  getSelectQueries,
  getDriver,
  runEnvironment,
} from '../helpers';

export function testConnection(type: string): void {
  describe(`Raw @cubejs-backend/${type}-driver`, () => {
    jest.setTimeout(60 * 5 * 1000);
    let driver: BaseDriver;
    let query: string[];
    let env: Environment;
  
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
  
    it('must establish a connection', async () => {
      await driver.testConnection();
    });
  
    it('must creates a data source', async () => {
      query = getCreateQueries(type);
      await Promise.all(query.map(async (q) => {
        await driver.query(q);
      }));
    });

    it('must select from the data source', async () => {
      query = getSelectQueries(type);
      const response = await Promise.all(
        query.map(async (q) => {
          const res = await driver.query(q);
          return res;
        })
      );
      expect(response).toMatchSnapshot();
    });

    it('must delete the data source', async () => {
      await Promise.all(['ecommerce', 'customers', 'products'].map(async (t) => {
        await driver.dropTable(t);
      }));
    });
  });
}
