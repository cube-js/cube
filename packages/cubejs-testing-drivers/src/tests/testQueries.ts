import { jest, expect, beforeAll, afterAll } from '@jest/globals';
import { BaseDriver } from '@cubejs-backend/base-driver';
import cubejs, { CubejsApi } from '@cubejs-client/core';
import { Environment } from '../types/Environment';
import {
  getCreateQueries,
  getDriver,
  runEnvironment,
} from '../helpers';

export function testQueries(type: string): void {
  describe(`Queries with the @cubejs-backend/${type}-driver`, () => {
    jest.setTimeout(60 * 5 * 1000);

    let driver: BaseDriver;
    let client: CubejsApi;
    let query: string[];
    let env: Environment;

    beforeAll(async () => {
      env = await runEnvironment(type);
      if (env.data) {
        process.env.CUBEJS_DB_HOST = '127.0.0.1';
        process.env.CUBEJS_DB_PORT = `${env.data.port}`;
      }
      client = cubejs('mysupersecret', {
        apiUrl: `http://127.0.0.1:${env.cube.port}/cubejs-api/v1`,
      });
      driver = await getDriver(type);
      query = getCreateQueries(type);
      await Promise.all(query.map(async (q) => {
        await driver.query(q);
      }));
    });
  
    afterAll(async () => {
      await Promise.all(['ecommerce', 'customers', 'products'].map(async (t) => {
        await driver.dropTable(t);
      }));
      await driver.release();
      await env.stop();
    });

    it('query', async () => {
      const response = await client.load({
        dimensions: [
          'Products.category',
          'Products.subCategory',
          'Products.productName'
        ]
      });
    });
  });
}
