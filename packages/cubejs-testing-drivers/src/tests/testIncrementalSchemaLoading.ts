import { jest, expect, beforeAll, afterAll } from '@jest/globals';
import {
  BaseDriver,
} from '@cubejs-backend/base-driver';
import { Environment } from '../types/Environment';
import {
  getFixtures,
  getCreateQueries,
  getDriver,
  runEnvironment,
} from '../helpers';

export function testIncrementalSchemaLoading(type: string): void {
  describe(`Incremental schema loading @cubejs-backend/${type}-driver`, () => {
    jest.setTimeout(60 * 5 * 1000);

    const fixtures = getFixtures(type);
    let driver: BaseDriver & {
      stream?: (
        query: string,
        values: string[],
        options: { highWaterMark: number },
      ) => Promise<any>
    };
    let query: string[];
    let env: Environment;
    let schemas: any;
    let tables: any;

    function execute(name: string, test: () => Promise<void>) {
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
  
    execute('should establish a connection', async () => {
      await driver.testConnection();
    });

    execute('should load and check driver capabilities', async () => {
      const capabilities = driver.capabilities();
      expect(capabilities).toMatchObject({
        incrementalSchemaLoading: true,
      });
    });

    execute('should load schemas', async () => {
      schemas = await driver.getSchemas();
      expect(schemas).toBeInstanceOf(Array);
      expect(schemas[0]).toMatchSnapshot({
        schema_name: expect.any(String),
      });
    });

    execute('should load tables for specific schemas', async () => {
      tables = await driver.getTablesForSpecificSchemas(schemas);
      expect(tables).toBeInstanceOf(Array);
      expect(tables[0]).toMatchSnapshot({
        schema_name: expect.any(String),
        table_name: expect.any(String),
      });
    });

    execute('should load columns for specific tables', async () => {
      const columns = await driver.getColumnsForSpecificTables(tables);
      expect(columns).toBeInstanceOf(Array);
      expect(columns[0]).toMatchSnapshot({
        schema_name: expect.any(String),
        table_name: expect.any(String),
        column_name: expect.any(String),
        data_type: expect.any(String),
      });
    });
  });
}
