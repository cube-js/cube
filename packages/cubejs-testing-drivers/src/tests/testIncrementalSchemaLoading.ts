import { jest, expect, beforeAll, afterAll } from '@jest/globals';
import {
  BaseDriver, QuerySchemasResult, QueryTablesResult,
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
    let inputSchemas: QuerySchemasResult[];
    let inputTables: QueryTablesResult[];

    const tables = Object
      .keys(fixtures.tables)
      .map((key: string) => `${fixtures.tables[key]}_driver`);

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
  
    execute('should create the data source', async () => {
      query = getCreateQueries(type, 'driver');
      await Promise.all(query.map(async (q) => {
        await driver.query(q);
      }));
    });

    execute('should load and check driver capabilities', async () => {
      const capabilities = driver.capabilities();
      expect(capabilities).toHaveProperty('incrementalSchemaLoading');
      expect(capabilities.incrementalSchemaLoading).toBe(true);
    });

    execute('should load schemas', async () => {
      inputSchemas = await driver.getSchemas();
      console.log('schemas', inputSchemas);
      expect(inputSchemas).toBeInstanceOf(Array);
      expect(inputSchemas.length).toBeGreaterThan(0);
      expect(inputSchemas[0]).toMatchSnapshot({
        schema_name: expect.any(String),
      });
    });

    execute('should load tables for specific schemas', async () => {
      inputSchemas = inputSchemas.filter((s) => !!s.schema_name);
      console.log('inputSchemas', inputSchemas);
      inputTables = await driver.getTablesForSpecificSchemas(inputSchemas);
      console.log('tablesForSchemas', inputTables);
      expect(inputTables).toBeInstanceOf(Array);
      expect(inputTables.length).toBeGreaterThan(0);
      expect(inputTables[0]).toMatchSnapshot({
        schema_name: expect.any(String),
        table_name: expect.any(String),
      });
    });

    execute('should load columns for specific tables', async () => {
      const createdTables = tables.map((t) => t.split('.').pop());
      inputTables = inputTables.filter((t) => createdTables.includes(t.table_name));
      console.log('tables', tables);
      console.log('inputTables', inputTables);
      const columnsForTables = await driver.getColumnsForSpecificTables(inputTables);
      console.log('columnsForTables', columnsForTables);
      expect(columnsForTables).toBeInstanceOf(Array);
      expect(columnsForTables.length).toBeGreaterThan(0);
      expect(columnsForTables[0]).toMatchSnapshot({
        schema_name: expect.any(String),
        table_name: expect.any(String),
        column_name: expect.any(String),
        data_type: expect.any(String),
      });
    });

    execute('should delete the data source', async () => {
      await Promise.all(
        tables.map(async (t) => {
          await driver.dropTable(t);
        })
      );
    });
  });
}
