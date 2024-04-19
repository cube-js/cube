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

export function incrementalSchemaLoadingSuite(
  execute: (name: string, test: () => Promise<void>) => void,
  driver: () => BaseDriver & { stream?: (query: string, values: string[], options: { highWaterMark: number }) => Promise<any> },
  tables: string[]
) {
  execute('should establish a connection', async () => {
    await driver().testConnection();
  });

  execute('should load and check driver capabilities', async () => {
    const capabilities = driver().capabilities();
    expect(capabilities).toHaveProperty('incrementalSchemaLoading');
    expect(capabilities.incrementalSchemaLoading).toBe(true);
  });

  execute('should load schemas', async () => {
    const inputSchemas: QuerySchemasResult[] = await driver().getSchemas();
    expect(inputSchemas).toBeInstanceOf(Array);
    expect(inputSchemas.length).toBeGreaterThan(0);
    expect(inputSchemas).toContainEqual({
      schema_name: expect.any(String),
    });
  });

  execute('should load tables for specific schemas', async () => {
    let inputSchemas: QuerySchemasResult[] = await driver().getSchemas();
    inputSchemas = inputSchemas.filter((s) => !!s.schema_name);
    const inputTables = await driver().getTablesForSpecificSchemas(inputSchemas);
    expect(inputTables).toBeInstanceOf(Array);
    expect(inputTables.length).toBeGreaterThan(0);
    expect(inputTables).toContainEqual({
      schema_name: expect.any(String),
      table_name: expect.any(String),
    });
  });

  execute('should load columns for specific tables', async () => {
    const createdTables = tables.map((t) => t.split('.').pop()?.toUpperCase());
    const inputSchemas: QuerySchemasResult[] = await driver().getSchemas();
    let inputTables: QueryTablesResult[] = await driver().getTablesForSpecificSchemas(inputSchemas);
    inputTables = inputTables.filter((t) => createdTables.includes(t.table_name.toUpperCase()));
    const columnsForTables = await driver().getColumnsForSpecificTables(inputTables);
    expect(columnsForTables).toBeInstanceOf(Array);
    expect(columnsForTables.length).toBeGreaterThan(0);

    columnsForTables.forEach((it) => {
      expect(it.schema_name).toEqual(expect.any(String));
      expect(it.table_name).toEqual(expect.any(String));
      expect(it.column_name).toEqual(expect.any(String));
      expect(it.data_type).toEqual(expect.any(String));
    });
  });
}

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
    let env: Environment;
    let inputSchemas: QuerySchemasResult[];
    let inputTables: QueryTablesResult[];

    const suffix = `driver_${new Date().getTime().toString(32)}`;
    const tables = Object
      .keys(fixtures.tables)
      .map((key: string) => `${fixtures.tables[key]}_${suffix}`);

    function execute(name: string, test: () => Promise<void>) {
      if (fixtures.skip && fixtures.skip.indexOf(name) >= 0) {
        it.skip(name, test);
      } else {
        it(name, test);
      }
    }
  
    beforeAll(async () => {
      env = await runEnvironment(type, suffix);
      if (env.data) {
        process.env.CUBEJS_DB_HOST = '127.0.0.1';
        process.env.CUBEJS_DB_PORT = `${env.data.port}`;
      }
      driver = (await getDriver(type)).source;
      const queries = getCreateQueries(type, suffix);
      console.log(`Creating ${queries.length} fixture tables`);
      try {
        for (const q of queries) {
          await driver.query(q);
        }
        console.log(`Creating ${queries.length} fixture tables completed`);
      } catch (e: any) {
        console.log('Error creating fixtures', e.stack);
        throw e;
      }
    });
  
    afterAll(async () => {
      try {
        console.log(`Dropping ${tables.length} fixture tables`);
        for (const t of tables) {
          await driver.dropTable(t);
        }
        console.log(`Dropping ${tables.length} fixture tables completed`);
      } finally {
        await driver.release();
        await env.stop();
      }
    });

    incrementalSchemaLoadingSuite(execute, () => driver, tables);
  });
}
