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
