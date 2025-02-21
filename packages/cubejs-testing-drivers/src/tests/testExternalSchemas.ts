import { expect } from '@jest/globals';
import { BaseDriver, QuerySchemasResult } from '@cubejs-backend/base-driver';

const EXTERNAL_SCHEMA = 'spectrum_test_schema';
const EXTERNAL_TABLE = 'sales';

export function redshiftExternalSchemasSuite(
  execute: (name: string, test: () => Promise<void>) => void,
  driver: () => BaseDriver & { stream?: (query: string, values: string[], options: { highWaterMark: number }) => Promise<any> }
) {
  execute('should establish a connection', async () => {
    await driver().testConnection();
  });

  execute('should load schemas (including external)', async () => {
    const inputSchemas: QuerySchemasResult[] = await driver().getSchemas();
    expect(inputSchemas).toBeInstanceOf(Array);
    expect(inputSchemas.length).toBeGreaterThan(0);
    expect(inputSchemas).toContainEqual({
      schema_name: EXTERNAL_SCHEMA,
    });
  });

  execute('should load tables for external schema', async () => {
    const inputTables = await driver().getTablesForSpecificSchemas([{ schema_name: EXTERNAL_SCHEMA }]);
    expect(inputTables).toBeInstanceOf(Array);
    expect(inputTables.length).toBeGreaterThan(0);
    expect(inputTables).toContainEqual({
      schema_name: EXTERNAL_SCHEMA,
      table_name: EXTERNAL_TABLE,
    });
  });

  execute('should load columns for external table', async () => {
    const columnsForTables = await driver().getColumnsForSpecificTables([{
      schema_name: EXTERNAL_SCHEMA,
      table_name: EXTERNAL_TABLE,
    }]);
    expect(columnsForTables).toBeInstanceOf(Array);
    expect(columnsForTables.length).toBeGreaterThan(0);

    columnsForTables.forEach((it) => {
      expect(it.schema_name).toEqual(EXTERNAL_SCHEMA);
      expect(it.table_name).toEqual(EXTERNAL_TABLE);
      expect(it.column_name).toEqual(expect.any(String));
      expect(it.data_type).toEqual(expect.any(String));
    });
  });

  execute('should load columns types for external table', async () => {
    const columnsForTables = await driver().tableColumnTypes(`${EXTERNAL_SCHEMA}.${EXTERNAL_TABLE}`);

    expect(columnsForTables).toBeInstanceOf(Array);
    expect(columnsForTables.length).toBeGreaterThan(0);

    columnsForTables.forEach((it) => {
      expect(it.name).toEqual(expect.any(String));
      expect(it.type).toEqual(expect.any(String));
    });
  });
}
