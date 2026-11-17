import { DatabaseError } from 'pg';
import { PostgresDriver } from '../src';
import { ConnectionError } from '../src/errors';

const databaseError = (code: string) => {
  const error = new DatabaseError('rejected', 0, 'error');
  error.code = code;
  return error;
};

// Answers schema queries without a server. Queries that read materialized views from pg_catalog
// throw `catalogError` while it is set, the way Postgres-compatible databases like RisingWave do.
class StubPostgresDriver extends PostgresDriver {
  public queries: string[] = [];

  public catalogError: Error | null = null;

  public plainError: Error | null = null;

  public async query<R = unknown>(query: string, _values: unknown[]): Promise<R[]> {
    if (query.includes('pg_catalog.pg_constraint')) {
      // Primary and foreign keys, not part of what's under test
      return [];
    }

    const withMaterializedViews = query.includes('c.relkind = \'m\'');
    this.queries.push(withMaterializedViews ? 'with materialized views' : 'information_schema only');

    if (withMaterializedViews && this.catalogError) {
      throw this.catalogError;
    }

    if (this.plainError) {
      throw this.plainError;
    }

    return [{
      schema_name: 'public',
      table_schema: 'public',
      table_name: 'orders',
      column_name: 'id',
      data_type: 'integer',
      numeric_precision: null,
      numeric_scale: null,
    }] as R[];
  }
}

describe('PostgresDriver materialized views fallback', () => {
  let driver: StubPostgresDriver;

  beforeEach(() => {
    driver = new StubPostgresDriver({ host: 'localhost', database: 'test' });
  });

  afterEach(async () => {
    await driver.release();
  });

  test('falls back to information_schema when the server rejects the catalog query, and remembers it', async () => {
    driver.catalogError = databaseError('42883');

    expect(await driver.tablesSchema()).toEqual({ public: { orders: [{ name: 'id', type: 'integer', attributes: [] }] } });
    expect((await driver.getSchemas()).map(s => s.schema_name)).toEqual(['public']);
    expect(await driver.tableColumnTypes('public.orders')).toEqual([{ name: 'id', type: 'int' }]);

    expect(driver.queries).toEqual([
      'with materialized views',
      'information_schema only',
      'information_schema only',
      'information_schema only',
    ]);
  });

  test('falls back for tableColumnTypes on the pre-aggregation path', async () => {
    driver.catalogError = databaseError('XX000');

    expect(await driver.tableColumnTypes('public.orders')).toEqual([{ name: 'id', type: 'int' }]);
    expect(driver.queries).toEqual(['with materialized views', 'information_schema only']);
  });

  test('logs the fallback once, with the rejection', async () => {
    const logger = jest.fn();
    driver.setLogger(logger);
    driver.catalogError = databaseError('42883');

    await driver.tablesSchema();
    await driver.getSchemas();

    const fallbackLogs = logger.mock.calls.filter(([event]) => event === 'Materialized views query failed. Listing information_schema only');
    expect(fallbackLogs).toHaveLength(1);
    expect(fallbackLogs[0][1].error).toContain('rejected');
  });

  test.each([
    ['a connection error', new ConnectionError(new Error('ECONNREFUSED'), 'postgres')],
    ['a pool error', new Error('ResourceRequest timed out')],
    ['a cancelled query', databaseError('57014')],
    ['a serialization failure', databaseError('40001')],
  ])('rethrows %s without falling back', async (_name, error) => {
    driver.catalogError = error;

    await expect(driver.getColumnsForSpecificTables([{ schema_name: 'public', table_name: 'orders' }])).rejects.toBe(error);

    driver.catalogError = null;
    await driver.getColumnsForSpecificTables([{ schema_name: 'public', table_name: 'orders' }]);

    expect(driver.queries).toEqual(['with materialized views', 'with materialized views']);
  });

  test('rethrows once the catalog query has worked', async () => {
    await driver.getTablesForSpecificSchemas([{ schema_name: 'public' }]);

    const error = databaseError('42P01');
    driver.catalogError = error;
    await expect(driver.getTablesForSpecificSchemas([{ schema_name: 'public' }])).rejects.toBe(error);

    expect(driver.queries).toEqual(['with materialized views', 'with materialized views']);
  });

  test('throws the fallback error and keeps trying the catalog query when both fail', async () => {
    driver.catalogError = databaseError('42883');
    const plainError = databaseError('42501');
    driver.plainError = plainError;

    await expect(driver.tablesSchema()).rejects.toBe(plainError);

    driver.catalogError = null;
    driver.plainError = null;
    await driver.tablesSchema();

    expect(driver.queries).toEqual(['with materialized views', 'information_schema only', 'with materialized views']);
  });
});
