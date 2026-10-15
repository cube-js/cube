import { StartedTestContainer } from 'testcontainers';
import { CrateDBRunner, DriverTests } from '@cubejs-backend/testing-shared';
import { CrateDriver } from '../src';

describe('CrateDriver', () => {
  let db: StartedTestContainer;
  let tests: DriverTests;
  let driver: CrateDriver;
  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    db = await CrateDBRunner.startContainer({ volumes: [] });
    const config = {
      host: db.getHost(),
      port: db.getMappedPort(5432),
      user: 'crate',
      password: '',
      database: 'crate',
    };
    tests = new DriverTests(new CrateDriver(config), {});
    driver = new CrateDriver(config);
  });

  afterAll(async () => {
    await tests.release();
    await driver.release();
    await db.stop();
  });

  test('query', async () => {
    await tests.testQuery();
  });

  // Key queries are inherited from PostgresDriver and are not supported by CrateDB:
  // they must fail softly instead of breaking schema loading
  describe('primary and foreign keys', () => {
    beforeAll(async () => {
      await driver.query('CREATE TABLE IF NOT EXISTS keys_test.customers (id INT PRIMARY KEY, name TEXT)', []);
      await driver.query('CREATE TABLE IF NOT EXISTS keys_test.orders (id INT PRIMARY KEY, customer_id INT)', []);
    });

    afterAll(async () => {
      await driver.query('DROP TABLE IF EXISTS keys_test.orders', []);
      await driver.query('DROP TABLE IF EXISTS keys_test.customers', []);
    });

    test('tablesSchemaV2', async () => {
      const { keys_test: schema } = await driver.tablesSchemaV2();

      expect(schema).toEqual({
        customers: [
          { name: 'id', type: 'integer', attributes: [] },
          { name: 'name', type: 'text', attributes: [] },
        ],
        orders: [
          { name: 'customer_id', type: 'integer', attributes: [] },
          { name: 'id', type: 'integer', attributes: [] },
        ],
      });
    });

    test('getColumnsForSpecificTables', async () => {
      const columns = await driver.getColumnsForSpecificTables([
        { schema_name: 'keys_test', table_name: 'orders' },
      ]);

      expect(columns.map(({ table_name, column_name, attributes, foreign_keys }) => ({
        table_name, column_name, attributes, foreign_keys,
      })).sort((a, b) => a.column_name.localeCompare(b.column_name))).toEqual([
        { table_name: 'orders', column_name: 'customer_id', attributes: undefined, foreign_keys: [] },
        { table_name: 'orders', column_name: 'id', attributes: undefined, foreign_keys: [] },
      ]);
    });
  });
});
