/* globals describe, afterAll, beforeAll, test, expect, jest */
const { VerticaDBRunner } = require('@cubejs-backend/testing-shared');
const VerticaDriver = require('../src/VerticaDriver.js');

describe('VerticaDriver', () => {
  let container;
  let driver;

  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    container = await VerticaDBRunner.startContainer();
    driver = new VerticaDriver({
      host: container.getHost(),
      port: container.getMappedPort(5433),
      user: 'dbadmin',
      password: '',
      database: 'test',
    });
  });

  afterAll(async () => {
    await driver.release();

    if (container) {
      await container.stop();
    }
  });

  test('test connection', async () => {
    const ping = await driver.testConnection();

    expect(ping).toEqual([
      { n: 1 }
    ]);
  });

  test('test default tz', async () => {
    const result = await driver.query('SHOW TIMEZONE');
    expect(result[0].name).toBe('timezone');
    expect(result[0].setting).toBe('UTC');
  });

  test('simple query', async () => {
    const data = await driver.query(
      `
        SELECT
          '2020-01-01'::date                      AS date,
          '2020-01-01 00:00:00'::timestamp        AS timestamp,
          '2020-01-01 21:30:45.015004'::timestamp AS timestamp_us,
          '2020-01-01 00:00:00+02'::timestamptz   AS timestamptz,
          '1.01'::decimal(10,2)                   AS decimal,
          1::int                                  AS integer
      `,
      []
    );

    expect(data).toEqual([
      {
        date: '2020-01-01',
        timestamp: '2020-01-01 00:00:00',
        timestamp_us: '2020-01-01 21:30:45.015004',
        timestamptz: '2019-12-31 22:00:00+00',
        decimal: '1.01',
        integer: 1,
      }
    ]);
  });

  test('parameterized query', async () => {
    const data = await driver.query(
      `
        WITH testdata AS (
          select 1 as id, 'foo' as val union all
          select 2 as id, 'bar' as val union all
          select 3 as id, 'baz' as val union all
          select 4 as id, 'qux' as val
        )
        SELECT *
        FROM testdata
        WHERE id = ?
           OR val = ?
        ORDER BY id
      `,
      [1, 'baz']
    );

    expect(data).toEqual([
      { id: 1, val: 'foo' },
      { id: 3, val: 'baz' },
    ]);
  });

  test('get tables', async () => {
    await driver.query('DROP SCHEMA IF EXISTS test_get_tables CASCADE;');
    await driver.query('CREATE SCHEMA test_get_tables;');
    await driver.query('CREATE TABLE test_get_tables.tab (id int);');

    const tables = await driver.getTablesQuery('test_get_tables');

    expect(tables).toEqual([
      { table_name: 'tab' },
    ]);
  });

  test('table column types', async () => {
    await driver.query('DROP SCHEMA IF EXISTS test_column_types CASCADE;');
    await driver.query('CREATE SCHEMA test_column_types;');
    await driver.query(`
      CREATE TABLE test_column_types.tab (
        integer_col   int,
        date_col      date,
        timestamp_col timestamp,
        decimal_col   decimal(10,2),
        varchar_col   varchar(64),
        char_col      char(2),
        set_col       set[int8],
        array_col     array[int8]
      );
    `);

    const columns = await driver.tableColumnTypes('test_column_types.tab');

    expect(columns).toEqual([
      { name: 'integer_col', type: 'bigint' },
      { name: 'date_col', type: 'date' },
      { name: 'timestamp_col', type: 'timestamp' },
      { name: 'decimal_col', type: 'decimal' },
      { name: 'varchar_col', type: 'text' },
      { name: 'char_col', type: 'text' },
      { name: 'set_col', type: 'text' },
      { name: 'array_col', type: 'text' },
    ]);
  });

  test('create schema', async () => {
    await driver.createSchemaIfNotExists('new_schema');
    await driver.createSchemaIfNotExists('new_schema');
    const schema = await driver.query(`
      SELECT count(1) AS cnt
      FROM v_catalog.schemata
      WHERE schema_name = 'new_schema';
    `);

    expect(schema).toEqual([{ cnt: 1 }]);
  });
});
