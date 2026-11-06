import { PostgresDBRunner } from '@cubejs-backend/testing-shared';
import { StartedTestContainer } from 'testcontainers';
import { PostgresDriver } from '../src';

const streamToArray = require('stream-to-array');

function largeParams(): Array<string> {
  return new Array(65536).fill('foo');
}

describe('PostgresDriver', () => {
  let container: StartedTestContainer;
  let driver: PostgresDriver;

  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    container = await PostgresDBRunner.startContainer({ volumes: [] });
    driver = new PostgresDriver({
      host: container.getHost(),
      port: container.getMappedPort(5432),
      user: 'test',
      password: 'test',
      database: 'test',
    });
    await driver.query('CREATE SCHEMA IF NOT EXISTS test;', []);
  });

  afterAll(async () => {
    await container.stop();
  });

  test('type coercion', async () => {
    await driver.query('CREATE TYPE CUBEJS_TEST_ENUM AS ENUM (\'FOO\');', []);

    const data = await driver.query(
      `
        SELECT
          CAST('2020-01-01' as DATE) as date,
          CAST('2020-01-01 00:00:00' as TIMESTAMP) as timestamp,
          CAST('2020-01-01 00:00:00+02' as TIMESTAMPTZ) as timestamptz,
          CAST('1.0' as DECIMAL(10,2)) as decimal,
          CAST('FOO' as CUBEJS_TEST_ENUM) as enum
      `,
      []
    );

    expect(data).toEqual([
      {
        // Date in UTC
        date: '2020-01-01T00:00:00.000',
        timestamp: '2020-01-01T00:00:00.000',
        // converted to utc
        timestamptz: '2019-12-31T22:00:00.000',
        // Numerics as string
        decimal: '1.00',
        // Enum datatypes as string
        enum: 'FOO',
      }
    ]);
  });

  test('too many params', async () => {
    await expect(
      driver.query(`SELECT 'foo'::TEXT;`, largeParams())
    )
      .rejects
      .toThrow('PostgreSQL protocol does not support more than 65535 parameters, but 65536 passed');
  });

  test('stream', async () => {
    await driver.uploadTable(
      'test.streaming_test',
      [
        { name: 'id', type: 'bigint' },
        { name: 'created', type: 'date' },
        { name: 'price', type: 'decimal' }
      ],
      {
        rows: [
          { id: 1, created: '2020-01-01', price: '100' },
          { id: 2, created: '2020-01-02', price: '200' },
          { id: 3, created: '2020-01-03', price: '300' }
        ]
      }
    );

    const tableData = await driver.stream('select * from test.streaming_test', [], {
      highWaterMark: 1000,
    });

    try {
      expect(await tableData.types).toEqual([
        {
          name: 'id',
          type: 'bigint'
        },
        {
          name: 'created',
          type: 'date'
        },
        {
          name: 'price',
          type: 'decimal'
        },
      ]);
      expect(await streamToArray(tableData.rowStream)).toEqual([
        { id: '1', created: '2020-01-01T00:00:00.000', price: '100' },
        { id: '2', created: '2020-01-02T00:00:00.000', price: '200' },
        { id: '3', created: '2020-01-03T00:00:00.000', price: '300' }
      ]);
    } finally {
      await (<any> tableData).release();
    }
  });

  test('stream (array-typed columns)', async () => {
    // Streaming must not fail when a query returns array-typed columns.
    // Array types are reported as `text` and node-postgres parses them into
    // JS arrays. See CORE-522.
    const tableData = await driver.stream(
      `SELECT
        ARRAY['oops', 'test']::text[] as text_array,
        ARRAY[1, 2, 3]::int[] as int_array`,
      [],
      {
        highWaterMark: 1000,
      }
    );

    try {
      expect(await tableData.types).toEqual([
        {
          name: 'text_array',
          type: 'text'
        },
        {
          name: 'int_array',
          type: 'text'
        },
      ]);
      expect(await streamToArray(tableData.rowStream)).toEqual([
        { text_array: ['oops', 'test'], int_array: [1, 2, 3] },
      ]);
    } finally {
      await (<any> tableData).release();
    }
  });

  test('stream (user defined type)', async () => {
    await driver.query('CREATE TYPE CUBEJS_TEST_POINT AS (x int, y int);', []);
    // Postgres reports the base type oid in RowDescription rather than the domain one.
    await driver.query('CREATE DOMAIN CUBEJS_TEST_INT AS int;', []);

    // A driver of its own, the shared one loaded its types before this type existed.
    const freshDriver = new PostgresDriver({
      host: container.getHost(),
      port: container.getMappedPort(5432),
      user: 'test',
      password: 'test',
      database: 'test',
    });

    try {
      const tableData = await freshDriver.stream(
        `SELECT
          ARRAY[CAST(ROW(1, 2) as CUBEJS_TEST_POINT)] as points,
          CAST(5 as CUBEJS_TEST_INT) as aliased`,
        [],
        {
          highWaterMark: 1000,
        }
      );

      try {
        expect(await tableData.types).toEqual([
          {
            name: 'points',
            type: 'text'
          },
          {
            name: 'aliased',
            type: 'int'
          },
        ]);
        expect(await streamToArray(tableData.rowStream)).toEqual([
          { points: '{"(1,2)"}', aliased: 5 },
        ]);
      } finally {
        await (<any> tableData).release();
      }
    } finally {
      await freshDriver.release();
    }
  });

  test('stream (exception)', async () => {
    try {
      await driver.stream('select * from test.random_name_for_table_that_doesnot_exist_sql_must_fail', [], {
        highWaterMark: 1000,
      });

      throw new Error('stream must throw an exception');
    } catch (e: any) {
      expect(e.message).toEqual(
        'relation "test.random_name_for_table_that_doesnot_exist_sql_must_fail" does not exist'
      );
    }
  });

  test('stream (too many params)', async () => {
    try {
      await driver.stream('select * from test.streaming_test', largeParams(), {
        highWaterMark: 1000,
      });

      throw new Error('stream must throw an exception');
    } catch (e: any) {
      expect(e.message).toEqual(
        'PostgreSQL protocol does not support more than 65535 parameters, but 65536 passed'
      );
    }
  });

  test('table name check', async () => {
    const tblName = 'really-really-really-looooooooooooooooooooooooooooooooooooooooooooooooooooong-table-name';

    try {
      await driver.createTable(tblName, [{ name: 'id', type: 'bigint' }]);

      throw new Error('createTable must throw an exception');
    } catch (e: any) {
      expect(e.message).toEqual(
        'PostgreSQL can not work with table names longer than 63 symbols. ' +
        `Consider using the 'sqlAlias' attribute in your cube definition for ${tblName}.`
      );
    }
  });

  describe('primary and foreign keys', () => {
    const schemas = ['fk_name', 'fk_tenant_a', 'fk_tenant_b', 'fk_composite', 'fk_partitioned'];
    let readerDriver: PostgresDriver;

    beforeAll(async () => {
      for (const schema of schemas) {
        await driver.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`, []);
        await driver.query(`CREATE SCHEMA ${schema}`, []);
      }

      // Constraint names only have to be unique per table
      await driver.query('CREATE TABLE fk_name.posts (id INT PRIMARY KEY)', []);
      await driver.query(
        'CREATE TABLE fk_name.comments (id INT PRIMARY KEY, post_id INT, ' +
        'CONSTRAINT fk_parent FOREIGN KEY (post_id) REFERENCES fk_name.posts (id))',
        []
      );
      await driver.query(
        'CREATE TABLE fk_name.categories (id INT PRIMARY KEY, parent_id INT, ' +
        'CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES fk_name.categories (id))',
        []
      );

      // Schema-per-tenant: identical DDL, identical auto-generated constraint names
      for (const schema of ['fk_tenant_a', 'fk_tenant_b']) {
        await driver.query(`CREATE TABLE ${schema}.customers (id INT PRIMARY KEY)`, []);
        await driver.query(
          `CREATE TABLE ${schema}.orders (id INT PRIMARY KEY, customer_id INT REFERENCES ${schema}.customers (id))`,
          []
        );
      }

      await driver.query('CREATE TABLE fk_composite.parents (x INT, y INT, PRIMARY KEY (x, y))', []);
      await driver.query(
        'CREATE TABLE fk_composite.children (a INT, b INT, FOREIGN KEY (a, b) REFERENCES fk_composite.parents (x, y))',
        []
      );

      // Postgres clones an FK per partition of a partitioned referenced table (orders -> customers_1, ...)
      await driver.query('CREATE TABLE fk_partitioned.customers (id INT PRIMARY KEY) PARTITION BY RANGE (id)', []);
      await driver.query('CREATE TABLE fk_partitioned.customers_1 PARTITION OF fk_partitioned.customers FOR VALUES FROM (0) TO (100)', []);
      await driver.query('CREATE TABLE fk_partitioned.customers_2 PARTITION OF fk_partitioned.customers FOR VALUES FROM (100) TO (200)', []);
      await driver.query(
        'CREATE TABLE fk_partitioned.orders (id INT PRIMARY KEY, customer_id INT REFERENCES fk_partitioned.customers (id))',
        []
      );
      await driver.query(
        'CREATE TABLE fk_partitioned.events (id INT, customer_id INT REFERENCES fk_partitioned.customers (id)) PARTITION BY RANGE (id)',
        []
      );
      await driver.query('CREATE TABLE fk_partitioned.events_1 PARTITION OF fk_partitioned.events FOR VALUES FROM (0) TO (100)', []);

      // information_schema.constraint_column_usage only shows tables owned by the current role
      await driver.query('DROP ROLE IF EXISTS fk_reader', []);
      await driver.query('CREATE ROLE fk_reader LOGIN PASSWORD \'fk_reader\'', []);

      for (const schema of schemas) {
        await driver.query(`GRANT USAGE ON SCHEMA ${schema} TO fk_reader`, []);
        await driver.query(`GRANT SELECT ON ALL TABLES IN SCHEMA ${schema} TO fk_reader`, []);
      }

      readerDriver = new PostgresDriver({
        host: container.getHost(),
        port: container.getMappedPort(5432),
        user: 'fk_reader',
        password: 'fk_reader',
        database: 'test',
      });
    });

    afterAll(async () => {
      await readerDriver.release();

      for (const schema of schemas) {
        await driver.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`, []);
      }
      await driver.query('DROP ROLE IF EXISTS fk_reader', []);
    });

    const expectedSchema = {
      fk_name: {
        categories: [
          { name: 'id', type: 'integer', attributes: ['primaryKey'] },
          { name: 'parent_id', type: 'integer', attributes: [], foreign_keys: [{ target_table: 'categories', target_column: 'id' }] },
        ],
        comments: [
          { name: 'id', type: 'integer', attributes: ['primaryKey'] },
          { name: 'post_id', type: 'integer', attributes: [], foreign_keys: [{ target_table: 'posts', target_column: 'id' }] },
        ],
        posts: [
          { name: 'id', type: 'integer', attributes: ['primaryKey'] },
        ],
      },
      ...Object.fromEntries(['fk_tenant_a', 'fk_tenant_b'].map((schema) => [schema, {
        customers: [
          { name: 'id', type: 'integer', attributes: ['primaryKey'] },
        ],
        orders: [
          { name: 'customer_id', type: 'integer', attributes: [], foreign_keys: [{ target_table: 'customers', target_column: 'id' }] },
          { name: 'id', type: 'integer', attributes: ['primaryKey'] },
        ],
      }])),
      fk_composite: {
        children: [
          { name: 'a', type: 'integer', attributes: [], foreign_keys: [{ target_table: 'parents', target_column: 'x' }] },
          { name: 'b', type: 'integer', attributes: [], foreign_keys: [{ target_table: 'parents', target_column: 'y' }] },
        ],
        parents: [
          { name: 'x', type: 'integer', attributes: ['primaryKey'] },
          { name: 'y', type: 'integer', attributes: ['primaryKey'] },
        ],
      },
      fk_partitioned: {
        ...Object.fromEntries(['customers', 'customers_1', 'customers_2'].map((table) => [table, [
          { name: 'id', type: 'integer', attributes: ['primaryKey'] },
        ]])),
        events: [
          { name: 'customer_id', type: 'integer', attributes: [], foreign_keys: [{ target_table: 'customers', target_column: 'id' }] },
          { name: 'id', type: 'integer', attributes: [] },
        ],
        events_1: [
          { name: 'customer_id', type: 'integer', attributes: [], foreign_keys: [{ target_table: 'customers', target_column: 'id' }] },
          { name: 'id', type: 'integer', attributes: [] },
        ],
        orders: [
          { name: 'customer_id', type: 'integer', attributes: [], foreign_keys: [{ target_table: 'customers', target_column: 'id' }] },
          { name: 'id', type: 'integer', attributes: ['primaryKey'] },
        ],
      },
    };

    const pickTestSchemas = (tablesSchema: Record<string, unknown>) => Object.fromEntries(
      schemas.map((schema) => [schema, tablesSchema[schema]])
    );

    test('tablesSchemaV2 pairs each foreign key column with its own target', async () => {
      expect(pickTestSchemas(await driver.tablesSchemaV2())).toEqual(expectedSchema);
    });

    test('tablesSchemaV2 detects keys on tables the role does not own', async () => {
      expect(pickTestSchemas(await readerDriver.tablesSchemaV2())).toEqual(expectedSchema);
    });

    test('getColumnsForSpecificTables applies the table filter to keys', async () => {
      const columns = await readerDriver.getColumnsForSpecificTables([
        { schema_name: 'fk_name', table_name: 'comments' },
        { schema_name: 'fk_composite', table_name: 'children' },
      ]);

      expect(columns.map(({ schema_name, table_name, column_name, attributes, foreign_keys }) => ({
        schema_name, table_name, column_name, attributes, foreign_keys,
      })).sort((a, b) => `${a.schema_name}.${a.table_name}.${a.column_name}`.localeCompare(`${b.schema_name}.${b.table_name}.${b.column_name}`))).toEqual([
        {
          schema_name: 'fk_composite',
          table_name: 'children',
          column_name: 'a',
          attributes: undefined,
          foreign_keys: [{ target_table: 'parents', target_column: 'x' }],
        },
        {
          schema_name: 'fk_composite',
          table_name: 'children',
          column_name: 'b',
          attributes: undefined,
          foreign_keys: [{ target_table: 'parents', target_column: 'y' }],
        },
        { schema_name: 'fk_name', table_name: 'comments', column_name: 'id', attributes: ['primaryKey'], foreign_keys: [] },
        {
          schema_name: 'fk_name',
          table_name: 'comments',
          column_name: 'post_id',
          attributes: undefined,
          foreign_keys: [{ target_table: 'posts', target_column: 'id' }],
        },
      ]);
    });
  });

  // Note: This test MUST be the last in the list.
  test('release', async () => {
    expect(async () => {
      await driver.release();
    }).not.toThrowError(
      /Called end on pool more than once/
    );

    expect(async () => {
      await driver.release();
    }).not.toThrowError(
      /Called end on pool more than once/
    );
  });
});
