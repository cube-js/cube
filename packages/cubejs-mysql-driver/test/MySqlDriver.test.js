/* globals describe, afterAll, beforeAll, test, expect, jest */
const { GenericContainer } = require("testcontainers");
const MySqlDriver = require('../driver/MySqlDriver');

const version = process.env.TEST_MYSQL_VERSION || '5.7';

const startContainer = async () => {
  const builder = new GenericContainer(`mysql:${version}`)
    .withEnv('MYSQL_ROOT_PASSWORD', process.env.TEST_DB_PASSWORD || 'Test1test')
    .withExposedPorts(3306);

  if (version.split('.')[0] === '8') {
    /**
     * workaround for MySQL 8 and unsupported auth in mysql package
     * @link https://github.com/mysqljs/mysql/pull/2233
     */
    builder.withCmd('--default-authentication-plugin=mysql_native_password');
  }

  return builder.start();
};

const createDriver = (c) => new MySqlDriver({
  host: c.getHost(),
  user: 'root',
  password: process.env.TEST_DB_PASSWORD || 'Test1test',
  port: c.getMappedPort(3306),
  database: 'mysql',
});

describe('MySqlDriver', () => {
  let container;
  let mySqlDriver;

  jest.setTimeout(50000);

  beforeAll(async () => {
    container = await startContainer();
    mySqlDriver = createDriver(container);
    mySqlDriver.setLogger((msg, event) => console.log(`${msg}: ${JSON.stringify(event)}`));

    await mySqlDriver.createSchemaIfNotExists('test');
    await mySqlDriver.query('DROP SCHEMA test');
    await mySqlDriver.createSchemaIfNotExists('test');
  });

  afterAll(async () => {
    await mySqlDriver.release();
    if (container) {
      await container.stop();
    }
  });

  test('truncated wrong value', async () => {
    await mySqlDriver.uploadTable(`test.wrong_value`, [{ name: 'value', type: 'string' }], {
      rows: [{ value: "Tekirdağ" }]
    });
    expect(JSON.parse(JSON.stringify(await mySqlDriver.query('select * from test.wrong_value'))))
      .toStrictEqual([{ value: "Tekirdağ" }]);
    expect(JSON.parse(JSON.stringify((await mySqlDriver.downloadQueryResults('select * from test.wrong_value')).rows)))
      .toStrictEqual([{ value: "Tekirdağ" }]);
  });

  test('mysql to generic type', async () => {
    await mySqlDriver.query('CREATE TABLE test.var_types (some_big bigint(9), some_medium mediumint(9), some_small smallint(3), med_text mediumtext, long_text longtext)');
    await mySqlDriver.query('INSERT INTO test.var_types (some_big, some_medium, some_small) VALUES (123, 345, 4)');
    expect(JSON.parse(JSON.stringify((await mySqlDriver.downloadQueryResults('select * from test.var_types')).types)))
      .toStrictEqual([
        { name: 'some_big', type: 'int' },
        { name: 'some_medium', type: 'int' },
        { name: 'some_small', type: 'int' },
        { name: 'med_text', type: 'text' },
        { name: 'long_text', type: 'text' },
      ]);
  });

  test('boolean field', async () => {
    await mySqlDriver.uploadTable(`test.boolean`, [{ name: 'b_value', type: 'boolean' }], {
      rows: [
        { b_value: true },
        { b_value: true },
        { b_value: 'true' },
        { b_value: false },
        { b_value: 'false' },
        { b_value: null }
      ]
    });
    expect(JSON.parse(JSON.stringify(await mySqlDriver.query('select * from test.boolean where b_value = ?', [true]))))
      .toStrictEqual([{ b_value: 1 }, { b_value: 1 }, { b_value: 1 }]);
    expect(JSON.parse(JSON.stringify(await mySqlDriver.query('select * from test.boolean where b_value = ?', [false]))))
      .toStrictEqual([{ b_value: 0 }, { b_value: 0 }]);
  });
});

describe('MySqlDriver Pool', () => {
  test('database pool error', async () => {
    const poolErrorContainer = await startContainer();

    let databasePoolErrorLogged = false;

    const poolErrorDriver = createDriver(poolErrorContainer);
    poolErrorDriver.setLogger((msg, event) => {
      if (msg === 'Database Pool Error') {
        databasePoolErrorLogged = true;
      }
      console.log(`${msg}: ${JSON.stringify(event)}`);
    });

    try {
      await poolErrorDriver.createSchemaIfNotExists('test');
      await poolErrorDriver.query('DROP SCHEMA test');
      await poolErrorDriver.createSchemaIfNotExists('test');
      await poolErrorDriver.query('SELECT 1');
      await poolErrorContainer.stop();

      try {
        await poolErrorDriver.query('SELECT 1');
      } catch (e) {
        console.log(e);
        expect(e.toString()).toContain('ResourceRequest timed out');
      }

      expect(databasePoolErrorLogged).toBe(true);
    } finally {
      await poolErrorDriver.release();
    }
  });
});
