/* globals describe, afterAll, beforeAll, test, expect, jest */
const { GenericContainer } = require("testcontainers");
const MySqlDriver = require('../driver/MySqlDriver');

describe('MySqlDriver', () => {
  let container;
  let mySqlDriver;

  jest.setTimeout(50000);

  const version = process.env.TEST_MYSQL_VERSION || '5.7';

  const startContainer = () => new GenericContainer('mysql', version)
    .withEnv('MYSQL_ROOT_PASSWORD', process.env.TEST_DB_PASSWORD || 'Test1test')
    .withExposedPorts(3306)
    .start();

  const createDriver = (c) => new MySqlDriver({
    host: 'localhost',
    user: 'root',
    password: process.env.TEST_DB_PASSWORD || 'Test1test',
    port: c && c.getMappedPort(3306) || 3306,
    database: 'mysql',
  });

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

  test('database pool error', async () => {
    const poolErrorContainer = await startContainer();
    const poolErrorDriver = createDriver(poolErrorContainer);
    let databasePoolErrorLogged = false;
    poolErrorDriver.setLogger((msg, event) => {
      if (msg === 'Database Pool Error') {
        databasePoolErrorLogged = true;
      }
      console.log(`${msg}: ${JSON.stringify(event)}`);
    });
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
    await poolErrorDriver.release();
  });
});
