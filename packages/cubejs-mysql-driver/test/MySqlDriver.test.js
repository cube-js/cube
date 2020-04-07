/* globals describe, afterAll, beforeAll, test, expect, jest */
const { GenericContainer } = require("testcontainers");
const MySqlDriver = require('../driver/MySqlDriver');

describe('MySqlDriver', () => {
  let container;
  let mySqlDriver;

  jest.setTimeout(50000);

  beforeAll(async () => {
    if (!process.env.TEST_LOCAL) {
      container = await new GenericContainer("mysql", '5.7')
        .withEnv("MYSQL_ROOT_PASSWORD", process.env.TEST_DB_PASSWORD || "Test1test")
        .withExposedPorts(3306)
        .start();
    }

    mySqlDriver = new MySqlDriver({
      host: 'localhost',
      user: 'root',
      password: process.env.TEST_DB_PASSWORD || "Test1test",
      port: container && container.getMappedPort(3306) || 3306,
      database: 'mysql'
    });
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
});
