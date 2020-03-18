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
      port: container && container.getMappedPort(3306) || 3306
    });
    await mySqlDriver.createSchemaIfNotExists('test');
    await mySqlDriver.query('DROP SCHEMA test');
    await mySqlDriver.createSchemaIfNotExists('test');
  });

  afterAll(async () => {
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
    await mySqlDriver.release();
  });
});
