/* globals describe, afterAll, beforeAll, test, expect, jest */
const { createDriver, startContainer } = require('./mysql.db.runner');

describe('MySqlDriver', () => {
  let container;
  let mySqlDriver;

  jest.setTimeout(2 * 60 * 1000);

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
