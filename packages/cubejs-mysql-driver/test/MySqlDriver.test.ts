import { MysqlDBRunner } from '@cubejs-backend/testing-shared';

import { StartedTestContainer } from 'testcontainers';
import { createDriver } from './mysql.db.runner';

import { MySqlDriver } from '../src';

const streamToArray = require('stream-to-array');

describe('MySqlDriver', () => {
  let container: StartedTestContainer;
  let mySqlDriver: MySqlDriver;

  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    container = await MysqlDBRunner.startContainer({});
    mySqlDriver = createDriver(container);
    mySqlDriver.setLogger((msg: any, event: any) => console.log(`${msg}: ${JSON.stringify(event)}`));

    await mySqlDriver.createSchemaIfNotExists('test');
    await mySqlDriver.query('DROP SCHEMA test', []);
    await mySqlDriver.createSchemaIfNotExists('test');
  });

  afterAll(async () => {
    await mySqlDriver.release();

    if (container) {
      await container.stop();
    }
  });

  test('truncated wrong value', async () => {
    await mySqlDriver.uploadTable('test.wrong_value', [{ name: 'value', type: 'string' }], {
      rows: [{ value: 'Tekirdağ' }]
    });
    expect(JSON.parse(JSON.stringify(await mySqlDriver.query('select * from test.wrong_value', []))))
      .toStrictEqual([{ value: 'Tekirdağ' }]);
    expect(JSON.parse(JSON.stringify((await mySqlDriver.downloadQueryResults('select * from test.wrong_value', [], { highWaterMark: 1000 })).rows)))
      .toStrictEqual([{ value: 'Tekirdağ' }]);
  });

  test('mysql to generic type', async () => {
    await mySqlDriver.query('CREATE TABLE test.var_types (some_big bigint(9), some_medium mediumint(9), some_small smallint(3), med_text mediumtext, long_text longtext)', []);
    await mySqlDriver.query('INSERT INTO test.var_types (some_big, some_medium, some_small) VALUES (123, 345, 4)', []);
    expect(JSON.parse(JSON.stringify((await mySqlDriver.downloadQueryResults('select * from test.var_types', [], { highWaterMark: 1000 })).types)))
      .toStrictEqual([
        { name: 'some_big', type: 'int' },
        { name: 'some_medium', type: 'int' },
        { name: 'some_small', type: 'int' },
        { name: 'med_text', type: 'text' },
        { name: 'long_text', type: 'text' },
      ]);
  });

  test('boolean field', async () => {
    await mySqlDriver.uploadTable('test.boolean', [{ name: 'b_value', type: 'boolean' }], {
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

  test('stream', async () => {
    await mySqlDriver.uploadTable(
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

    const tableData = await mySqlDriver.stream('select * from test.streaming_test', [], {
      highWaterMark: 1000,
    });

    try {
      expect(await tableData.types).toEqual([
        {
          name: 'id',
          type: 'int'
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
        { id: 1, created: '2020-01-01', price: 100 },
        { id: 2, created: '2020-01-02', price: 200 },
        { id: 3, created: '2020-01-03', price: 300 }
      ]);
    } finally {
      await tableData.release();
    }
  });
});
