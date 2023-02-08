import { createDriver } from './hana.db.runner';
import { SapHanaDriver } from '../src';

const streamToArray = require('stream-to-array');

const maybe = process.env.TEST_DB_SERVER && process.env.TEST_DB_USER && process.env.TEST_DB_PASSWORD
  ? describe
  : describe.skip;

maybe('SapHanaDriver', () => {
  let hanaDriver: SapHanaDriver;

  jest.setTimeout(60000);

  beforeAll(async () => {
    hanaDriver = createDriver();
    hanaDriver.setLogger((msg: any, event: any) => console.log(`${msg}: ${JSON.stringify(event)}`));

    await hanaDriver.query('CREATE SCHEMA test;', []);
  });

  afterAll(async () => {
    await hanaDriver.query('DROP SCHEMA test cascade;', []);
    await hanaDriver.release();
  });

  test('test hana connection', async () => {
    const result = await hanaDriver.testConnection();
    expect(result).toStrictEqual([{ 1: 1 }]);
  });

  test('hana types to generic types', async () => {
    await hanaDriver.query('CREATE TABLE test.var_types (second_date seconddate, small_decimal smalldecimal)', []);
    await hanaDriver.query('INSERT INTO  test.var_types (second_date, small_decimal) values(\'2022-11-17 10:20:30\', \'123.45\')', []);
    expect(JSON.parse(JSON.stringify((await hanaDriver.downloadQueryResults('select * from test.var_types', [], { highWaterMark: 1000 })).types)))
      .toStrictEqual([
        { name: 'SECOND_DATE', type: 'timestamp' },
        { name: 'SMALL_DECIMAL', type: 'decimal' },
      ]);
  });

  test('hana boolean field', async () => {
    // hana boolean doesn't accept 'true', 'false' and null
    await hanaDriver.uploadTable('test.boolean', [{ name: 'b_value', type: 'boolean' }], {
      rows: [
        { b_value: true },
        { b_value: true },
        { b_value: false },
      ]
    });

    // HANA always use upper case in result set if not double quotes
    const trueResult = await hanaDriver.query('select * from test.boolean where "b_value" = ?', [true]);
    expect(JSON.parse(JSON.stringify(trueResult))).toStrictEqual([{ b_value: true }, { b_value: true }]);
    
    const falseResult = await hanaDriver.query('select * from test.boolean where "b_value" = ?', [false]);
    expect(JSON.parse(JSON.stringify(falseResult))).toStrictEqual([{ b_value: false }]);
  });

  test('hana object stream', async () => {
    await hanaDriver.uploadTable(
      'test.streaming_test',
      [
        { name: 'id', type: 'bigint' },
        { name: 'created', type: 'date' },
        { name: 'price', type: 'decimal' }
      ],
      {
        rows: [
          { id: 1, created: '2022-01-01', price: '100' },
          { id: 2, created: '2022-01-02', price: '200' },
          { id: 3, created: '2022-01-03', price: '300' }
        ]
      }
    );

    const tableData = await hanaDriver.stream('select * from test.streaming_test', [], {
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
        { id: 1, created: '2022-01-01', price: '100' },
        { id: 2, created: '2022-01-02', price: '200' },
        { id: 3, created: '2022-01-03', price: '300' }
      ]);
    } finally {
      await tableData.release();
    }
  });

  test('release', async () => {
    expect(async () => {
      await hanaDriver.release();
    }).not.toThrowError();

    expect(async () => {
      await hanaDriver.release();
    }).not.toThrowError(
      /Called end on pool more than once/
    );
  });
});
