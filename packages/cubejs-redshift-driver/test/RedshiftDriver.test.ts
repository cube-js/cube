import { PostgresDBRunner } from '@cubejs-backend/testing';

import { RedshiftDriver } from '../src';

const streamToArray = require('stream-to-array');

describe('RedshiftDriver', () => {
  let driver: RedshiftDriver;

  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    driver = new RedshiftDriver({
      host: process.env.CUBEJS_TESTING_REDSHIFT_HOST,
      port: parseInt(process.env.CUBEJS_TESTING_REDSHIFT_PORT || '5439', 10),
      user: process.env.CUBEJS_TESTING_REDSHIFT_USER,
      password: process.env.CUBEJS_TESTING_REDSHIFT_PASSWORD,
      database: process.env.CUBEJS_TESTING_REDSHIFT_DATABASE,
    });
  });

  afterAll(async () => {
    await driver.release();
  });

  test('super', async () => {
    await driver.query('create temporary table test_super_type (id int, extra super);', []);
    await driver.query(`insert into test_super_type (extra, id) values (JSON_PARSE('{"mykey": 5}'), 2);`, []);

    const tableData = await driver.stream('SELECT extra.mykey FROM test_super_type', [], {
      highWaterMark: 1000,
    });

    try {
      expect(await tableData.types).toEqual([
        {
          name: 'id',
          type: 'bigint'
        },
        {
          name: 'extra',
          type: 'super'
        }
      ]);
    } finally {
      await (<any> tableData).release();
    }
  });
});
