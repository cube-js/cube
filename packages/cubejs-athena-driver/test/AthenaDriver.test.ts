import { DriverTests } from '@cubejs-backend/testing';

import { AthenaDriver } from '../src';

DriverTests.config();

describe('AthenaDriver', () => {
  let tests: DriverTests;

  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    tests = new DriverTests(
      new AthenaDriver({}),
      {
        expectStringFields: true,
        preAggregationWrapLoadSql: (tableName: string, query: string): string => {
          return `CREATE TABLE ${tableName} AS ${query}`;
        }
      }
    );
  });

  afterAll(async () => {
    await tests.release();
  });

  test('query', async () => {
    await tests.testQuery();
  });

  test('stream', async () => {
    await tests.testStream();
  });

  test('unload', async () => {
    await tests.testUnload();
  });
});
