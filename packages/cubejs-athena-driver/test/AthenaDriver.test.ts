// eslint-disable-next-line import/no-extraneous-dependencies
import { DriverTests } from '@cubejs-backend/testing-shared';

import { AthenaDriver } from '../src';

describe('AthenaDriver', () => {
  let tests: DriverTests;

  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    tests = new DriverTests(
      new AthenaDriver({}),
      {
        expectStringFields: true,
        csvNoHeader: true,
        wrapLoadQueryWithCtas: true,
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

  test('unload CSV escape symbol', async () => {
    await tests.testUnloadEscapeSymbolOp1(AthenaDriver);
    await tests.testUnloadEscapeSymbolOp2(AthenaDriver);
    await tests.testUnloadEscapeSymbolOp3(AthenaDriver);
  });

  test('unload empty', async () => {
    await tests.testUnloadEmpty();
  });
});
