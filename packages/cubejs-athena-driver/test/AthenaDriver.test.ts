// eslint-disable-next-line import/no-extraneous-dependencies
import { DriverTests, smartStringTrim } from '@cubejs-backend/testing-shared';

import { AthenaDriver } from '../src';

class AthenaDriverTest extends DriverTests {
  protected getExpectedCsvRows() {
    // Athena uses \N for null values
    return smartStringTrim`
      orders__status,orders__amount
      new,300
      processed,400
      \N,500
    `;
  }
}

describe('AthenaDriver', () => {
  let tests: AthenaDriverTest;

  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    tests = new AthenaDriverTest(
      new AthenaDriver({}),
      {
        expectStringFields: true,
        csvNoHeader: true,
        wrapLoadQueryWithCtas: true,
        delimiter: '\x01',
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
