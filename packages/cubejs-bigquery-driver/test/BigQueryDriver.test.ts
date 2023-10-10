// eslint-disable-next-line import/no-extraneous-dependencies
import { DriverTests } from '@cubejs-backend/testing-shared';

import { BigQueryDriver } from '../src';

describe('BigQueryDriver', () => {
  let tests: DriverTests;

  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    tests = new DriverTests(new BigQueryDriver({}));
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
    await tests.testUnloadEscapeSymbolOp1(BigQueryDriver);
    await tests.testUnloadEscapeSymbolOp2(BigQueryDriver);
    await tests.testUnloadEscapeSymbolOp3(BigQueryDriver);
  });
});
