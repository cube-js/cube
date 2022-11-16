import { createDriver } from './hana.db.runner';
import { SapHanaDriver } from '../src';

describe('SapHanaDriver', () => {
  let hanaDriver: SapHanaDriver

  jest.setTimeout(60000);

  beforeAll(async () => {
    hanaDriver = createDriver();
    hanaDriver.setLogger((msg: any, event: any) => console.log(`${msg}: ${JSON.stringify(event)}`));

    await hanaDriver.query('CREATE SCHEMA test;', []);
  });

  afterAll(async () => {
    await hanaDriver.query('DROP SCHEMA test', []);
    await hanaDriver.release();
  });

  test('test hana connection', async () => {
    const result = await hanaDriver.testConnection();
    expect(result).toStrictEqual([{ "1": 1 }]);
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
