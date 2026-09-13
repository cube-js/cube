import { BigQueryDriver } from '../../src';

describe('BigQueryDriver', () => {
  test('throws non-not found errors while fetching tables', async () => {
    const driver = new BigQueryDriver({});
    const error = new Error('Permission denied');

    (driver as any).bigquery = {
      dataset: () => ({
        getTables: () => {
          throw error;
        },
      }),
    };

    await expect(driver.getTablesQuery('schema')).rejects.toThrow(error);
  });
});
