import { BigQueryDriver } from '../src';

function driverWith(getDatasets: jest.Mock): BigQueryDriver {
  return Object.assign(Object.create(BigQueryDriver.prototype) as BigQueryDriver, {
    bigquery: { getDatasets },
  });
}

describe('BigQueryDriver.testConnection', () => {
  // Listing every dataset paged through the whole project, which took tens of seconds on a
  // large one and made /livez outlast its probes.
  test('lists one dataset, not every dataset in the project', async () => {
    const getDatasets = jest.fn(async () => [[], null, {}]);

    await driverWith(getDatasets).testConnection();

    expect(getDatasets).toHaveBeenCalledTimes(1);
    expect(getDatasets).toHaveBeenCalledWith({ maxResults: 1, autoPaginate: false });
  });

  test('passes a listing error through', async () => {
    const getDatasets = jest.fn(async () => {
      throw new Error('Access Denied');
    });

    await expect(driverWith(getDatasets).testConnection()).rejects.toThrow('Access Denied');
  });
});
