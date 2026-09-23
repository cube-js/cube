import { CubeStoreDriver } from '../src/CubeStoreDriver';

jest.mock('@cubejs-backend/native', () => ({
  parseCubestoreResultMessage: jest.fn(),
}));

class FailingCubeStoreDriver extends CubeStoreDriver {
  public async query<R = any>(): Promise<R[]> {
    throw new Error('Internal: Timeout during create table finalization: earnings_rollup');
  }
}

describe('CubeStoreDriver.createTableWithOptions', () => {
  it('reports only the table name and the underlying error on failure', async () => {
    const driver = new FailingCubeStoreDriver();

    await expect(driver.createTableWithOptions(
      'cube_agg.earnings_rollup',
      [{ name: 'id', type: 'int' }],
      {
        inputFormat: 'csv_no_header',
        indexes: 'INDEX idx (id)',
        files: ['https://bucket.s3.amazonaws.com/data_0.csv.gz?X-Amz-Security-Token=secret'],
      },
      {},
    )).rejects.toThrow(
      new Error(
        'Error during create table cube_agg.earnings_rollup: ' +
        'Internal: Timeout during create table finalization: earnings_rollup'
      )
    );
  });
});
