// Regression test for https://github.com/cube-js/cube/issues/10627
//
// Export bucket unload for a Trino source backed by an Iceberg catalog fails with
// "Catalog 'iceberg' table property 'external_location' does not exist", because
// the driver always issues `CREATE TABLE ... WITH (external_location = ..., format = 'CSV')`,
// which only the Hive connector understands.
import { TrinoDriver } from '../../src';

const mockExecute: jest.Mock = jest.fn();

jest.mock('@cubejs-backend/schema-compiler', () => ({
  PrestodbQuery: class { },
}));

jest.mock('presto-client', () => ({
  Client: jest.fn().mockImplementation(() => ({
    execute: (...args: any[]) => mockExecute(...args),
    nodes: jest.fn(),
  })),
}));

class TestTrinoDriver extends TrinoDriver {
  public override async extractFilesFromGCS(): Promise<string[]> {
    return ['https://storage.googleapis.com/my-bucket/my_schema/my_table/part-0.csv'];
  }
}

describe('issue 10627: Trino export bucket with Iceberg catalog', () => {
  beforeEach(() => {
    mockExecute.mockReset();
    mockExecute.mockImplementation((opts: any) => {
      if (opts.columns) {
        // Streaming `LIMIT 0` call used by queryColumnTypes().
        opts.columns(null, [{ name: 'id', type: 'integer' }]);
      }
      opts.success?.();
    });
  });

  it('does not use the Hive-only external_location table property on an Iceberg catalog', async () => {
    const driver = new TestTrinoDriver({
      host: 'trino.local',
      port: '8080',
      catalog: 'iceberg',
      schema: 'my_schema',
      bucketType: 'gcs',
      exportBucket: 'my-bucket',
      credentials: {},
    });

    await driver.unload('my_schema.my_table', {
      maxFileSize: 64,
      query: { sql: 'SELECT 1 AS id', params: [] },
    });

    const queries: string[] = mockExecute.mock.calls.map(([opts]) => opts.query);
    const createTable = queries.find((q) => q.startsWith('CREATE TABLE'));

    expect(createTable).toBeDefined();
    expect(createTable).toContain('iceberg.my_schema.my_table');
    // Trino's Iceberg connector has no `external_location` table property
    // (and no CSV `format`), so this statement fails on Iceberg catalogs.
    expect(createTable).not.toMatch(/external_location/);
  });
});
