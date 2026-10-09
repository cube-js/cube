import { AthenaDriver } from '../../src/AthenaDriver';

class TestAthenaDriver extends AthenaDriver {
  public readonly started: string[] = [];

  public readonly listedPrefixes: string[] = [];

  protected async startQuery(query: string, _values: unknown[]) {
    this.started.push(query);
    return { QueryExecutionId: `q${this.started.length}` };
  }

  protected async waitForSuccess() {
    // Every stubbed query succeeds immediately
  }

  public async queryColumnTypes() {
    return [{ name: 'id', type: 'int' }];
  }

  protected async extractUnloadedFilesFromS3(_options: unknown, _bucket: string, prefix: string) {
    this.listedPrefixes.push(prefix);
    return [`https://signed/${prefix}/part-0.gz`];
  }
}

const createDriver = (config: Record<string, unknown> = {}) => {
  const driver = new TestAthenaDriver({
    region: 'us-east-1',
    exportBucket: 's3://export-bucket/exports',
    ...config,
  });
  (driver as any).athena.getQueryResults = async () => ({});
  return driver;
};

describe('AthenaDriver readOnly', () => {
  it('is readOnly by default', () => {
    expect(createDriver().readOnly()).toBe(true);
    expect(createDriver({ exportBucket: undefined }).readOnly()).toBe(true);
  });

  it('respects an explicit readOnly: false', () => {
    expect(createDriver({ readOnly: false }).readOnly()).toBe(false);
  });
});

describe('AthenaDriver.unloadFromQuery', () => {
  it('unloads the query into a fresh prefix of the export bucket', async () => {
    const driver = createDriver();

    const first = await driver.unloadFromQuery('SELECT id FROM t WHERE x = ?', [1], { maxFileSize: 64 });
    const second = await driver.unloadFromQuery('SELECT id FROM t WHERE x = ?', [1], { maxFileSize: 64 });

    expect(driver.started).toHaveLength(2);
    const locations = driver.started.map((sql) => /TO 's3:\/\/export-bucket\/exports\/([^']+)'/.exec(sql)![1]);
    expect(locations[0]).not.toEqual(locations[1]);
    expect(driver.started[0]).toContain('UNLOAD (SELECT id FROM t WHERE x = ?)');
    expect(driver.listedPrefixes).toEqual(locations.map((l) => `exports/${l}`));

    expect(first).toMatchObject({
      csvFile: [`https://signed/exports/${locations[0]}/part-0.gz`],
      types: [{ name: 'id', type: 'int' }],
      csvNoHeader: true,
      csvDelimiter: '^A',
      csvDisableQuoting: true,
    });
    expect(second.csvFile).toEqual([`https://signed/exports/${locations[1]}/part-0.gz`]);
  });

  it('serves unload() with a query through unloadFromQuery', async () => {
    const driver = createDriver();

    await driver.unload('prod_pre_aggregations.orders_main', {
      maxFileSize: 64,
      query: { sql: 'SELECT id FROM t', params: [] },
    });

    expect(driver.started).toHaveLength(1);
    expect(driver.started[0]).toContain('UNLOAD (SELECT id FROM t)');
    expect(driver.started[0]).not.toContain('orders_main');
  });

  it('fails without an export bucket', async () => {
    await expect(
      createDriver({ exportBucket: undefined }).unloadFromQuery('SELECT 1', [], { maxFileSize: 64 })
    ).rejects.toThrow('Export bucket is not configured.');
  });
});
