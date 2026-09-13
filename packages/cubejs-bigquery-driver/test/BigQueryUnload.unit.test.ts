import { BigQueryDriver } from '../src';

function fixture(exportBucketFormat?: 'csv' | 'parquet') {
  const extracted: string[] = [];
  const deleted: string[] = [];
  const createExtractJob = jest.fn(async (destination: { name: string }, _options: unknown) => {
    extracted.push(destination.name);
    return [{}];
  });
  const getSignedUrl = jest.fn(async () => ['https://signed.example/object']);
  const getFiles = jest.fn(async ({ prefix }: { prefix: string }) => [
    [
      { name: `${prefix}part-0000.${extracted.some(p => p.startsWith(prefix) && p.endsWith('.parquet')) ? 'parquet' : 'csv.gz'}`,
        getSignedUrl,
        delete: jest.fn(async () => { deleted.push(prefix); }) },
      { name: 'another-export/stale.parquet', delete: jest.fn(), getSignedUrl },
    ],
  ] as any);
  const driver = Object.create(BigQueryDriver.prototype) as BigQueryDriver;
  Object.assign(driver, {
    options: { exportBucketFormat },
    bucket: { name: 'bucket', file: (name: string) => ({ name }), getFiles },
    bigquery: { dataset: () => ({ table: () => ({ createExtractJob }) }) },
    waitForJobResult: jest.fn(async () => undefined),
  });
  return { driver, createExtractJob, getSignedUrl, extracted, deleted, getFiles };
}

describe('BigQuery unload format and isolation', () => {
  test.each([undefined, 'parquet'] as const)('uses CSV without explicit destination support (%s)', async format => {
    const f = fixture(format);
    const result = await f.driver.unload('schema.table');
    expect(result).toHaveProperty('csvFile', ['https://signed.example/object']);
    expect(f.createExtractJob.mock.calls[0][1]).toEqual({ format: 'CSV', gzip: true });
    expect(f.getSignedUrl).toHaveBeenCalledTimes(1);
  });

  test('uses GCS Parquet only when configured and supported; releases only its own files', async () => {
    const f = fixture('parquet');
    const results = await Promise.all([
      f.driver.unload('schema.table', { maxFileSize: 64, parquetImport: true }),
      f.driver.unload('schema.table', { maxFileSize: 64, parquetImport: true }),
    ]);
    expect(new Set(f.extracted).size).toBe(2);
    expect(f.deleted).toEqual([]);
    expect(f.getSignedUrl).not.toHaveBeenCalled();
    for (const result of results) {
      expect(result).toHaveProperty('parquetFile');
      const files = (result as { parquetFile: string[] }).parquetFile;
      expect(files).toHaveLength(1);
      expect(files[0]).toMatch(/^gs:\/\/bucket\/schema.table-.*\/part-0000.parquet$/);
      await result.release!();
    }
    expect(new Set(f.deleted).size).toBe(2);
  });
});
