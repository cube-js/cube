import { BigQueryDriver } from '../src';

type StoredFile = {
  name: string,
  getSignedUrl: jest.Mock,
  delete: jest.Mock,
};

/**
 * A bucket that behaves like GCS for what `unload` needs: an extract job
 * writes one shard for its wildcard destination, and listing returns every
 * object under the requested prefix, including other attempts' objects.
 */
function fixture(exportBucketFormat?: 'csv' | 'parquet', failDelete?: (name: string) => boolean) {
  const objects = new Map<string, StoredFile>();
  const deleted: string[] = [];
  const logger = jest.fn();

  const store = (name: string) => {
    const file: StoredFile = {
      name,
      getSignedUrl: jest.fn(async () => [`https://signed.example/${name}`]),
      delete: jest.fn(async () => {
        if (failDelete?.(name)) {
          throw new Error(`storage.objects.delete denied on ${name}`);
        }
        objects.delete(name);
        deleted.push(name);
      }),
    };
    objects.set(name, file);
    return file;
  };

  const createExtractJob = jest.fn(async (destination: { name: string }, _options: unknown) => {
    store(destination.name.replace('*', '000000000000'));
    return [{ id: 'job' }];
  });
  const getFiles = jest.fn(async ({ prefix }: { prefix: string }) => [
    [...objects.values()].filter(file => file.name.startsWith(prefix)),
  ]);

  const driver = Object.create(BigQueryDriver.prototype) as BigQueryDriver;
  Object.assign(driver, {
    options: { exportBucketFormat },
    bucket: { name: 'bucket', file: (name: string) => ({ name }), getFiles },
    bigquery: { dataset: () => ({ table: () => ({ createExtractJob }) }) },
    waitForJobResult: jest.fn(async () => undefined),
    logger,
  });

  return { driver, createExtractJob, getFiles, objects, deleted, store, logger };
}

const TABLE = 'schema.table';
const UUID_PREFIX = /^schema\.table-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\/$/;

function parquetPrefix(location: string) {
  return location.replace('gs://bucket/', '').replace(/part-\d+\.parquet$/, '');
}

describe('BigQuery unload', () => {
  describe('CSV (default)', () => {
    // Each case is a way the Parquet path could be reached by accident.
    test.each([
      ['format not configured, destination not asked', undefined, undefined],
      ['format not configured, destination supports Parquet', undefined, true],
      ['format csv, destination supports Parquet', 'csv', true],
      ['format parquet, destination did not declare support', 'parquet', undefined],
      ['format parquet, destination declined', 'parquet', false],
    ] as const)('keeps the previous export: %s', async (_name, format, parquetImport) => {
      const f = fixture(format);
      const result = await f.driver.unload(TABLE, parquetImport === undefined ? undefined : { maxFileSize: 64, parquetImport });

      const [destination, options] = f.createExtractJob.mock.calls[0];
      expect(destination.name).toBe('schema.table-*.csv.gz');
      expect(options).toEqual({ format: 'CSV', gzip: true });
      expect(f.getFiles).toHaveBeenCalledWith({ prefix: 'schema.table-' });
      expect(result).toHaveProperty('csvFile', ['https://signed.example/schema.table-000000000000.csv.gz']);
      expect(result).not.toHaveProperty('parquetFile');
    });

    test('does not delete anything, so it needs no delete permission', async () => {
      const f = fixture();
      const result = await f.driver.unload(TABLE);

      expect(result.release).toBeUndefined();
      expect(f.deleted).toEqual([]);
      expect([...f.objects.keys()]).toEqual(['schema.table-000000000000.csv.gz']);
    });
  });

  describe('Parquet (opt-in)', () => {
    const options = { maxFileSize: 64, parquetImport: true };

    test('exports Parquet to a per-attempt prefix and returns gs:// locations', async () => {
      const f = fixture('parquet');
      const result = await f.driver.unload(TABLE, options);

      const [destination, extractOptions] = f.createExtractJob.mock.calls[0];
      expect(extractOptions).toEqual({ format: 'PARQUET' });
      expect(destination.name).toMatch(/\/part-\*\.parquet$/);

      expect(result).not.toHaveProperty('csvFile');
      const files = (result as { parquetFile: string[] }).parquetFile;
      expect(files).toHaveLength(1);
      expect(parquetPrefix(files[0])).toMatch(UUID_PREFIX);
      // gs:// locations are read with the destination's own identity
      expect(f.objects.get(files[0].replace('gs://bucket/', ''))!.getSignedUrl).not.toHaveBeenCalled();
    });

    test('concurrent attempts never see or delete each other\'s objects', async () => {
      const f = fixture('parquet');
      // Left behind by an earlier CSV export and an earlier, abandoned Parquet attempt of the same table
      f.store('schema.table-000000000000.csv.gz');
      f.store('schema.table-00000000-0000-4000-8000-000000000000/part-000000000000.parquet');
      f.store('schema.table_other-000000000000.parquet');
      const before = [...f.objects.keys()];

      const [first, second] = await Promise.all([
        f.driver.unload(TABLE, options),
        f.driver.unload(TABLE, options),
      ]) as { parquetFile: string[], release: () => Promise<void> }[];

      expect(first.parquetFile).toHaveLength(1);
      expect(second.parquetFile).toHaveLength(1);
      expect(parquetPrefix(first.parquetFile[0])).not.toBe(parquetPrefix(second.parquetFile[0]));

      await first.release();
      expect(f.deleted).toEqual([first.parquetFile[0].replace('gs://bucket/', '')]);
      // the other attempt is still importable
      expect(f.objects.has(second.parquetFile[0].replace('gs://bucket/', ''))).toBe(true);

      await second.release();
      expect(f.deleted).toHaveLength(2);
      expect([...f.objects.keys()]).toEqual(before);
    });

    test('ignores objects under its prefix that are not Parquet shards', async () => {
      const f = fixture('parquet');
      f.createExtractJob.mockImplementationOnce(async (destination: { name: string }) => {
        f.store(destination.name.replace('*', '000000000000'));
        f.store(destination.name.replace('part-*.parquet', 'manifest.json'));
        return [{ id: 'job' }];
      });
      const result = await f.driver.unload(TABLE, options) as { parquetFile: string[] };

      expect(result.parquetFile).toHaveLength(1);
      expect(result.parquetFile[0]).toMatch(/part-000000000000\.parquet$/);
    });

    test('a failed cleanup is logged and does not replace the outcome of the import', async () => {
      const f = fixture('parquet', () => true);
      const result = await f.driver.unload(TABLE, options);

      await expect(result.release!()).resolves.toBeUndefined();
      expect(f.deleted).toEqual([]);
      expect(f.logger).toHaveBeenCalledTimes(1);
      const [message, params] = f.logger.mock.calls[0];
      expect(message).toBe('BigQuery Parquet Export Cleanup Failed');
      expect(params.warning).toMatch(/Could not delete 1 of 1 exported objects under gs:\/\/bucket\/schema\.table-.*storage\.objects\.delete denied/);
    });
  });

  test('requires an export bucket', async () => {
    const f = fixture('parquet');
    Object.assign(f.driver, { bucket: undefined });
    await expect(f.driver.unload(TABLE, { maxFileSize: 64, parquetImport: true })).rejects.toThrow('Unload is not configured');
  });
});
