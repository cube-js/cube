// https://github.com/cube-js/cube/issues/10875
// Streaming SQL API: BigQuery driver does not forward stream errors.
// An 'error' emitted by the BigQuery query stream must reach the returned rowStream,
// so the native bridge (which listens on rowStream 'error') can report it to the client.
import { PassThrough } from 'stream';
import { BigQueryDriver } from '../src';

describe('issue-10875: BigQueryDriver.stream forwards source stream errors', () => {
  test('source stream error is emitted on rowStream', async () => {
    const source = new PassThrough({ objectMode: true });
    // Keep the source error from crashing the test process when it is NOT forwarded,
    // so we observe the actual bug (rowStream never errors) instead of an uncaught exception.
    const sourceErrors: Error[] = [];
    source.on('error', (e) => sourceErrors.push(e));

    const driver = Object.create(BigQueryDriver.prototype) as BigQueryDriver;
    (driver as any).bigquery = { createQueryStream: async () => source };

    const { rowStream } = await driver.stream('SELECT 1', [], {} as any);

    const result = new Promise<Error | null>((resolve) => {
      rowStream.on('error', (e: Error) => resolve(e));
      setTimeout(() => resolve(null), 500);
    });

    source.emit('error', new Error('No matching signature for operator = for argument types: TIMESTAMP, DATE'));

    const err = await result;
    expect(sourceErrors).toHaveLength(1);
    expect(err).not.toBeNull();
    expect(err?.message).toContain('No matching signature');
  });
});
