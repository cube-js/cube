/* eslint-disable no-restricted-syntax */
import { PassThrough, Readable } from 'stream';

import { BigQueryDriver } from '../../src';

// Regression tests for #10875: errors from the BigQuery source stream must
// reach the returned rowStream instead of crashing the process, and
// destroying rowStream must tear down the source.
describe('BigQueryDriver.stream — lifecycle propagation (issue #10875)', () => {
  function newDriverWithMockSource(mockSource: Readable): BigQueryDriver {
    const driver = new BigQueryDriver({});
    (driver as unknown as { bigquery: { createQueryStream: () => Readable } }).bigquery = {
      createQueryStream: () => mockSource,
    };
    return driver;
  }

  it('forwards source-stream errors to the returned rowStream', async () => {
    const source = new PassThrough({ objectMode: true });
    const driver = newDriverWithMockSource(source);

    const { rowStream } = await driver.stream('SELECT 1', []);

    const observedError = new Promise<Error>((resolve) => {
      rowStream.on('error', (err: Error) => resolve(err));
    });

    const cause = new Error('No matching signature for operator = for argument types: TIMESTAMP, DATE');
    source.destroy(cause);

    await expect(observedError).resolves.toBe(cause);
  });

  it('propagates rowStream destruction back to the source stream', async () => {
    const source = new PassThrough({ objectMode: true });
    const driver = newDriverWithMockSource(source);

    const { rowStream } = await driver.stream('SELECT 1', []);

    const sourceDestroyed = new Promise<void>((resolve) => {
      source.on('close', () => resolve());
    });

    (rowStream as PassThrough).destroy();

    await expect(sourceDestroyed).resolves.toBeUndefined();
    expect(source.destroyed).toBe(true);
  });
});
