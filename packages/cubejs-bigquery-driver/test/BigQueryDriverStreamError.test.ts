/* eslint-disable no-restricted-syntax */
import { PassThrough, Readable } from 'stream';

import { BigQueryDriver } from '../src';

// Regression tests for cube-js/cube#10875 — `BigQueryDriver.stream` must:
//   (1) Forward errors emitted by the underlying BigQuery source stream to
//       the returned rowStream, so that the cubejs-server stream pump can
//       observe them via `rowStream.on('error', ...)`. Without this, the
//       source's 'error' event has no listener (Node's `stream.pipe` does
//       NOT forward error events) and the process terminates via Node's
//       unhandled-rejection handler.
//   (2) Propagate consumer-side cancellation: destroying the returned
//       rowStream must destroy the BigQuery source stream too, so the
//       driver doesn't keep paging results into the void after the consumer
//       has gone away.
//
// Both behaviours come "for free" from `stream.pipeline`. These tests stub
// out the BigQuery client with a synthetic PassThrough so the assertions
// hold without contacting a real BigQuery instance.
describe('BigQueryDriver.stream — lifecycle propagation (issue #10875)', () => {
  function newDriverWithMockSource(mockSource: Readable): BigQueryDriver {
    const driver = new BigQueryDriver({});
    // Replace the @google-cloud/bigquery client with a stub that yields our
    // synthetic source. Cast through `unknown` because the field is declared
    // `readonly` but we need to inject for the test.
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

    // The consumer cancels mid-stream. With `pipeline`, the destruction of
    // the destination propagates to the source. With a bare `.pipe()`, the
    // source would keep running until BigQuery itself terminated the call.
    (rowStream as PassThrough).destroy();

    await expect(sourceDestroyed).resolves.toBeUndefined();
    expect(source.destroyed).toBe(true);
  });
});
