import { PassThrough, Writable } from 'stream';
import type { BigQuery } from '@google-cloud/bigquery';

import { BigQueryDriver } from '../../src';
import { HydrationStream } from '../../src/HydrationStream';

type QueryStream = ReturnType<BigQuery['createQueryStream']>;

class BigQueryDriverOpen extends BigQueryDriver {
  public constructor(source: PassThrough) {
    super({});
    // A PassThrough stands in for the paginator's ResourceStream, which is a Transform too.
    jest.spyOn(this.bigquery, 'createQueryStream').mockReturnValue(source as unknown as QueryStream);
  }
}

const sourceError = () => new Error('No matching signature for operator = for argument types: TIMESTAMP, DATE');

describe('BigQueryDriver.stream — lifecycle propagation (issue #10875)', () => {
  it('forwards source-stream errors to the returned rowStream', async () => {
    const source = new PassThrough({ objectMode: true });
    const { rowStream } = await new BigQueryDriverOpen(source).stream('SELECT 1', []);

    const observedError = new Promise<Error>((resolve) => {
      rowStream.on('error', resolve);
    });

    const cause = sourceError();
    source.destroy(cause);

    await expect(observedError).resolves.toBe(cause);
  });

  it('propagates rowStream destruction back to the source stream', async () => {
    const source = new PassThrough({ objectMode: true });
    const { rowStream } = await new BigQueryDriverOpen(source).stream('SELECT 1', []);

    const sourceClosed = new Promise<void>((resolve) => {
      source.on('close', resolve);
    });

    expect(rowStream).toBeInstanceOf(HydrationStream);
    if (rowStream instanceof HydrationStream) {
      rowStream.destroy();
    }

    await sourceClosed;
    expect(source.destroyed).toBe(true);
  });

  // Same wiring as QueryCache's streamHandler: listeners on rowStream and target, rowStream.pipe(target).
  it('delivers rows, then the error, to a consumer piping rowStream into a target', async () => {
    const source = new PassThrough({ objectMode: true });
    const { rowStream } = await new BigQueryDriverOpen(source).stream('SELECT 1', []);

    const received: unknown[] = [];
    const target = new Writable({
      objectMode: true,
      write(row, _encoding, callback) {
        received.push(row);
        callback();
      },
    });

    const targetError = new Promise<Error>((resolve) => {
      target.once('error', resolve);
    });
    const outcome = new Promise<Error>((resolve, reject) => {
      rowStream.once('error', (err: Error) => {
        target.destroy(err);
        resolve(err);
      });
      rowStream.once('end', () => reject(new Error('rowStream ended without the source error')));
    });

    rowStream.pipe(target);

    source.write({ id: 1, amount: 2.5 });
    source.write({ id: 2, amount: 3 });
    await new Promise((resolve) => setImmediate(resolve));

    const cause = sourceError();
    source.destroy(cause);

    await expect(outcome).resolves.toBe(cause);
    expect(received).toEqual([{ id: '1', amount: '2.5' }, { id: '2', amount: '3' }]);
    await expect(targetError).resolves.toBe(cause);
    expect(target.destroyed).toBe(true);
  });
});
