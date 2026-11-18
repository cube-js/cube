import { PassThrough, Readable } from 'stream';
import { once } from 'events';

import { BigQueryDriver } from '../src';

class BigQueryDriverOpen extends BigQueryDriver {
  public useQueryStream(source: Readable): void {
    // `bigquery` is readonly, and the real constructor needs credentials
    Object.defineProperty(this, 'bigquery', {
      value: { createQueryStream: () => source },
    });
  }
}

function createDriver(source: Readable): BigQueryDriverOpen {
  const driver = Object.create(BigQueryDriverOpen.prototype) as BigQueryDriverOpen;
  driver.useQueryStream(source);

  return driver;
}

describe('BigQueryDriver.stream', () => {
  test('forwards a mid-stream source error to rowStream', async () => {
    const source = new PassThrough({ objectMode: true });
    const { rowStream } = await createDriver(source).stream('SELECT 1', []);

    const failure = once(rowStream, 'error');
    const cause = new Error('No matching signature for operator = for argument types: TIMESTAMP, DATE');
    source.destroy(cause);

    const [error] = await failure;
    expect(error).toBe(cause);
  });

  test('destroys the source when rowStream is destroyed', async () => {
    const source = new PassThrough({ objectMode: true });
    const { rowStream } = await createDriver(source).stream('SELECT 1', []);

    // not events.once(): pipeline destroys the source with a premature-close
    // error, which would reject the promise before 'close' arrives
    const closed = new Promise<void>((resolve) => {
      source.on('close', () => resolve());
    });
    (rowStream as Readable).destroy();

    await closed;
    expect(source.destroyed).toBe(true);
  });

  test('hydrates rows and ends with the source', async () => {
    const source = new PassThrough({ objectMode: true });
    const { rowStream } = await createDriver(source).stream('SELECT 1', []);

    source.write({ id: 1, created_at: { value: '2024-01-08T00:00:00.000Z' } });
    source.end({ id: 2, created_at: null });

    const rows: unknown[] = [];

    for await (const row of rowStream) {
      rows.push(row);
    }

    expect(rows).toEqual([
      { id: '1', created_at: '2024-01-08T00:00:00.000Z' },
      { id: '2', created_at: null },
    ]);
  });
});
