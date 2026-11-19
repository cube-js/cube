import { Readable, Writable, pipeline } from 'stream';

import { withLargeStreamWarning } from '../../src/orchestrator/StreamObjectsCounter';

function failingSource(rows: unknown[], error: Error): Readable {
  const pending = [...rows];

  return new Readable({
    objectMode: true,
    read() {
      if (pending.length > 0) {
        this.push(pending.shift());
      } else {
        this.destroy(error);
      }
    },
  });
}

function collect(stream: NodeJS.ReadableStream): Promise<unknown[]> {
  const rows: unknown[] = [];

  return new Promise((resolve, reject) => {
    pipeline(
      stream,
      new Writable({
        objectMode: true,
        write(row, _encoding, callback) {
          rows.push(row);
          callback();
        },
      }),
      (err) => (err ? reject(err) : resolve(rows))
    );
  });
}

describe('withLargeStreamWarning', () => {
  const uncaught: unknown[] = [];
  const onUncaught = (err: unknown) => uncaught.push(err);

  beforeEach(() => {
    uncaught.length = 0;
    process.on('uncaughtException', onUncaught);
  });

  afterEach(() => {
    process.off('uncaughtException', onUncaught);
  });

  test('passes rows through', async () => {
    const rows = [{ id: 1 }, { id: 2 }, { id: 3 }];
    const stream = withLargeStreamWarning(Readable.from(rows), 'orders.main', () => undefined);

    await expect(collect(stream)).resolves.toEqual(rows);
  });

  test('forwards a mid-stream source error to the consumer', async () => {
    const error = new Error('Bad double value: abc');
    const stream = withLargeStreamWarning(failingSource([{ id: 1 }], error), 'orders.main', () => undefined);

    await expect(collect(stream)).rejects.toBe(error);
    await new Promise((resolve) => setImmediate(resolve));

    expect(uncaught).toEqual([]);
  });

  test('does not leave a source error unhandled when it fires before a consumer attaches', async () => {
    const error = new Error('Bad double value: abc');
    const stream = withLargeStreamWarning(failingSource([], error), 'orders.main', () => undefined);

    await new Promise((resolve) => setImmediate(resolve));

    expect(uncaught).toEqual([]);
    expect(stream.destroyed).toBe(true);
    await expect(collect(stream)).rejects.toBeDefined();
  });

  test('destroys the source when the consumer gives up', async () => {
    const source = new Readable({ objectMode: true, read() { this.push({ id: 1 }); } });
    const stream = withLargeStreamWarning(source, 'orders.main', () => undefined);

    stream.destroy(new Error('Import aborted'));
    await new Promise((resolve) => setImmediate(resolve));

    expect(source.destroyed).toBe(true);
    expect(uncaught).toEqual([]);
  });
});
