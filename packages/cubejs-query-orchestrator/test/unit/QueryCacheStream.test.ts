import { Readable } from 'stream';

import { QueryOrchestrator } from '../../src/orchestrator/QueryOrchestrator';

class StreamMockDriver {
  public released = 0;

  public constructor(public readonly source: Readable) {}

  public capabilities() {
    return {};
  }

  public async query(): Promise<unknown[]> {
    return [];
  }

  public async stream() {
    return {
      rowStream: this.source,
      release: async (): Promise<void> => {
        this.released += 1;
      },
    };
  }

  public async release(): Promise<null> {
    return null;
  }

  public nowTimestamp() {
    return new Date().getTime();
  }
}

const tick = () => new Promise((resolve) => setTimeout(resolve, 20));

describe('QueryCache stream handler', () => {
  let orchestrator: QueryOrchestrator;
  let logs: { msg: string, params: Record<string, any> }[];
  let counter = 0;
  const uncaught: unknown[] = [];
  const onUncaught = (err: unknown) => uncaught.push(err);

  const streamFrom = async (source: Readable) => {
    const driver = new StreamMockDriver(source);
    counter += 1;
    orchestrator = new QueryOrchestrator(
      `STREAM_TEST_${counter}`,
      () => driver as any,
      (msg, params) => logs.push({ msg, params }),
      {
        cacheAndQueueDriver: 'memory',
        queryCacheOptions: {
          queueOptions: () => ({ concurrency: 1 }),
        },
      },
    );

    const target: Readable = await orchestrator.fetchQuery({
      query: 'SELECT 1', values: [], requestId: 'stream-test', persistent: true,
    });

    return { driver, target };
  };

  const logged = (msg: string) => logs.filter((l) => l.msg === msg);

  beforeEach(() => {
    logs = [];
    uncaught.length = 0;
    process.on('uncaughtException', onUncaught);
  });

  afterEach(async () => {
    process.off('uncaughtException', onUncaught);
    await orchestrator.cleanup();
  });

  test('streams rows and releases the source', async () => {
    const rows = [{ id: 1 }, { id: 2 }];
    const { driver, target } = await streamFrom(Readable.from(rows));

    const received: unknown[] = [];

    for await (const row of target) {
      received.push(row);
    }
    await tick();

    expect(received).toEqual(rows);
    expect(driver.released).toBe(1);
    expect(logged('Streaming successfully completed')).toHaveLength(1);
    expect(logged('Streaming done with error')).toHaveLength(0);
  });

  test('forwards a mid-stream source error to the reader', async () => {
    const error = new Error('Bad double value: abc');
    const pending: unknown[] = [{ id: 1 }];
    const source = new Readable({
      objectMode: true,
      read() {
        if (pending.length > 0) {
          this.push(pending.shift());
        } else {
          this.destroy(error);
        }
      },
    });
    const { driver, target } = await streamFrom(source);

    const read = (async () => {
      for await (const _row of target) {
        // drain
      }
    })();

    await expect(read).rejects.toBe(error);
    await tick();

    expect(driver.released).toBe(1);
    expect(logged('Streaming done with error')).toHaveLength(1);
    expect(logged('Streaming successfully completed')).toHaveLength(0);
    expect(uncaught).toEqual([]);
  });

  test('destroys the source when the reader abandons the stream', async () => {
    const source = new Readable({ objectMode: true, read() { this.push({ id: 1 }); } });
    const { driver, target } = await streamFrom(source);

    target.destroy();
    await tick();

    expect(source.destroyed).toBe(true);
    expect(driver.released).toBe(1);
    expect(logged('Streaming successfully completed')).toHaveLength(1);
    expect(logged('Streaming done with error')).toHaveLength(0);
    expect(uncaught).toEqual([]);
  });

  test('fails the stream when the source closes before ending', async () => {
    const source = new Readable({ objectMode: true, read() { /* never ends */ } });
    const { target } = await streamFrom(source);

    const read = (async () => {
      for await (const _row of target) {
        // drain
      }
    })();

    source.destroy();

    await expect(read).rejects.toMatchObject({ code: 'ERR_STREAM_PREMATURE_CLOSE' });
    await tick();

    expect(logged('Streaming done with error')).toHaveLength(1);
    expect(uncaught).toEqual([]);
  });
});
