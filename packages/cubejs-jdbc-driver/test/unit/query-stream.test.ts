import { QueryStream } from '../../src/QueryStream';
import type { Row } from '../../src/QueryStream';

function batches(source: Row[][]) {
  const queue = [...source];
  let inFlight = 0;
  let maxInFlight = 0;
  let calls = 0;

  const next = async () => {
    calls++;
    inFlight++;
    maxInFlight = Math.max(maxInFlight, inFlight);
    await new Promise((resolve) => { setImmediate(resolve); });
    inFlight--;

    return queue.shift() || null;
  };

  return { next, stats: () => ({ calls, maxInFlight }) };
}

async function collect(stream: QueryStream): Promise<Row[]> {
  const rows: Row[] = [];

  for await (const row of stream) {
    rows.push(row);
  }
  return rows;
}

describe('QueryStream', () => {
  it('emits rows of every batch in order', async () => {
    const { next, stats } = batches([[{ id: 1 }, { id: 2 }], [{ id: 3 }], [{ id: 4 }, { id: 5 }]]);

    expect(await collect(new QueryStream(next, 2))).toEqual(
      [{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }, { id: 5 }]
    );
    expect(stats()).toEqual({ calls: 4, maxInFlight: 1 });
  });

  it('ends on an empty result set', async () => {
    const { next } = batches([]);

    expect(await collect(new QueryStream(next, 16))).toEqual([]);
  });

  it('never fetches two batches concurrently', async () => {
    const source = Array.from({ length: 50 }, (_, i) => [{ id: i * 2 }, { id: i * 2 + 1 }]);
    const { next, stats } = batches(source);

    // highWaterMark above the batch size makes the stream ask for more while a fetch is pending
    const rows = await collect(new QueryStream(next, 1000));

    expect(rows).toHaveLength(100);
    expect(stats().maxInFlight).toBe(1);
  });

  it('stops fetching once the consumer has enough buffered rows', async () => {
    const source = Array.from({ length: 100 }, (_, i) => [{ id: i }]);
    const { next, stats } = batches(source);
    const stream = new QueryStream(next, 4);

    stream.read(0);
    await new Promise((resolve) => { setTimeout(resolve, 50); });

    expect(stats().calls).toBeLessThan(10);
    stream.destroy();
  });

  it('fails the stream when a batch fails', async () => {
    let calls = 0;
    const next = async () => {
      if (calls++ === 0) {
        return [{ id: 1 }];
      }
      throw new Error('fetch failed');
    };

    await expect(collect(new QueryStream(next, 16))).rejects.toThrow('fetch failed');
  });

  it('does not push after destroy', async () => {
    const pending: ((rows: Row[]) => void)[] = [];
    const next = () => new Promise<Row[]>((resolve) => { pending.push(resolve); });
    const stream = new QueryStream(next, 16);
    const onData = jest.fn();

    stream.on('data', onData);
    await new Promise((resolve) => { setImmediate(resolve); });
    stream.destroy();
    pending.forEach((resolve) => resolve([{ id: 1 }]));
    await new Promise((resolve) => { setImmediate(resolve); });

    expect(onData).not.toHaveBeenCalled();
  });
});
