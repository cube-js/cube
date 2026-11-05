import { Readable } from 'stream';

import { QueryCache } from '../../src';

class QueryCacheOpened extends QueryCache {
  public async csvQuery(client, q) {
    return super.csvQuery(client, q);
  }
}

/**
 * A source that, like a driver's row stream over HTTP, produces rows asynchronously and whose
 * underlying connection is torn down by `release()` (ClickHouse: `client.close()`), which aborts
 * the stream with `Error: aborted` / ECONNRESET if it is still in flight.
 *
 * https://github.com/cube-js/cube/issues/9566
 */
function slowStreamingClient(rowCount: number) {
  let produced = 0;
  let released = false;
  let releasedBeforeEnd = false;

  const rowStream = new Readable({
    objectMode: true,
    read() {
      setTimeout(() => {
        if (produced >= rowCount) {
          this.push(null);
          return;
        }
        produced++;
        this.push({ id: produced, name: `row ${produced}` });
      }, 1);
    },
  });

  const client = {
    stream: async () => ({
      rowStream,
      types: [{ name: 'id', type: 'int' }, { name: 'name', type: 'text' }],
      release: async () => {
        released = true;
        if (!rowStream.readableEnded) {
          releasedBeforeEnd = true;
          const err: any = new Error('aborted');
          err.code = 'ECONNRESET';
          rowStream.destroy(err);
        }
      },
    }),
  };

  return {
    client,
    state: () => ({ produced, released, releasedBeforeEnd }),
  };
}

describe('QueryCache.csvQuery (lambda source data)', () => {
  const queryCache = new QueryCacheOpened(
    'TEST',
    () => { throw new Error('driverFactory is not used'); },
    () => undefined,
    { cacheAndQueueDriver: 'memory' },
  );

  const q = {
    query: 'SELECT id, name FROM source',
    values: [],
    lambdaTypes: [{ name: 'id', type: 'int' }, { name: 'name', type: 'text' }],
  };

  it('releases the stream only after it has been fully consumed', async () => {
    const rowCount = 50;
    const { client, state } = slowStreamingClient(rowCount);

    const result = await queryCache.csvQuery(client, q);

    expect(state().releasedBeforeEnd).toBe(false);
    expect(state().released).toBe(true);
    expect(result.rowCount).toBe(rowCount);
  });

  it('surfaces a stream error as a lambda query error', async () => {
    const rowStream = new Readable({
      objectMode: true,
      read() {
        setTimeout(() => this.destroy(new Error('boom')), 1);
      },
    });
    const release = jest.fn(async () => undefined);
    const client = {
      stream: async () => ({ rowStream, types: q.lambdaTypes, release }),
    };

    await expect(queryCache.csvQuery(client, q)).rejects.toThrow(/Lambda query errors.*boom/);
    expect(release).toHaveBeenCalledTimes(1);
  });
});
