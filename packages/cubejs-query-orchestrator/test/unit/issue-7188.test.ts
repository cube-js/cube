// https://github.com/cube-js/cube/issues/7188
// Lambda pre-aggregations (union_with_source_data) download the source rows through
// QueryCache#csvQuery. It awaits the *callback* form of `stream.pipeline`, which returns
// immediately, so `tableData.release()` runs while rows are still streaming. MSSqlDriver
// implements release() as `request.cancel()`, which fails the query with "Canceled.".
import crypto from 'crypto';
import { EventEmitter } from 'events';
import { Readable } from 'stream';

import { QueryCache } from '../../src';

class TestQueryCache extends QueryCache {
  public csvQuery(client: any, q: any) {
    return super.csvQuery(client, q);
  }
}

const lambdaTypes = [
  { name: 'e__ts_hour', type: 'timestamp' },
  { name: 'e__count', type: 'int' },
];

/**
 * Mirrors MSSqlDriver#stream + QueryStream: a tedious-like request emits rows
 * asynchronously, and release() calls request.cancel(), which errors the stream
 * with "Canceled." if the request is still running.
 */
function mssqlLikeClient(totalRows: number) {
  let canceledMidStream = false;
  return {
    canceledMidStream: () => canceledMidStream,
    stream: async () => {
      const request = new EventEmitter() as EventEmitter & { done: boolean, cancel: () => void };
      request.done = false;
      let sent = 0;
      const rowStream = new Readable({
        objectMode: true,
        read() {
          setImmediate(() => {
            if (request.done) return;
            if (sent >= totalRows) {
              request.done = true;
              this.push(null);
              return;
            }
            sent++;
            this.push({ e__ts_hour: '2023-10-03T00:00:00.000', e__count: sent });
          });
        },
      });
      request.cancel = () => {
        if (!request.done) {
          canceledMidStream = true;
          request.done = true;
          rowStream.destroy(new Error('Canceled.'));
        }
      };
      return {
        rowStream,
        types: lambdaTypes,
        release: async () => request.cancel(),
      };
    },
  };
}

describe('issue-7188: lambda source download releases the MSSQL stream too early', () => {
  const cache = new TestQueryCache(
    crypto.randomBytes(16).toString('hex'),
    () => {
      throw new Error('driverFactory is not used');
    },
    jest.fn(),
    { cacheAndQueueDriver: 'memory' } as any,
  );

  afterAll(async () => {
    await cache.cleanup();
  });

  test('csvQuery reads every row before releasing the driver stream', async () => {
    const client = mssqlLikeClient(50);

    const result = await cache.csvQuery(client, { query: 'SELECT 1', values: [], lambdaTypes });

    expect(client.canceledMidStream()).toBe(false);
    expect(result.rowCount).toBe(50);
  });
});
