// https://github.com/cube-js/cube/issues/7188
//
// Lambda pre-aggregations (union_with_source_data) download source rows through
// QueryCache.csvQuery(). It must consume the whole driver stream before calling
// tableData.release(): drivers like MSSQL implement release() as request.cancel(),
// which aborts a still-running query with "Canceled.".
import crypto from 'crypto';
import { Readable } from 'stream';

import { QueryCache } from '../../src';

class QueryCacheOpened extends QueryCache {
  public csvQuery(client: any, q: any) {
    return super.csvQuery(client, q);
  }
}

const lambdaTypes = [
  { name: 'e__ts_hour', type: 'timestamp' },
  { name: 'e__count', type: 'int' },
];

/**
 * Mimics MSSqlDriver.stream(): rows arrive asynchronously from the server and
 * release() cancels the in-flight request (QueryStream._destroy -> request.cancel()).
 */
function mssqlLikeClient(totalRows: number) {
  let released = false;
  const client = {
    released: () => released,
    stream: async () => {
      let sent = 0;
      const rowStream = new Readable({
        objectMode: true,
        read() {
          setImmediate(() => {
            if (sent >= totalRows) {
              this.push(null);
              return;
            }
            sent++;
            this.push({ e__ts_hour: `2026-10-03T${String(sent % 24).padStart(2, '0')}:00:00.000`, e__count: sent });
          });
        },
      });
      return {
        rowStream,
        types: lambdaTypes,
        release: async () => {
          released = true;
          rowStream.destroy(new Error('Canceled.'));
        },
      };
    },
  };
  return client;
}

describe('QueryCache.csvQuery (lambda source download)', () => {
  const cache = new QueryCacheOpened(
    crypto.randomBytes(16).toString('hex'),
    () => {
      throw new Error('driverFactory is not implemented, mock should be used...');
    },
    jest.fn(),
    { cacheAndQueueDriver: 'memory', backgroundRenew: false },
  );

  afterAll(async () => {
    await cache.cleanup();
  });

  test('reads every streamed row before releasing the stream', async () => {
    const client = mssqlLikeClient(50);

    const result = await cache.csvQuery(client, {
      query: 'SELECT 1',
      values: [],
      lambdaTypes,
    });

    expect(client.released()).toBe(true);
    expect(result.rowCount).toBe(50);
    expect(result.csvRows.split('\n').filter(Boolean)).toHaveLength(50);
  });
});
