/* globals describe, beforeEach, afterEach, test, expect */
import { Readable } from 'stream';
import { QueryOrchestrator } from '../../src/orchestrator/QueryOrchestrator';
import { QueryCache } from '../../src/orchestrator/QueryCache';

class TestQueryCache extends QueryCache {
  public traceQuery(req: Parameters<QueryCache['traceQuery']>[0]) {
    return super.traceQuery(req);
  }
}

class TraceMockDriver {
  public executedQueries: any[] = [];

  public streamedQueries: any[] = [];

  public capabilities() {
    return {};
  }

  public query(query: any) {
    this.executedQueries.push(query);
    return Promise.resolve([query]);
  }

  public async downloadQueryResults(query: any) {
    return this.query(query);
  }

  public async stream(query: any) {
    this.streamedQueries.push(query);
    return { rowStream: Readable.from([]), release: async (): Promise<void> => undefined };
  }

  public async tablesSchema() {
    return {};
  }

  public async getTablesQuery(): Promise<unknown[]> {
    return [];
  }

  public async release(): Promise<null> {
    return null;
  }

  public nowTimestamp() {
    return new Date().getTime();
  }
}

const SQL = 'SELECT 1';
const TRACE_ID = 'f47ac10b-58cc-4372-a567-0e02b2c3d479-span-1';
// What the comment carries: the export's trace_id (span suffix dropped).
const EXPECTED_COMMENT = '/* trace_id: f47ac10b-58cc-4372-a567-0e02b2c3d479 */';

describe('SQL trace comment', () => {
  let driver: TraceMockDriver;
  let orchestrator: QueryOrchestrator;
  let counter = 0;

  const run = async (overrides: { requestId?: string, query?: string } = {}) => orchestrator.fetchQuery({
    query: SQL,
    values: [],
    requestId: TRACE_ID,
    ...overrides,
  });

  const createOrchestrator = () => {
    counter += 1;
    orchestrator = new QueryOrchestrator(
      `TRACE_TEST_${counter}`,
      () => driver as any,
      () => undefined,
      {
        cacheAndQueueDriver: 'memory',
        queryCacheOptions: {
          queueOptions: () => ({ concurrency: 1 }),
        },
      },
    );
  };

  // The flag is read once, when the query cache is constructed.
  const enable = async () => {
    process.env.CUBEJS_SQL_INCLUDE_TRACE_ID = 'true';
    await orchestrator.cleanup();
    createOrchestrator();
  };

  beforeEach(() => {
    driver = new TraceMockDriver();
    createOrchestrator();
  });

  afterEach(async () => {
    delete process.env.CUBEJS_SQL_INCLUDE_TRACE_ID;
    await orchestrator.cleanup();
  });

  test('is absent when the env var is unset', async () => {
    await run();
    expect(driver.executedQueries).toContain(SQL);
    expect(driver.executedQueries.join('\n')).not.toContain('trace_id');
  });

  test('is appended when enabled', async () => {
    await enable();
    await run();
    expect(driver.executedQueries).toContain(`${SQL}\n${EXPECTED_COMMENT}`);
  });

  test('ignores the env var once the query cache exists', async () => {
    process.env.CUBEJS_SQL_INCLUDE_TRACE_ID = 'true';
    await run();
    expect(driver.executedQueries.join('\n')).not.toContain('trace_id');
  });

  test('is absent for background refreshes', async () => {
    await enable();
    await run({ requestId: `scheduler-${TRACE_ID}` });
    expect(driver.executedQueries.join('\n')).not.toContain('trace_id');
  });

  test('is absent when there is no request id', async () => {
    await enable();
    const query = 'SELECT 2';
    await run({ requestId: undefined, query });
    expect(driver.executedQueries).toContain(query);
    expect(driver.executedQueries.join('\n')).not.toContain('trace_id');
  });

  // The streaming path reaches the driver through createQueue's traceFn rather
  // than the query handler, so it needs its own coverage.
  test('tags the streaming path with the SQL, not something else', async () => {
    await enable();
    const query = 'SELECT 7 AS streamed';
    await orchestrator.fetchQuery({ query, values: [], requestId: TRACE_ID, persistent: true });

    expect(driver.streamedQueries).toHaveLength(1);
    expect(typeof driver.streamedQueries[0]).toBe('string');
    expect(driver.streamedQueries[0]).toBe(`${query}\n${EXPECTED_COMMENT}`);
  });

  test('streams plain SQL when the env var is unset', async () => {
    const query = 'SELECT 8 AS streamed';
    await orchestrator.fetchQuery({ query, values: [], requestId: TRACE_ID, persistent: true });

    expect(driver.streamedQueries).toEqual([query]);
  });

  // Refresh keys, pre-aggregation helpers and other supporting queries travel
  // the same path carrying the user's request id, but answer to no API request
  // of their own.
  test('is absent for supporting queries issued under a user request', async () => {
    await enable();
    await orchestrator.fetchQuery({
      query: SQL,
      values: [],
      requestId: TRACE_ID,
      cacheKeyQueries: { renewalThreshold: 120, queries: [['SELECT MAX(updated_at) FROM orders', []]] },
    });

    const refreshKeyQuery = driver.executedQueries
      .find(q => typeof q === 'string' && q.includes('MAX(updated_at)'));
    expect(refreshKeyQuery).toBe('SELECT MAX(updated_at) FROM orders');

    expect(driver.executedQueries).toContain(`${SQL}\n${EXPECTED_COMMENT}`);
  });

  // `cache=stale-while-revalidate` is client-settable and reaches the data source
  // through the background fetch call site, which needs `primaryQuery` of its own.
  test('is appended on the background fetch path', async () => {
    await enable();
    const query = 'SELECT 9 AS background';

    await orchestrator.fetchQuery({
      query,
      values: [],
      requestId: TRACE_ID,
      cacheMode: 'stale-while-revalidate',
      cacheKeyQueries: { renewalThreshold: 120, queries: [] },
    });

    expect(driver.executedQueries).toContain(`${query}\n${EXPECTED_COMMENT}`);
  });

  test('leaves the query untagged for a hostile request id', async () => {
    await enable();
    await run({ requestId: '*/ DROP TABLE users; /*' });

    expect(driver.executedQueries).toContain(SQL);
    expect(driver.executedQueries.some(q => typeof q === 'string' && q.includes('trace_id'))).toBe(false);
  });

  // `queryWithRetryAndRelease` is public and takes `query` as `any`, so the
  // tuple form is handled even though today's internal callers all unwrap it
  // upstream and pass the SQL on its own.
  describe('tuple-form queries', () => {
    const traceQuery = (req: Parameters<TestQueryCache['traceQuery']>[0]) => new TestQueryCache(
      'TRACE_TEST_TUPLE',
      async () => new TraceMockDriver() as any,
      jest.fn(),
      { cacheAndQueueDriver: 'memory' },
    ).traceQuery(req);

    test('tags the SQL and leaves the parameters alone', () => {
      process.env.CUBEJS_SQL_INCLUDE_TRACE_ID = 'true';

      expect(traceQuery({
        query: [SQL, [1, 'two']],
        requestId: TRACE_ID,
        primaryQuery: true,
      })).toEqual([`${SQL}\n${EXPECTED_COMMENT}`, [1, 'two']]);
    });

    test('is left untouched when the env var is unset', () => {
      const query = [SQL, [1]];

      expect(traceQuery({ query, requestId: TRACE_ID, primaryQuery: true })).toBe(query);
    });
  });
});

describe('SQL trace comment and the cache key', () => {
  afterEach(() => {
    delete process.env.CUBEJS_SQL_INCLUDE_TRACE_ID;
  });

  // The comment is attached at execution time, after the key is derived from
  // the untagged SQL. Were it attached earlier, every request would produce a
  // distinct key and defeat both result caching and in-flight coalescing.
  test('is unchanged by the feature', () => {
    const queryBody = { query: SQL, values: [] as string[], requestId: TRACE_ID };

    delete process.env.CUBEJS_SQL_INCLUDE_TRACE_ID;
    const withoutFlag = JSON.stringify(QueryCache.queryCacheKey(queryBody));

    process.env.CUBEJS_SQL_INCLUDE_TRACE_ID = 'true';
    const withFlag = JSON.stringify(QueryCache.queryCacheKey(queryBody));

    expect(withFlag).toBe(withoutFlag);
    expect(withFlag).not.toContain('trace_id');
  });

  test('is derived from the untagged SQL of the very query that gets tagged', async () => {
    process.env.CUBEJS_SQL_INCLUDE_TRACE_ID = 'true';

    const driver = new TraceMockDriver();
    const orchestrator = new QueryOrchestrator(
      'TRACE_TEST_CACHE_KEY',
      () => driver as any,
      () => undefined,
      {
        cacheAndQueueDriver: 'memory',
        queryCacheOptions: { queueOptions: () => ({ concurrency: 1 }) },
      },
    );

    const queryBody = { query: SQL, values: [] as string[], requestId: TRACE_ID };

    try {
      await orchestrator.fetchQuery(queryBody);
    } finally {
      await orchestrator.cleanup();
    }

    expect(driver.executedQueries).toContain(`${SQL}\n${EXPECTED_COMMENT}`);
    expect(JSON.stringify(QueryCache.queryCacheKey(queryBody))).not.toContain('trace_id');
  });
});
