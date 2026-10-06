import { randomUUID } from 'crypto';
import { Readable } from 'stream';
import { QueryBody } from '@cubejs-backend/query-orchestrator';

import { CubejsServerCore } from '../../src';
import { OrchestratorApi } from '../../src/core/OrchestratorApi';
import { QueryTagsFn } from '../../src/core/types';

describe('OrchestratorApi', () => {
  // https://github.com/cube-js/cube/issues/11313
  test('getPreAggregationQueueStates forwards dataSource to the orchestrator', async () => {
    const api = Object.create(OrchestratorApi.prototype);
    const getPreAggregationQueueStates = jest.fn(async () => []);
    api.orchestrator = { getPreAggregationQueueStates };

    await api.getPreAggregationQueueStates('test_ds');
    expect(getPreAggregationQueueStates).toHaveBeenLastCalledWith('test_ds');

    // QueryOrchestrator#getPreAggregationQueueStates defaults an undefined
    // dataSource to 'default', so calls without one keep working.
    await api.getPreAggregationQueueStates();
    expect(getPreAggregationQueueStates).toHaveBeenLastCalledWith(undefined);
  });
});

describe('OrchestratorApi queryTags', () => {
  const apis: OrchestratorApi[] = [];

  /**
   * A real orchestrator on the in-memory cache and queue, so the tags travel the same path to the
   * driver as in production. The memory cache is shared by the process, hence a prefix per test.
   */
  function createApi(queryTags?: QueryTagsFn) {
    const driver = {
      query: jest.fn(async () => [{ value: 1 }]),
      stream: jest.fn(async () => ({ rowStream: Readable.from([{ value: 1 }]) })),
      release: async () => undefined,
    };

    const api = new OrchestratorApi(async () => driver as any, () => undefined, {
      cacheAndQueueDriver: 'memory',
      contextToDbType: async () => 'bigquery',
      contextToExternalDbType: () => 'cubestore',
      queryCacheOptions: { queueOptions: async () => ({ concurrency: 1 }) },
      redisPrefix: randomUUID(),
      queryTags,
    });
    apis.push(api);

    return { api, driver };
  }

  afterEach(async () => {
    await Promise.all(apis.splice(0).map(api => api.release()));
  });

  const userQuery = (sub: string, extra: Partial<QueryBody> = {}): QueryBody => ({
    query: 'SELECT 1',
    values: [],
    requestId: `request-${sub}`,
    context: { securityContext: { sub }, requestId: `request-${sub}` },
    ...extra,
  });

  const tagUser: QueryTagsFn = ({ securityContext }) => ({ user_id: securityContext.sub });

  test.each([undefined, 'stale-while-revalidate', 'must-revalidate'] as const)(
    'labels the data source query with the tags of the requesting user (cacheMode: %s)',
    async (cacheMode) => {
      const queryTags = jest.fn(tagUser);
      const { api, driver } = createApi(queryTags);

      await api.executeQuery(userQuery('alice', { cacheMode }));

      expect(queryTags).toHaveBeenCalledWith({
        securityContext: { sub: 'alice' },
        requestId: 'request-alice',
        dataSource: 'default',
      });
      expect(driver.query).toHaveBeenCalledWith('SELECT 1', [], expect.objectContaining({
        requestId: 'request-alice',
        queryTags: { user_id: 'alice' },
      }));
    }
  );

  test('stringifies tag values and drops missing ones', async () => {
    // A JS or Python hook can return anything the security context holds
    const { api, driver } = createApi(() => ({ user_id: 42, org: undefined, team: null }));

    await api.executeQuery(userQuery('alice'));

    expect(driver.query).toHaveBeenCalledWith('SELECT 1', [], expect.objectContaining({
      queryTags: { user_id: '42' },
    }));
  });

  test('labels a streamed query', async () => {
    const { api, driver } = createApi(tagUser);

    const stream = await api.streamQuery(userQuery('alice', { persistent: true }));
    await (stream as unknown as Readable).toArray();

    expect(driver.stream).toHaveBeenCalledWith('SELECT 1', [], expect.objectContaining({
      requestId: 'request-alice',
      queryTags: { user_id: 'alice' },
    }));
  });

  test('keeps the tags out of the cache key, so users share cached results', async () => {
    const { api, driver } = createApi(tagUser);

    await api.executeQuery(userQuery('alice'));
    await api.executeQuery(userQuery('bob'));

    expect(driver.query).toHaveBeenCalledTimes(1);
  });

  test('leaves queries without a request context, like scheduled refresh, untagged', async () => {
    const queryTags = jest.fn(tagUser);
    const { api, driver } = createApi(queryTags);

    await api.executeQuery({ query: 'SELECT 1', values: [], requestId: 'scheduler' });

    expect(queryTags).not.toHaveBeenCalled();
    expect(driver.query).toHaveBeenCalledWith('SELECT 1', [], expect.not.objectContaining({
      queryTags: expect.anything(),
    }));
  });

  test('skips queries that run in Cube Store, which ignores tags', async () => {
    const queryTags = jest.fn(tagUser);
    const { api } = createApi(queryTags);
    jest.spyOn(api.getQueryOrchestrator(), 'fetchQuery').mockResolvedValue({ data: [] });

    await api.executeQuery(userQuery('alice', { external: true }));

    expect(queryTags).not.toHaveBeenCalled();
  });

  test('CubejsServerCore hands the queryTags option to its orchestrator api', async () => {
    const core = new CubejsServerCore(<any>{
      apiSecret: 'secret',
      driverFactory: () => ({ type: 'bigquery' }),
      cacheAndQueueDriver: 'memory',
      queryTags: tagUser,
    });

    try {
      const context = { securityContext: { sub: 'alice' }, requestId: 'request-alice' };
      const api = await core.getOrchestratorApi(context);
      const fetchQuery = jest.spyOn(api.getQueryOrchestrator(), 'fetchQuery').mockResolvedValue({ data: [] });

      await api.executeQuery(userQuery('alice'));

      expect(fetchQuery).toHaveBeenCalledWith(expect.objectContaining({ queryTags: { user_id: 'alice' } }));
    } finally {
      await core.releaseConnections();
    }
  });
});
