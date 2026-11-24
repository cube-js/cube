import { testLogger } from '@cubejs-backend/shared';
import { ApiGateway } from '../src';
import { compilerApi, AdapterApiMock, DataSourceStorageMock } from './mocks';

const LAST_REFRESH_TIME = new Date('2024-01-01T00:00:00.000Z');

// Shape the orchestrator reports: identity plus the refresh key values, which
// must not reach a regular client.
const USED_PRE_AGGREGATIONS = {
  'schema.orders_main20240101': {
    preAggregationId: 'Orders.main',
    targetTableName: 'schema.orders_main20240101_abc_def_1712',
    lastUpdatedAt: 1712000000000,
    type: 'rollup',
    refreshKeyValues: [[{ max_updated_at: '2024-01-01T00:00:00.000Z' }]],
  },
};

class FreshnessAdapterApiMock extends AdapterApiMock {
  public lastRefreshTime: Date | undefined;

  public usedPreAggregations: Record<string, any> | undefined;

  public executedQueries: any[] = [];

  public constructor(lastRefreshTime?: Date, usedPreAggregations?: Record<string, any>) {
    super();
    this.lastRefreshTime = lastRefreshTime;
    this.usedPreAggregations = usedPreAggregations;
  }

  public async executeQuery(query: any) {
    this.executedQueries.push(query);
    return {
      data: [{ foo__bar: 42 }],
      lastRefreshTime: this.lastRefreshTime,
      // The orchestrator echoes the flag of the query it ran
      external: Boolean(query.external),
      usedPreAggregations: this.usedPreAggregations,
    };
  }
}

function createGateway(adapterApi: any) {
  return new ApiGateway('secret', compilerApi, async () => adapterApi, testLogger, {
    standalone: true,
    dataSourceStorage: new DataSourceStorageMock(),
    basePath: '/cubejs-api',
    refreshScheduler: {},
  });
}

async function sqlApiLoad(
  adapterApi: any,
  sqlQuery?: [string, any[]],
  signedWithPlaygroundAuthSecret = false,
  request: Record<string, any> = {},
) {
  const apiGateway = createGateway(adapterApi);

  let response: any;

  await apiGateway.sqlApiLoad({
    query: { measures: ['Foo.bar'] },
    sqlQuery,
    streaming: false,
    ...request,
    memberExpressions: true,
    context: {
      requestId: 'sql-api-load-test',
      securityContext: {},
      signedWithPlaygroundAuthSecret,
    } as any,
    res: (r: any) => {
      response = r;
    },
    apiType: 'sql',
  } as any);

  return response;
}

describe('sqlApiLoad freshness metadata', () => {
  // cubesql hands pre-generated SQL back to the gateway for any query it pushes
  // down (`sqlQuery` set). That branch used to build its result object without
  // `lastRefreshTime`, so the SQL API reported the result's age as unknown even
  // though the underlying query-cache entry had the timestamp.
  test('pushed-down sqlQuery result carries lastRefreshTime', async () => {
    const response = await sqlApiLoad(
      new FreshnessAdapterApiMock(LAST_REFRESH_TIME),
      ['SELECT * FROM test', []]
    );

    expect(response.results).toHaveLength(1);
    expect(response.results[0].lastRefreshTime).toBe(
      LAST_REFRESH_TIME.toISOString()
    );
    expect(response.results[0].external).toBe(false);
  });

  // No cache entry and no pre-aggregation: the orchestrator returns no
  // timestamp at all, so the gateway must not invent one. `JSON.stringify`
  // then drops the `undefined` and the Rust side decodes the missing key into
  // `None` (`V1LoadResult.last_refresh_time: Option<String>`).
  test('pushed-down sqlQuery result omits lastRefreshTime when absent', async () => {
    const response = await sqlApiLoad(
      new FreshnessAdapterApiMock(undefined),
      ['SELECT * FROM test', []]
    );

    expect(response.results).toHaveLength(1);
    expect(response.results[0].lastRefreshTime).toBeUndefined();
  });

  // Pre-aggregation identity lets a client join a chart's result to the build
  // it is waiting on. It has to travel with the pushed-down result too, since
  // that is the branch every cubesql query takes.
  test('pushed-down sqlQuery result carries usedPreAggregations', async () => {
    const response = await sqlApiLoad(
      new FreshnessAdapterApiMock(LAST_REFRESH_TIME, USED_PRE_AGGREGATIONS),
      ['SELECT * FROM test', []]
    );

    expect(response.results[0].usedPreAggregations).toEqual({
      'schema.orders_main20240101': {
        preAggregationId: 'Orders.main',
        lastUpdatedAt: 1712000000000,
        type: 'rollup',
      },
    });
  });

  // Refresh key values are rows of the refresh key queries, and a
  // `refreshKey.sql` is often not filtered by the security context the cube
  // itself applies. `targetTableName` names the physical table of one build,
  // hashes included. Both stay dev-mode only.
  test('usedPreAggregations omits refreshKeyValues and targetTableName', async () => {
    const response = await sqlApiLoad(
      new FreshnessAdapterApiMock(LAST_REFRESH_TIME, USED_PRE_AGGREGATIONS),
      ['SELECT * FROM test', []]
    );

    const usage = response.results[0].usedPreAggregations['schema.orders_main20240101'];
    expect(usage.refreshKeyValues).toBeUndefined();
    expect(usage.targetTableName).toBeUndefined();
  });

  // A query that hit no pre-aggregation reports nothing rather than `{}`.
  test('usedPreAggregations is absent when no pre-aggregation was used', async () => {
    const response = await sqlApiLoad(
      new FreshnessAdapterApiMock(LAST_REFRESH_TIME, {}),
      ['SELECT * FROM test', []]
    );

    expect(response.results[0].usedPreAggregations).toBeUndefined();
  });

  // Playground and dev mode get the object unredacted, but the same
  // nothing-to-report normalization: a client checking for the key must not see
  // it appear only because the deployment runs in dev mode.
  test('usedPreAggregations is absent under playground auth when empty', async () => {
    const response = await sqlApiLoad(
      new FreshnessAdapterApiMock(LAST_REFRESH_TIME, {}),
      undefined,
      true,
    );

    const result = response.getResults()[0].getRootResultObject()[0];

    expect(result.usedPreAggregations).toBeUndefined();
  });

  test('usedPreAggregations keeps the full object under playground auth', async () => {
    const response = await sqlApiLoad(
      new FreshnessAdapterApiMock(LAST_REFRESH_TIME, USED_PRE_AGGREGATIONS),
      undefined,
      true,
    );

    const result = response.getResults()[0].getRootResultObject()[0];

    expect(result.usedPreAggregations['schema.orders_main20240101'])
      .toEqual(USED_PRE_AGGREGATIONS['schema.orders_main20240101']);
  });

  // Deliberate asymmetry with the branch above: the pushed-down result keeps
  // the redacted projection even under Playground auth, because that branch
  // serves the SQL API and carries no dev-only fields at all.
  test('pushed-down sqlQuery result stays redacted under playground auth', async () => {
    const response = await sqlApiLoad(
      new FreshnessAdapterApiMock(LAST_REFRESH_TIME, USED_PRE_AGGREGATIONS),
      ['SELECT * FROM test', []],
      true,
    );

    const usage = response.results[0].usedPreAggregations['schema.orders_main20240101'];
    expect(usage.targetTableName).toBeUndefined();
    expect(usage.refreshKeyValues).toBeUndefined();
  });

  // The non-pushdown branch goes through `prepareResultTransformData`, which is
  // also what the REST `/load` response is built from.
  test('regular query result carries usedPreAggregations', async () => {
    const response = await sqlApiLoad(
      new FreshnessAdapterApiMock(LAST_REFRESH_TIME, USED_PRE_AGGREGATIONS)
    );

    const result = response.getResults()[0].getRootResultObject()[0];

    expect(result.usedPreAggregations['schema.orders_main20240101'].preAggregationId)
      .toBe('Orders.main');
    expect(result.usedPreAggregations['schema.orders_main20240101'].refreshKeyValues)
      .toBeUndefined();
    expect(result.usedPreAggregations['schema.orders_main20240101'].targetTableName)
      .toBeUndefined();
  });
});

describe('sqlApiLoad over Cube Store pre-aggregations', () => {
  const PRE_AGGREGATION = {
    tableName: 'dev_pre_aggregations.orders_main',
    external: true,
  };

  // cubesql rendered the pushed-down SQL for Cube Store over the rollups it was
  // told about, so the query has to run there with those rollups built and
  // their table names swapped in
  test('external sqlQuery runs on Cube Store with its pre-aggregations', async () => {
    const adapterApi = new FreshnessAdapterApiMock();
    const response = await sqlApiLoad(
      adapterApi,
      ['SELECT * FROM dev_pre_aggregations.orders_main', []],
      false,
      { external: true, preAggregations: [PRE_AGGREGATION, PRE_AGGREGATION] },
    );

    expect(adapterApi.executedQueries).toHaveLength(1);
    const [executed] = adapterApi.executedQueries;
    expect(executed.query).toBe('SELECT * FROM dev_pre_aggregations.orders_main');
    expect(executed.external).toBe(true);
    // Merged by table: the same rollup reported twice is built once
    expect(executed.preAggregations).toEqual([PRE_AGGREGATION]);

    expect(response.results[0].external).toBe(true);
  });

  test('sqlQuery without external runs on the data source', async () => {
    const adapterApi = new FreshnessAdapterApiMock();
    await sqlApiLoad(
      adapterApi,
      ['SELECT 1', []],
      false,
      { preAggregations: [PRE_AGGREGATION] },
    );

    const [executed] = adapterApi.executedQueries;
    expect(executed.external).toBeFalsy();
    expect(executed.preAggregations).toBeUndefined();
  });
});

describe('Cube Store pre-aggregations in stream mode', () => {
  afterEach(() => {
    delete process.env.CUBESQL_STREAM_MODE;
    delete process.env.CUBESQL_STREAM_MODE_PRE_AGGREGATIONS;
  });

  class StreamAdapterApiMock extends FreshnessAdapterApiMock {
    public streamedQueries: any[] = [];

    public async streamQuery(query: any) {
      this.streamedQueries.push(query);
      return { pipe: () => undefined };
    }
  }

  function createStreamGateway(compilerApiMock: any, adapterApi: any = new StreamAdapterApiMock()) {
    return new ApiGateway('secret', jest.fn().mockImplementation(async () => ({
      ...(await compilerApi()),
      ...compilerApiMock,
    })), async () => adapterApi, testLogger, {
      standalone: true,
      dataSourceStorage: new DataSourceStorageMock(),
      basePath: '/cubejs-api',
      refreshScheduler: {},
    });
  }

  async function load(streaming: boolean, sqlQuery?: [string, any[]]) {
    const getSql = jest.fn(async (query: any) => ({
      sql: ['SELECT * FROM test', []],
      external: !query.disableExternalPreAggregations,
      aliasNameToMember: { foo__bar: 'Foo.bar' },
      order: [],
      dataSource: 'default',
    }));
    const adapterApi = new StreamAdapterApiMock();
    await createStreamGateway({ getSql }, adapterApi).sqlApiLoad({
      query: { measures: ['Foo.bar'] },
      sqlQuery,
      streaming,
      memberExpressions: true,
      context: { requestId: 'sql-api-stream-test', securityContext: {} } as any,
      res: () => undefined,
      apiType: 'sql',
    } as any);

    return { disabled: getSql.mock.calls[0][0].disableExternalPreAggregations, adapterApi };
  }

  async function sqlGenerators() {
    const externalPreAggregationMembers = jest.fn(async () => ['Foo.bar']);
    let response: any;
    await createStreamGateway({
      memberToDataSource: async () => ({ 'Foo.bar': 'default' }),
      getCompilers: async () => ({}),
      createQueryByDataSource: async () => ({
        externalQueryClass: class {},
        externalQuery: () => 'cube-store-generator',
      }),
      externalPreAggregationMembers,
    }).sqlGenerators({
      context: { requestId: 'sql-generators-test', securityContext: {} } as any,
      res: (r: any) => {
        response = r;
      },
    });
    return response;
  }

  test('streamed query reads the data source', async () => {
    process.env.CUBESQL_STREAM_MODE = 'true';

    for (const sqlQuery of [undefined, ['SELECT 1', []] as [string, any[]]]) {
      const { disabled, adapterApi } = await load(true, sqlQuery);

      expect(disabled).toBe(true);
      expect(adapterApi.streamedQueries).toHaveLength(1);
      expect(adapterApi.streamedQueries[0].external).toBe(false);
    }
  });

  test('non-streamed query reads the data source in stream mode without pre-aggregations', async () => {
    process.env.CUBESQL_STREAM_MODE = 'true';
    process.env.CUBESQL_STREAM_MODE_PRE_AGGREGATIONS = 'false';

    const { disabled, adapterApi } = await load(false);

    expect(disabled).toBe(true);
    expect(adapterApi.executedQueries[0].external).toBe(false);
  });

  test('non-streamed query reads Cube Store in stream mode by default', async () => {
    process.env.CUBESQL_STREAM_MODE = 'true';

    const { disabled, adapterApi } = await load(false);

    expect(disabled).toBe(false);
    expect(adapterApi.executedQueries[0].external).toBe(true);
  });

  test('non-streamed query reads Cube Store outside stream mode', async () => {
    const { disabled, adapterApi } = await load(false);

    expect(disabled).toBe(false);
    expect(adapterApi.executedQueries[0].external).toBe(true);
  });

  // Without a Cube Store SQL generator cubesql plans no SQL over Cube Store rollups
  test('sqlGenerators leaves out Cube Store in stream mode without pre-aggregations', async () => {
    expect((await sqlGenerators()).externalSqlGenerator).toEqual('cube-store-generator');

    process.env.CUBESQL_STREAM_MODE = 'true';
    expect((await sqlGenerators()).externalSqlGenerator).toEqual('cube-store-generator');

    process.env.CUBESQL_STREAM_MODE_PRE_AGGREGATIONS = 'false';
    expect(await sqlGenerators()).toMatchObject({
      externalSqlGenerator: undefined,
      externalPreAggregationMembers: [],
    });
  });
});

describe('preAggregationMatch', () => {
  // cubesql asks which cube queries a Cube Store rollup would serve before it plans
  // SQL over them; the answer is `external` of a match-only compile of each query
  async function preAggregationMatch(getSql: (query: any, options: any) => Promise<any>, queries: any[]) {
    const matchCompilerApi = jest.fn().mockImplementation(async () => ({
      ...(await compilerApi()),
      getSql,
    }));
    const apiGateway = new ApiGateway('secret', matchCompilerApi, async () => new AdapterApiMock(), testLogger, {
      standalone: true,
      dataSourceStorage: new DataSourceStorageMock(),
      basePath: '/cubejs-api',
      refreshScheduler: {},
    });

    let response: any;
    await apiGateway.preAggregationMatch({
      queries,
      context: {
        requestId: 'pre-aggregation-match-test',
        securityContext: {},
      } as any,
      res: (r: any) => {
        response = r;
      },
    });

    return response;
  }

  test('answers per query from a match-only compile with externals allowed', async () => {
    const getSql = jest.fn(async (query: any, options: any) => ({
      external: query.measures.includes('Foo.bar'),
      sql: options?.preAggregationsOnly ? null : ['SELECT 1', []],
      preAggregations: [],
      dataSource: 'default',
      order: [],
    }));

    const response = await preAggregationMatch(getSql, [
      { measures: ['Foo.bar'] },
      { measures: ['Foo.baz'] },
    ]);

    expect(response).toEqual({ matches: [true, false] });
    expect(getSql).toHaveBeenCalledTimes(2);

    for (const [query, options] of getSql.mock.calls) {
      expect(query.disableExternalPreAggregations).toBe(false);
      expect(options).toEqual({ preAggregationsOnly: true });
    }
  });
});
