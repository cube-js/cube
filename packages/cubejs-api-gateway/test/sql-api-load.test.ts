import { ApiGateway } from '../src';
import { compilerApi, AdapterApiMock, DataSourceStorageMock } from './mocks';

const logger = (type: any, message: any) => console.log({ type, ...message });

const LAST_REFRESH_TIME = new Date('2024-01-01T00:00:00.000Z');

class FreshnessAdapterApiMock extends AdapterApiMock {
  public lastRefreshTime: Date | undefined;

  public constructor(lastRefreshTime?: Date) {
    super();
    this.lastRefreshTime = lastRefreshTime;
  }

  public async executeQuery(_query: any) {
    return {
      data: [{ foo__bar: 42 }],
      lastRefreshTime: this.lastRefreshTime,
      // Always falsy for a pushdown query — see the note in
      // `sqlApiLoad`. Mirrors what the orchestrator actually echoes back.
      external: false,
    };
  }
}

function createGateway(adapterApi: any) {
  return new ApiGateway('secret', compilerApi, async () => adapterApi, logger, {
    standalone: true,
    dataSourceStorage: new DataSourceStorageMock(),
    basePath: '/cubejs-api',
    refreshScheduler: {},
  });
}

async function sqlApiLoad(adapterApi: any, sqlQuery?: [string, any[]]) {
  const apiGateway = createGateway(adapterApi);

  let response: any;

  await apiGateway.sqlApiLoad({
    query: { measures: ['Foo.bar'] },
    sqlQuery,
    streaming: false,
    memberExpressions: true,
    context: {
      requestId: 'sql-api-load-test',
      securityContext: {},
      signedWithPlaygroundAuthSecret: false,
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
});
