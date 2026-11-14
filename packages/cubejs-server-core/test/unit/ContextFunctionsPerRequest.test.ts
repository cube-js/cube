import { BaseDriver } from '@cubejs-backend/base-driver';
import type { ApiGateway } from '@cubejs-backend/api-gateway';
import { CubejsServerCore } from '../../src/core/server';
import type { RequestContext } from '../../src/core/types';

class MockDriver extends BaseDriver {
  public async testConnection(): Promise<void> {
    // Nothing to connect to
  }

  public async query<R = unknown>(sql: string): Promise<R[]> {
    return (sql.includes('total_count') ? [{ total_count: '0' }] : []) as R[];
  }
}

class Core extends CubejsServerCore {
  public gateway(): ApiGateway {
    return this.apiGateway();
  }
}

const cube = (name: string) => `
  - name: ${name}
    sql: SELECT * FROM ${name}
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: status
        sql: status
        type: string
    measures:
      - name: count
        type: count
    access_policy:
      - group: basic
        member_level:
          includes: "*"
`;

const model = `cubes:${['orders', 'users', 'products', 'lines'].map(cube).join('')}`;

describe('context functions are called at most once per request', () => {
  const hooks = {
    contextToAppId: jest.fn(({ securityContext }: RequestContext) => `app_${securityContext.tenant}`),
    contextToOrchestratorId: jest.fn(({ securityContext }: RequestContext) => `orchestrator_${securityContext.tenant}`),
    contextToGroups: jest.fn(({ securityContext }: RequestContext) => securityContext.groups),
    contextToApiScopes: jest.fn(async () => ['data', 'meta', 'sql', 'graphql'] as any),
    driverFactory: jest.fn(() => ({ type: 'postgres' as const })),
  };

  let core: Core;
  let gateway: ApiGateway;

  beforeAll(() => {
    jest.spyOn(CubejsServerCore.prototype, 'resolveDriver').mockImplementation(async () => new MockDriver());

    core = new Core(<any>{
      apiSecret: 'secret',
      scheduledRefreshTimer: false,
      cacheAndQueueDriver: 'memory',
      logger: () => {}, // eslint-disable-line @typescript-eslint/no-empty-function
      repositoryFactory: () => ({
        localPath: () => '/mock/path',
        dataSchemaFiles: () => Promise.resolve([{ fileName: 'cubes.yml', content: model }]),
      }),
      ...hooks,
    });
    gateway = core.gateway();
  });

  afterAll(async () => {
    jest.restoreAllMocks();
    await core.shutdown();
  });

  let requestNumber = 0;

  // A fresh context per request, the way the gateway builds one from each HTTP request.
  const request = async (call: (context: RequestContext, res: (r: any) => void) => Promise<unknown>) => {
    requestNumber += 1;
    const context: RequestContext = {
      securityContext: { tenant: 'acme', groups: ['basic'] },
      requestId: `request-${requestNumber}`,
    };

    let response: any;
    await call(context, (r) => {
      response = r;
    });

    if (response?.error) {
      throw new Error(response.error);
    }

    return response;
  };

  const load = (context: RequestContext, res: (r: any) => void) => gateway.load({
    // `total` makes the gateway plan and run a second, count query
    query: { measures: ['orders.count'], dimensions: ['orders.status'], limit: 10, total: true },
    context,
    res,
  } as any);

  const sql = (context: RequestContext, res: (r: any) => void) => gateway.sql({
    query: { measures: ['users.count'] },
    context,
    res,
  } as any);

  const meta = (context: RequestContext, res: (r: any) => void) => gateway.meta({ context, res });

  const expectAtMostOncePerRequest = () => {
    const calledMoreThanOnce = Object.entries(hooks)
      .map(([name, hook]): [string, number] => [name, hook.mock.calls.length])
      .filter(([, calls]) => calls > 1);
    expect(calledMoreThanOnce).toEqual([]);
  };

  test.each([
    ['load', load],
    ['sql', sql],
    ['meta', meta],
  ])('%s', async (_name, call) => {
    // The first request also builds the compiler and the orchestrator
    await request(call);

    Object.values(hooks).forEach(hook => hook.mockClear());
    await request(call);

    expectAtMostOncePerRequest();
  });
});
