import { CubejsServerCore } from '../../src';
import { OrchestratorApi, OrchestratorApiOptions } from '../../src/core/OrchestratorApi';
import type { DriverFactoryByDataSource } from '@cubejs-backend/query-orchestrator';
import type { RequestContext } from '../../src/core/types';

class TestServerCore extends CubejsServerCore {
  public readonly builtOptions: OrchestratorApiOptions[] = [];

  protected createOrchestratorApi(getDriver: DriverFactoryByDataSource, options: OrchestratorApiOptions): OrchestratorApi {
    this.builtOptions.push(options);

    return super.createOrchestratorApi(getDriver, options);
  }
}

const context = {
  requestId: 'request',
  authInfo: null,
  securityContext: { tenant: 'tenant-1' },
} as any;

describe('contextToPreAggregationOrchestratorId', () => {
  const cores: TestServerCore[] = [];
  const env = process.env.CUBEJS_PRE_AGGREGATIONS_ORCHESTRATOR_ID_FROM_ROUTER;

  function createServerCore(options: Record<string, unknown> = {}) {
    const core = new TestServerCore(<any>{
      apiSecret: 'secret',
      driverFactory: () => <any>({ type: 'postgres' }),
      contextToOrchestratorId: ({ securityContext }: RequestContext) => `ORCH_${securityContext.tenant}`,
      contextToCubeStoreRouterId: () => 'ROUTER_1',
      ...options,
    });

    cores.push(core);

    return core;
  }

  async function preAggregationsRedisPrefix(core: TestServerCore) {
    await core.getOrchestratorApi(context);

    return core.builtOptions[0].preAggregationsRedisPrefix;
  }

  afterEach(async () => {
    await Promise.all(cores.splice(0).map(core => core.releaseConnections()));

    if (env === undefined) {
      delete process.env.CUBEJS_PRE_AGGREGATIONS_ORCHESTRATOR_ID_FROM_ROUTER;
    } else {
      process.env.CUBEJS_PRE_AGGREGATIONS_ORCHESTRATOR_ID_FROM_ROUTER = env;
    }
  });

  test('defaults to the orchestrator id', async () => {
    delete process.env.CUBEJS_PRE_AGGREGATIONS_ORCHESTRATOR_ID_FROM_ROUTER;

    const core = createServerCore();

    expect(await preAggregationsRedisPrefix(core)).toBeUndefined();
    expect(core.builtOptions[0].redisPrefix).toEqual('ORCH_tenant-1');
  });

  test('uses the Cube Store router id when the env flag is set', async () => {
    process.env.CUBEJS_PRE_AGGREGATIONS_ORCHESTRATOR_ID_FROM_ROUTER = 'true';

    const core = createServerCore();

    expect(await preAggregationsRedisPrefix(core)).toEqual('ROUTER_1');
    expect(core.builtOptions[0].redisPrefix).toEqual('ORCH_tenant-1');
  });

  test('falls back to the orchestrator id when the env flag is set without a router', async () => {
    process.env.CUBEJS_PRE_AGGREGATIONS_ORCHESTRATOR_ID_FROM_ROUTER = 'true';

    const core = createServerCore({ contextToCubeStoreRouterId: undefined });

    expect(await preAggregationsRedisPrefix(core)).toBeUndefined();
  });

  test('a configured id equal to the orchestrator id is still passed on', async () => {
    delete process.env.CUBEJS_PRE_AGGREGATIONS_ORCHESTRATOR_ID_FROM_ROUTER;

    const core = createServerCore({
      contextToPreAggregationOrchestratorId: ({ securityContext }: RequestContext) => `ORCH_${securityContext.tenant}`,
    });

    expect(await preAggregationsRedisPrefix(core)).toEqual('ORCH_tenant-1');
  });

  test('an explicit option wins over the env flag', async () => {
    process.env.CUBEJS_PRE_AGGREGATIONS_ORCHESTRATOR_ID_FROM_ROUTER = 'true';

    const core = createServerCore({
      contextToPreAggregationOrchestratorId: ({ securityContext }: RequestContext) => `PRE_AGG_${securityContext.tenant}`,
    });

    expect(await preAggregationsRedisPrefix(core)).toEqual('PRE_AGG_tenant-1');
  });
});
