/* eslint-disable @typescript-eslint/no-empty-function */
import { BaseDriver } from '@cubejs-backend/query-orchestrator';
import type { DriverFactoryByDataSource } from '@cubejs-backend/query-orchestrator';

import { CreateOptions, CubejsServerCore } from '../../src';

type FakeDriver = BaseDriver & {
  builtFrom: any;
  release: jest.Mock;
  testConnection: jest.Mock;
};

/**
 * Stands in for real driver construction so these tests exercise the caching
 * decisions without needing a database. Everything else — the driver factory
 * closure, the fingerprinting, the rebuild — is the production code path.
 */
class TestServerCore extends CubejsServerCore {
  public builtDrivers: FakeDriver[] = [];

  /** Set to fail the next driver construction, as a bad credential would. */
  public failNextBuild = false;

  protected async createDriverFromFactoryResult(
    val: any,
    context: any,
    options?: any,
  ): Promise<BaseDriver> {
    // A factory that hands back a constructed driver takes the real path — that
    // branch is exactly what one of these tests is about.
    if (val instanceof BaseDriver) {
      return super.createDriverFromFactoryResult(val, context, options);
    }

    if (this.failNextBuild) {
      this.failNextBuild = false;

      throw new Error('driver construction failed');
    }

    const driver = {
      builtFrom: val,
      release: jest.fn(async () => {}),
      testConnection: jest.fn(async () => {}),
      setLogger: () => {},
    } as unknown as FakeDriver;

    this.builtDrivers.push(driver);

    return driver;
  }
}

/**
 * Boot a core, resolve its orchestrator once, and hand back the driver factory
 * the orchestrator was created with — the same closure the query orchestrator
 * calls for every query.
 */
async function createCore(options: CreateOptions, securityContext: unknown) {
  const core = new TestServerCore(<any>{
    contextToOrchestratorId: () => 'ORCHESTRATOR',
    ...options,
  });
  const spy = jest.spyOn(<any>core, 'createOrchestratorApi');

  await core.getOrchestratorApi(<any>{ requestId: 'req-1', securityContext });

  const driverFactory = <DriverFactoryByDataSource>spy.mock.calls[0][0];

  return {
    core,
    driverFactory,
    /** Serve another request through the cached orchestrator. */
    request: (nextSecurityContext: unknown, requestId = 'req-n') => core.getOrchestratorApi(<any>{ requestId, securityContext: nextSecurityContext }),
  };
}

describe('driver cache invalidation', () => {
  beforeAll(() => {
    process.env.CUBEJS_API_SECRET = 'api-secret';
  });

  // The CUB-3599 regression: the orchestrator closed over the context of the
  // request that created it, so a driver built from a per-user credential was
  // reused for the life of the process. Every connection it opened after the
  // token rotated failed to authenticate.
  test('rebuilds the driver when a context-derived credential changes', async () => {
    const { core, driverFactory, request } = await createCore({
      driverFactory: (ctx: any) => (<any>{ type: 'postgres', password: ctx.securityContext.token }),
    }, { token: 'token-a' });

    const first = <FakeDriver> await driverFactory('default');
    expect(first.builtFrom).toMatchObject({ password: 'token-a' });

    await request({ token: 'token-b' });
    const second = <FakeDriver> await driverFactory('default');

    expect(second).not.toBe(first);
    expect(second.builtFrom).toMatchObject({ password: 'token-b' });
    expect(core.builtDrivers).toHaveLength(2);
  });

  test('releases the driver it replaced, so its pool is drained', async () => {
    const { driverFactory, request } = await createCore({
      driverFactory: (ctx: any) => (<any>{ type: 'postgres', password: ctx.securityContext.token }),
    }, { token: 'token-a' });

    const first = <FakeDriver> await driverFactory('default');

    await request({ token: 'token-b' });
    await driverFactory('default');

    // Released off the request path, so give the detached promise a tick.
    await new Promise((resolve) => setImmediate(resolve));

    expect(first.release).toHaveBeenCalledTimes(1);
  });

  test('reuses the driver when the security context is unchanged', async () => {
    const factory = jest.fn((ctx: any) => (<any>{ type: 'postgres', password: ctx.securityContext.token }));
    const { driverFactory, request } = await createCore({ driverFactory: factory }, { token: 'token-a' });

    const first = await driverFactory('default');

    // A different request, same user, same credential.
    await request({ token: 'token-a' }, 'req-2');

    expect(await driverFactory('default')).toBe(first);
    // Not re-invoked: an unchanged security context short-circuits before the
    // user's factory is called at all.
    expect(factory).toHaveBeenCalledTimes(1);
  });

  test('reuses the driver when the factory ignores the security context', async () => {
    const factory = jest.fn(() => (<any>{ type: 'postgres', password: 'from-env' }));
    const { core, driverFactory, request } = await createCore({ driverFactory: factory }, { user: 'a' });

    const first = await driverFactory('default');

    await request({ user: 'b' });
    const second = await driverFactory('default');

    expect(second).toBe(first);
    expect(core.builtDrivers).toHaveLength(1);
    // Asked once more because the context changed, but the answer matched, so
    // nothing was rebuilt.
    expect(factory).toHaveBeenCalledTimes(2);
    // ...and that answer is remembered, so a third request with the same
    // context does not ask again.
    await request({ user: 'b' }, 'req-3');
    await driverFactory('default');
    expect(factory).toHaveBeenCalledTimes(2);
  });

  test('never rebuilds when the factory returns a constructed driver', async () => {
    class ConstructedDriver extends BaseDriver {
      public release = jest.fn(async () => {});

      public testConnection = jest.fn(async () => {});

      public async query<R = unknown>(): Promise<R[]> {
        return [];
      }
    }

    const driver = new ConstructedDriver();
    const factory = jest.fn(() => <any>driver);
    const { driverFactory, request } = await createCore({ driverFactory: factory }, { token: 'token-a' });

    const first = await driverFactory('default');

    await request({ token: 'token-b' });

    // A constructed driver carries no configuration to compare, so the previous
    // resolve-once behaviour is preserved rather than guessed at.
    expect(await driverFactory('default')).toBe(first);
    expect(driver.release).not.toHaveBeenCalled();
    expect(factory).toHaveBeenCalledTimes(1);
  });

  test('keeps the pre-aggregation alias pointing at the rebuilt driver', async () => {
    const { driverFactory, request } = await createCore({
      driverFactory: (ctx: any) => (<any>{ type: 'postgres', password: ctx.securityContext.token }),
    }, { token: 'token-a' });

    await driverFactory('default');
    await request({ token: 'token-b' });

    const rebuilt = <FakeDriver> await driverFactory('default');
    const preAgg = <FakeDriver> await driverFactory('default', true);

    expect(preAgg).toBe(rebuilt);
    expect(preAgg.builtFrom).toMatchObject({ password: 'token-b' });
  });

  // The `default` and `default@pre_agg` keys share one driver when the data
  // source has no separate pre-aggregation credentials. A pre-aggregation build
  // can be the first caller to observe a rotation, so invalidation has to clear
  // both keys whichever one asked: clearing only the requested key left the
  // other serving the drained driver, released it twice, and then built a
  // second pool for what should be a single shared driver.
  test('rebuilds once when a pre-aggregation build observes the rotation first', async () => {
    const { core, driverFactory, request } = await createCore({
      driverFactory: (ctx: any) => (<any>{ type: 'postgres', password: ctx.securityContext.token }),
    }, { token: 'token-a' });

    const first = <FakeDriver> await driverFactory('default');

    await request({ token: 'token-b' });

    const preAgg = <FakeDriver> await driverFactory('default', true);
    const regular = <FakeDriver> await driverFactory('default');

    await new Promise((resolve) => setImmediate(resolve));

    expect(preAgg).toBe(regular);
    expect(regular.builtFrom).toMatchObject({ password: 'token-b' });
    expect(core.builtDrivers).toHaveLength(2);
    // Exactly once — a second release would run against an already-drained pool.
    expect(first.release).toHaveBeenCalledTimes(1);
  });

  test('a failed rebuild does not leave a poisoned cache entry', async () => {
    const { core, driverFactory, request } = await createCore({
      driverFactory: (ctx: any) => (<any>{ type: 'postgres', password: ctx.securityContext.token }),
    }, { token: 'token-a' });

    await driverFactory('default');

    // The rotation is detected, but building the replacement fails.
    core.failNextBuild = true;
    await request({ token: 'token-b' });
    await expect(driverFactory('default')).rejects.toThrow('driver construction failed');

    // The next attempt resolves from scratch rather than serving the failure.
    const recovered = <FakeDriver> await driverFactory('default');
    expect(recovered.builtFrom).toMatchObject({ password: 'token-b' });
  });

  // The staleness check calls the factory speculatively, on a path that used to
  // be a pure cache hit. A factory that reads a secret store can fail
  // transiently, and that must not fail a query the cached driver can serve.
  test('reuses the cached driver when the staleness probe throws', async () => {
    let shouldFail = false;
    const { core, driverFactory, request } = await createCore({
      driverFactory: (ctx: any) => {
        if (shouldFail) {
          throw new Error('secret store unreachable');
        }

        return <any>{ type: 'postgres', password: ctx.securityContext.token };
      },
    }, { token: 'token-a' });

    const first = await driverFactory('default');

    shouldFail = true;
    await request({ token: 'token-b' });

    expect(await driverFactory('default')).toBe(first);
    expect(core.builtDrivers).toHaveLength(1);

    // Once the factory recovers, the rotation is picked up as usual.
    shouldFail = false;
    const rebuilt = <FakeDriver> await driverFactory('default');
    expect(rebuilt).not.toBe(first);
    expect(rebuilt.builtFrom).toMatchObject({ password: 'token-b' });
  });

  // Two queries in flight when a rotation lands both see the cached driver as
  // stale. Only one may rebuild: the loser must not release the driver the
  // winner has already handed to its caller, nor stand up a second pool.
  test('concurrent callers rebuild once and release once', async () => {
    const { core, driverFactory, request } = await createCore({
      driverFactory: (ctx: any) => (<any>{ type: 'postgres', password: ctx.securityContext.token }),
    }, { token: 'token-a' });

    const first = <FakeDriver> await driverFactory('default');

    await request({ token: 'token-b' });

    const [a, b] = <FakeDriver[]> await Promise.all([
      driverFactory('default'),
      driverFactory('default'),
    ]);

    await new Promise((resolve) => setImmediate(resolve));

    expect(a).toBe(b);
    expect(a.builtFrom).toMatchObject({ password: 'token-b' });
    expect(core.builtDrivers).toHaveLength(2);
    expect(first.release).toHaveBeenCalledTimes(1);
    // The driver handed back is usable — not one whose pool is being drained.
    expect(a.release).not.toHaveBeenCalled();
  });

  test('reuses the driver when the security context cannot be fingerprinted', async () => {
    const circular: any = { token: 'token-a' };
    circular.self = circular;

    const factory = jest.fn((ctx: any) => (<any>{ type: 'postgres', password: ctx.securityContext.token }));
    const { core, driverFactory, request } = await createCore({ driverFactory: factory }, { token: 'token-a' });

    const first = await driverFactory('default');

    // A circular security context fingerprints as null, which every caller must
    // read as "assume unchanged" rather than rebuilding blindly.
    await request(circular);

    expect(await driverFactory('default')).toBe(first);
    expect(core.builtDrivers).toHaveLength(1);
  });

  // The refresh scheduler's default context carries no security context at all,
  // so it shares an orchestrator with API traffic on a deployment that does not
  // partition by user.
  test('treats an absent security context as a change in both directions', async () => {
    const { core, driverFactory, request } = await createCore({
      driverFactory: (ctx: any) => (<any>{
        type: 'postgres',
        password: ctx.securityContext?.token ?? 'service-account',
      }),
    }, { token: 'token-a' });

    const user = <FakeDriver> await driverFactory('default');
    expect(user.builtFrom).toMatchObject({ password: 'token-a' });

    await request(undefined);
    const scheduler = <FakeDriver> await driverFactory('default');

    expect(scheduler).not.toBe(user);
    expect(scheduler.builtFrom).toMatchObject({ password: 'service-account' });
    expect(core.builtDrivers).toHaveLength(2);
  });
});
