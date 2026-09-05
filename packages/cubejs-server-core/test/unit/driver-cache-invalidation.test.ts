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
 * Rebuilds are rate-limited per data source, so a test that rebuilds more than
 * once has to say how much time passed in between. Patching `Date.now` rather
 * than using fake timers keeps the real microtask scheduling these tests depend
 * on to interleave concurrent resolutions.
 */
function fakeClock() {
  let now = Date.parse('2026-01-01T00:00:00Z');
  const spy = jest.spyOn(Date, 'now').mockImplementation(() => now);

  return {
    /** Longer than any rebuild interval the server enforces. */
    advancePastRebuildInterval: () => {
      now += 60 * 1000;
    },
    advance: (ms: number) => {
      now += ms;
    },
    restore: () => spy.mockRestore(),
  };
}

/**
 * Stands in for real driver construction so these tests exercise the caching
 * decisions without needing a database. Everything else — the driver factory
 * closure, the fingerprinting, the rebuild — is the production code path.
 */
class TestServerCore extends CubejsServerCore {
  public builtDrivers: FakeDriver[] = [];

  /** Set to fail the next driver construction, as a bad credential would. */
  public failNextBuild = false;

  /**
   * Runs before each staleness probe, standing in for another caller that wins
   * the race while this one is awaiting the factory. Re-entrant probes skip it,
   * so the hook can drive the driver factory itself.
   */
  public onStalenessProbe: (() => Promise<void>) | undefined;

  private inStalenessHook = false;

  protected async resolveDriverStaleness(origin: any, context: any): Promise<any> {
    if (this.onStalenessProbe && !this.inStalenessHook) {
      this.inStalenessHook = true;

      try {
        await this.onStalenessProbe();
      } finally {
        this.inStalenessHook = false;
      }
    }

    return super.resolveDriverStaleness(origin, context);
  }

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
  const logger = jest.fn();
  const core = new TestServerCore(<any>{
    contextToOrchestratorId: () => 'ORCHESTRATOR',
    logger,
    ...options,
  });
  const spy = jest.spyOn(<any>core, 'createOrchestratorApi');

  await core.getOrchestratorApi(<any>{ requestId: 'req-1', securityContext });

  const driverFactory = <DriverFactoryByDataSource>spy.mock.calls[0][0];

  return {
    core,
    driverFactory,
    logger,
    /** Log messages, with the params each was reported with. */
    logged: (message: string) => logger.mock.calls.filter(([msg]) => msg === message).map(([, params]) => params),
    /** Serve another request through the cached orchestrator. */
    request: (nextSecurityContext: unknown, requestId = 'req-n') => core.getOrchestratorApi(<any>{ requestId, securityContext: nextSecurityContext }),
  };
}

describe('driver cache invalidation', () => {
  let clock: ReturnType<typeof fakeClock>;

  beforeAll(() => {
    process.env.CUBEJS_API_SECRET = 'api-secret';
  });

  // Frozen by default, so a test that rebuilds twice has to be explicit about
  // the time in between rather than passing on whatever the wall clock did.
  beforeEach(() => {
    clock = fakeClock();
  });

  afterEach(() => {
    clock.restore();
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

    // Past the rebuild interval, so it is the alias bookkeeping that keeps these
    // two on one driver rather than the rate limit masking a second rebuild.
    clock.advancePastRebuildInterval();

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

  // Both rebuild logs have to survive the default log level, which drops any
  // message carrying neither `error` nor `warning`. Without that param the
  // rebuild — a connection pool being torn down — is invisible in production.
  test('reports every rebuild, and escalates once it looks like a misconfiguration', async () => {
    const { driverFactory, request, logged } = await createCore({
      driverFactory: (ctx: any) => (<any>{ type: 'postgres', password: ctx.securityContext.token }),
    }, { token: 'token-0' });

    await driverFactory('default');

    for (let i = 1; i <= 50; i++) {
      // Each rotation is its own, well clear of the previous rebuild, so all 50
      // are acted on rather than rate-limited.
      clock.advancePastRebuildInterval();
      // eslint-disable-next-line no-await-in-loop
      await request({ token: `token-${i}` }, `req-${i}`);
      // eslint-disable-next-line no-await-in-loop
      await driverFactory('default');
    }

    const rebuilds = logged('Rebuilding driver');

    expect(rebuilds).toHaveLength(50);
    expect(rebuilds.every((params) => params.warning)).toBe(true);
    expect(rebuilds.every((params) => params.reason === 'configuration change')).toBe(true);
    expect(rebuilds[0]).toMatchObject({ dataSource: 'default', rebuildCount: 1 });
    // Counted per alias set, so the 50th rotation reads as 50, not as a pair of
    // separate counters for `default` and `default@pre_agg`.
    expect(rebuilds[49]).toMatchObject({ rebuildCount: 50 });

    const escalations = logged('Driver rebuilt repeatedly');

    expect(escalations).toHaveLength(1);
    expect(escalations[0]).toMatchObject({ rebuildCount: 50 });
    expect(escalations[0].warning).toContain('contextToOrchestratorId');
  });

  // Losing the race enough times to exhaust the retry bound, where the winner's
  // own build then failed, leaves the key invalidated rather than replaced. The
  // driver this caller started from has already been released by that winner, so
  // it can be neither handed back nor released again — the only safe move is to
  // build. Reaching it needs four lost races and a failed build, hence the hook.
  test('builds instead of reusing a released driver when the retry bound is exhausted', async () => {
    const { core, driverFactory, request, logged } = await createCore({
      driverFactory: (ctx: any) => (<any>{ type: 'postgres', password: ctx.securityContext.token }),
    }, { token: 'token-a' });

    const first = <FakeDriver> await driverFactory('default');

    let round = 0;

    core.onStalenessProbe = async () => {
      round += 1;

      // A different context takes the orchestrator over, then rebuilds — which
      // replaces the key for the first three rounds. On the fourth that rebuild
      // fails, so it invalidates the key and releases what it replaced.
      await request({ token: `concurrent-${round}` }, `req-${round}`);

      if (round === 4) {
        core.failNextBuild = true;
      }

      await Promise.resolve(driverFactory('default')).catch(() => {});

      // Each round's rebuild has to sit outside the rate-limiting window by the
      // time the caller that lost the race retries — otherwise that retry is a
      // cache hit rather than another probe, and the bound is never reached.
      // Which is the rate limit working: displacement this rapid is what it
      // exists to stop, so provoking the bound means stepping past it.
      clock.advancePastRebuildInterval();
    };

    const resolved = <FakeDriver> await driverFactory('default');

    core.onStalenessProbe = undefined;
    await new Promise((resolve) => setImmediate(resolve));

    // Four rounds, so the bound was genuinely exhausted rather than short-circuited.
    expect(round).toBe(4);
    expect(resolved).not.toBe(first);
    // The returned driver is usable: not one some other caller already drained.
    expect(resolved.release).not.toHaveBeenCalled();
    // And nothing was released twice on the way there.
    expect(core.builtDrivers.every((driver) => driver.release.mock.calls.length <= 1)).toBe(true);
    expect(logged('Driver release error')).toHaveLength(0);
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

  // A `driverFactory` whose configuration is not stable across calls — a
  // credential minted per call, a nonce — resolves as changed every time. That
  // deployment works today, because the driver is resolved once and the
  // difference is never noticed, and it must not become a pool teardown per
  // query. Rebuilds are therefore rate-limited per data source, and inside the
  // window the cached driver is served exactly as it was before.
  test('rate-limits rebuilds, reporting the suppression once per window', async () => {
    let nonce = 0;
    const factory = jest.fn(() => {
      nonce += 1;

      return <any>{ type: 'postgres', password: `token-${nonce}` };
    });
    const { core, driverFactory, request, logged } = await createCore(
      { driverFactory: factory },
      { user: 'a' },
    );

    await driverFactory('default');

    clock.advancePastRebuildInterval();
    await request({ user: 'b' });
    const rebuilt = <FakeDriver> await driverFactory('default');

    expect(core.builtDrivers).toHaveLength(2);

    const callsBeforeSuppression = factory.mock.calls.length;

    // Three more contexts inside the window. Each would resolve a different
    // configuration, and none may replace the driver.
    for (const user of ['c', 'd', 'e']) {
      // eslint-disable-next-line no-await-in-loop
      await request({ user });
      // eslint-disable-next-line no-await-in-loop
      expect(await driverFactory('default')).toBe(rebuilt);
    }

    expect(core.builtDrivers).toHaveLength(2);
    expect(rebuilt.release).not.toHaveBeenCalled();
    // Not even asked: acting on the answer is what is rate-limited, and calling
    // user code per query to discard the result would be its own cost.
    expect(factory).toHaveBeenCalledTimes(callsBeforeSuppression);

    const suppressions = logged('Driver rebuild suppressed');

    // Once per window, not once per query.
    expect(suppressions).toHaveLength(1);
    expect(suppressions[0]).toMatchObject({ dataSource: 'default', rebuildCount: 1 });
    expect(suppressions[0].warning).toContain('driverFactory');
  });

  test('rebuilds again once the interval has passed', async () => {
    const { core, driverFactory, request } = await createCore({
      driverFactory: (ctx: any) => (<any>{ type: 'postgres', password: ctx.securityContext.token }),
    }, { token: 'token-a' });

    await driverFactory('default');

    clock.advancePastRebuildInterval();
    await request({ token: 'token-b' });
    const rebuilt = <FakeDriver> await driverFactory('default');

    // A second rotation inside the window is held back...
    await request({ token: 'token-c' });
    expect(await driverFactory('default')).toBe(rebuilt);

    // ...and picked up by the first resolution after it closes, so the rate
    // limit delays a rotation rather than dropping it.
    clock.advancePastRebuildInterval();
    const latest = <FakeDriver> await driverFactory('default');

    expect(latest).not.toBe(rebuilt);
    expect(latest.builtFrom).toMatchObject({ password: 'token-c' });
    expect(core.builtDrivers).toHaveLength(3);
  });

  // The probe can never observe a factory switching from configs to a
  // constructed driver — `OptsHandler` rejects the second shape — but what that
  // rejection must not do is fail a query. It surfaces as a probe failure, and
  // the driver the deployment is already using keeps serving. Pinned because the
  // staleness check is what put user code on a path that used to be a pure cache
  // hit, and the driver it hands back must never be one nobody owns.
  test('keeps serving the cached driver when the factory changes its return shape', async () => {
    class ConstructedDriver extends BaseDriver {
      public release = jest.fn(async () => {});

      public testConnection = jest.fn(async () => {});

      public async query<R = unknown>(): Promise<R[]> {
        return [];
      }
    }

    const constructed = new ConstructedDriver();
    let returnDriver = false;
    const factory = jest.fn((ctx: any) => (returnDriver
      ? <any>constructed
      : <any>{ type: 'postgres', password: ctx.securityContext.token }));

    const { core, driverFactory, request, logged } = await createCore(
      { driverFactory: factory },
      { token: 'token-a' },
    );

    const built = <FakeDriver> await driverFactory('default');

    returnDriver = true;
    clock.advancePastRebuildInterval();
    await request({ token: 'token-b' });

    expect(await driverFactory('default')).toBe(built);

    await new Promise((resolve) => setImmediate(resolve));

    expect(core.builtDrivers).toHaveLength(1);
    expect(built.release).not.toHaveBeenCalled();
    // Not touched either: whatever the factory constructed belongs to the
    // factory, which may be handing out a singleton it expects to keep working.
    expect(constructed.release).not.toHaveBeenCalled();
    expect(logged('Driver staleness check error')).toHaveLength(1);
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
  // The other half of CUB-3599, and the half no comparison can reach: the
  // credential stopped rotating instead of rotating to something new. The
  // factory keeps resolving an identical configuration while the connection
  // built from it is already dead, so only a stated lifetime can replace it.
  test('rebuilds when the stated lifetime elapses, configuration unchanged', async () => {
    const expiresAt = Date.now() + 60 * 60 * 1000;
    const { core, driverFactory } = await createCore({
      driverFactory: () => (<any>{ type: 'postgres', password: 'frozen-token', expiresAt }),
    }, { token: 'token-a' });

    const first = <FakeDriver> await driverFactory('default');

    // Same request context, same configuration — and past the deadline.
    clock.advance(61 * 60 * 1000);

    const second = <FakeDriver> await driverFactory('default');

    expect(second).not.toBe(first);
    expect(core.builtDrivers).toHaveLength(2);
    expect(first.release).toHaveBeenCalled();
  });

  test('reports the lifetime rebuild as its own reason', async () => {
    const expiresAt = Date.now() + 60 * 60 * 1000;
    const { driverFactory, logged } = await createCore({
      driverFactory: () => (<any>{ type: 'postgres', password: 'frozen-token', expiresAt }),
    }, { token: 'token-a' });

    await driverFactory('default');
    clock.advance(61 * 60 * 1000);
    await driverFactory('default');

    const rebuilds = logged('Rebuilding driver');

    expect(rebuilds).toHaveLength(1);
    expect(rebuilds[0]).toMatchObject({ reason: 'lifetime elapsed', rebuildCount: 1 });
  });

  test('keeps the driver until its lifetime elapses', async () => {
    const expiresAt = Date.now() + 60 * 60 * 1000;
    const { core, driverFactory } = await createCore({
      driverFactory: () => (<any>{ type: 'postgres', password: 'frozen-token', expiresAt }),
    }, { token: 'token-a' });

    const first = await driverFactory('default');

    clock.advance(59 * 60 * 1000);

    expect(await driverFactory('default')).toBe(first);
    expect(core.builtDrivers).toHaveLength(1);
  });

  // A deadline recomputed from the current clock differs on every call. It is
  // excluded from the configuration's identity precisely so that a factory
  // written that way does not rebuild the pool on a timer.
  test('a lifetime that changes on every call is not a configuration change', async () => {
    const { core, driverFactory, request } = await createCore({
      driverFactory: () => (<any>{
        type: 'postgres',
        password: 'stable-token',
        expiresAt: Date.now() + 60 * 60 * 1000,
      }),
    }, { token: 'token-a' });

    const first = await driverFactory('default');

    for (let i = 1; i <= 5; i++) {
      clock.advancePastRebuildInterval();
      // eslint-disable-next-line no-await-in-loop
      await request({ token: `token-${i}` }, `req-${i}`);
      // eslint-disable-next-line no-await-in-loop
      expect(await driverFactory('default')).toBe(first);
    }

    expect(core.builtDrivers).toHaveLength(1);
  });

  // The lifetime is not fingerprinted, so a credential re-issued with the same
  // value and a later deadline compares equal. Carrying the new deadline over is
  // what stops that driver from being rebuilt on the old one, once per window,
  // for as long as the process runs.
  test('carries a later deadline over when the configuration is unchanged', async () => {
    let expiresAt = Date.now() + 60 * 60 * 1000;
    const { core, driverFactory, request } = await createCore({
      driverFactory: () => (<any>{ type: 'postgres', password: 'stable-token', expiresAt }),
    }, { token: 'token-a' });

    const first = await driverFactory('default');

    // The credential is re-issued 30 minutes in: same token, later deadline.
    clock.advance(30 * 60 * 1000);
    expiresAt = Date.now() + 60 * 60 * 1000;
    await request({ token: 'token-b' });

    expect(await driverFactory('default')).toBe(first);

    // Past the original deadline, inside the new one.
    clock.advance(45 * 60 * 1000);

    expect(await driverFactory('default')).toBe(first);
    expect(core.builtDrivers).toHaveLength(1);
  });

  // A `driverFactory` that fails closed on an unusable credential is stating
  // that the connection must not serve queries. Reusing the cached driver
  // forever because the refusal arrives as a throw is how an expired credential
  // goes on being served from a pool nobody rebuilds.
  test('gives the driver up when the factory keeps refusing', async () => {
    let shouldFail = false;
    const { core, driverFactory, request, logged } = await createCore({
      driverFactory: (ctx: any) => {
        if (shouldFail) {
          throw new Error('credential is unusable');
        }

        return <any>{ type: 'postgres', password: ctx.securityContext.token };
      },
    }, { token: 'token-a' });

    const first = await driverFactory('default');

    shouldFail = true;

    // Two refusals inside the grace window: still transient as far as this can
    // tell, so the cached driver is reused.
    await request({ token: 'token-b' }, 'req-2');
    expect(await driverFactory('default')).toBe(first);

    clock.advance(4 * 60 * 1000);
    await request({ token: 'token-c' }, 'req-3');
    expect(await driverFactory('default')).toBe(first);

    // Sustained past the grace window — and with no gap long enough to have
    // aged the earlier refusals out. The driver is given up, and the caller
    // sees the factory's own error rather than a connection it refused to build.
    clock.advance(4 * 60 * 1000);
    await request({ token: 'token-d' }, 'req-4');

    await expect(driverFactory('default')).rejects.toThrow('credential is unusable');

    await new Promise(process.nextTick);
    expect((<FakeDriver>first).release).toHaveBeenCalled();
    expect(core.builtDrivers).toHaveLength(1);

    const released = logged('Rebuilding driver')
      .filter((params: any) => params.reason === 'repeated staleness check failures');

    expect(released).toHaveLength(1);
    expect(released[0]).toMatchObject({ dataSource: 'default', rebuildCount: 1 });

    // And once the credential is usable again, the next request rebuilds.
    shouldFail = false;
    await request({ token: 'token-e' }, 'req-5');

    const rebuilt = <FakeDriver> await driverFactory('default');

    expect(rebuilt.builtFrom).toMatchObject({ password: 'token-e' });
  });

  test('a recovered factory resets the refusal count', async () => {
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

    // Two refusals, then a probe that resolves — which clears the count, so the
    // two refusals after it cannot reach the bound between them.
    for (const token of ['token-b', 'token-c']) {
      shouldFail = true;
      // eslint-disable-next-line no-await-in-loop
      await request({ token }, `req-${token}`);
      // eslint-disable-next-line no-await-in-loop
      expect(await driverFactory('default')).toBe(first);
      clock.advance(31 * 1000);
    }

    shouldFail = false;
    await request({ token: 'token-a' }, 'req-recovered');
    expect(await driverFactory('default')).toBe(first);

    shouldFail = true;
    clock.advance(31 * 1000);
    await request({ token: 'token-d' }, 'req-d');

    expect(await driverFactory('default')).toBe(first);
    expect(core.builtDrivers).toHaveLength(1);
  });

  // Probes only run when the security context changes, so in a quiet deployment
  // they can be hours apart. A counter that only ever went up would read three
  // unrelated flakes on three different days as one sustained outage and tear
  // down a working pool for it.
  test('refusals spread beyond the window do not accumulate', async () => {
    let shouldFail = false;
    const { core, driverFactory, request, logged } = await createCore({
      driverFactory: (ctx: any) => {
        if (shouldFail) {
          throw new Error('secret store unreachable');
        }

        return <any>{ type: 'postgres', password: ctx.securityContext.token };
      },
    }, { token: 'token-a' });

    const first = await driverFactory('default');

    shouldFail = true;

    // Past retention between each, so every refusal opens a fresh window rather
    // than extending the one before it. Six of them, twice the bound, and the
    // driver is still there.
    for (const token of ['token-b', 'token-c', 'token-d', 'token-e', 'token-f', 'token-g']) {
      clock.advance(31 * 60 * 1000);
      // eslint-disable-next-line no-await-in-loop
      await request({ token }, `req-${token}`);
      // eslint-disable-next-line no-await-in-loop
      expect(await driverFactory('default')).toBe(first);
    }

    expect(core.builtDrivers).toHaveLength(1);
    expect((<FakeDriver>first).release).not.toHaveBeenCalled();
    expect(logged('Rebuilding driver')).toHaveLength(0);
  });

  // Giving a driver up tears down the same pool a configuration change does, so
  // it has to answer to the same rate limit. Bypassing it let a factory that
  // fails probes but succeeds when called directly churn a pool every window,
  // with nothing in the logs that read as churn.
  test('rate-limits repeated give-ups', async () => {
    let shouldFailProbe = false;
    const { core, driverFactory, request, logged } = await createCore({
      driverFactory: (ctx: any) => {
        if (shouldFailProbe) {
          throw new Error('secret store unreachable');
        }

        return <any>{ type: 'postgres', password: ctx.securityContext.token };
      },
    }, { token: 'token-a' });

    await driverFactory('default');

    // Three refusals spanning the grace window give the driver up. The build
    // that follows succeeds, because it calls the factory directly.
    shouldFailProbe = true;

    for (const token of ['token-b', 'token-c']) {
      // eslint-disable-next-line no-await-in-loop
      await request({ token }, `req-${token}`);
      // eslint-disable-next-line no-await-in-loop
      await driverFactory('default');
      clock.advance(4 * 60 * 1000);
    }

    await request({ token: 'token-d' }, 'req-d');
    shouldFailProbe = false;

    const rebuilt = await driverFactory('default');

    expect(logged('Rebuilding driver')).toHaveLength(1);
    expect(core.builtDrivers).toHaveLength(2);

    // The give-up opened a suppression window like any other replacement, so
    // the next changed context reuses rather than tearing the new pool down.
    await request({ token: 'token-e' }, 'req-e');

    expect(await driverFactory('default')).toBe(rebuilt);
    expect(core.builtDrivers).toHaveLength(2);
    expect(logged('Driver rebuild suppressed')).toHaveLength(1);
  });

  // The other half of that contract. Probes only fire when the security context
  // changes, so a few-user deployment may probe far slower than the grace
  // window — and a dead credential there has to be given up eventually, which
  // is what retention being longer than the grace window buys.
  test('gives up a refusal that is sustained but slow', async () => {
    let shouldFail = false;
    const { driverFactory, request, logged } = await createCore({
      driverFactory: (ctx: any) => {
        if (shouldFail) {
          throw new Error('credential is unusable');
        }

        return <any>{ type: 'postgres', password: ctx.securityContext.token };
      },
    }, { token: 'token-a' });

    const first = await driverFactory('default');

    shouldFail = true;

    // Ten minutes apart: slower than the grace window, well inside retention.
    for (const token of ['token-b', 'token-c']) {
      // eslint-disable-next-line no-await-in-loop
      await request({ token }, `req-${token}`);
      // eslint-disable-next-line no-await-in-loop
      expect(await driverFactory('default')).toBe(first);
      clock.advance(10 * 60 * 1000);
    }

    await request({ token: 'token-d' }, 'req-d');

    await expect(driverFactory('default')).rejects.toThrow('credential is unusable');

    await new Promise(process.nextTick);
    expect((<FakeDriver>first).release).toHaveBeenCalled();
    expect(logged('Rebuilding driver')
      .filter((params: any) => params.reason === 'repeated staleness check failures'))
      .toHaveLength(1);
  });

  // Retention outliving the grace window must not make the bound reachable
  // across unrelated incidents. Concurrent probes all fail on one blink of a
  // dependency, so a burst is one refusal — otherwise two brief outages half an
  // hour apart would drain a working pool.
  test('counts a burst of refusals as one incident', async () => {
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

    // Three refusals at the same instant: one dependency blink under load.
    for (const token of ['token-b', 'token-c', 'token-d']) {
      // eslint-disable-next-line no-await-in-loop
      await request({ token }, `req-${token}`);
      // eslint-disable-next-line no-await-in-loop
      expect(await driverFactory('default')).toBe(first);
    }

    // A second, unrelated blink six minutes later. Two incidents, not four
    // refusals, so the bound is not reached and the pool survives.
    clock.advance(6 * 60 * 1000);
    await request({ token: 'token-e' }, 'req-e');

    expect(await driverFactory('default')).toBe(first);
    expect((<FakeDriver>first).release).not.toHaveBeenCalled();
    expect(core.builtDrivers).toHaveLength(1);
  });

  // The other end of the traffic range from the burst case. Under steady load
  // refusals arrive faster than the coalescing window, so they are all one
  // incident — and an incident that never ends must be caught by having lasted,
  // or a permanently dead credential is never given up where it costs most.
  test('gives up an unbroken refusal stream under steady traffic', async () => {
    let shouldFail = false;
    const { driverFactory, request } = await createCore({
      driverFactory: (ctx: any) => {
        if (shouldFail) {
          throw new Error('credential is unusable');
        }

        return <any>{ type: 'postgres', password: ctx.securityContext.token };
      },
    }, { token: 'token-a' });

    const first = await driverFactory('default');

    shouldFail = true;

    const startedAt = Date.now();
    let gaveUpAfterMs: number | undefined;

    // A refusal every 1.5s — inside the coalescing window, so nothing here ever
    // starts a second incident.
    for (let i = 0; i < 400 && gaveUpAfterMs === undefined; i++) {
      clock.advance(1500);
      // eslint-disable-next-line no-await-in-loop
      await request({ token: `token-${i}` }, `req-${i}`);

      try {
        // eslint-disable-next-line no-await-in-loop
        await driverFactory('default');
      } catch (error) {
        gaveUpAfterMs = Date.now() - startedAt;
      }
    }

    // Given up once the incident had itself run the grace window, not before.
    expect(gaveUpAfterMs).toBeGreaterThanOrEqual(5 * 60 * 1000);
    expect(gaveUpAfterMs).toBeLessThan(6 * 60 * 1000);

    await new Promise(process.nextTick);
    expect((<FakeDriver>first).release).toHaveBeenCalled();
  });

  // The other side of that boundary. An incident bounded by its duration has to
  // be reused right up until the duration is reached, or a single dependency
  // outage shorter than the grace window drains the pool after all.
  test('reuses through an incident shorter than the grace window', async () => {
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

    // Four minutes of refusals at 1.5s — one unbroken incident, under the bound.
    for (let i = 0; i < 160; i++) {
      clock.advance(1500);
      // eslint-disable-next-line no-await-in-loop
      await request({ token: `token-${i}` }, `req-${i}`);
      // eslint-disable-next-line no-await-in-loop
      expect(await driverFactory('default')).toBe(first);
    }

    expect((<FakeDriver>first).release).not.toHaveBeenCalled();
    expect(core.builtDrivers).toHaveLength(1);
  });

  // Refusals are recorded for a context that changed, but most requests never
  // probe at all — a security context matching the cached driver is a plain
  // cache hit. If those cleared the record, one context's hits would erase
  // another's refusals forever, and on a shared orchestrator a refresh
  // scheduler tick alone would keep the bound permanently out of reach.
  test('a cache hit that never probed does not clear recorded refusals', async () => {
    let shouldFail = false;
    const { driverFactory, request } = await createCore({
      driverFactory: (ctx: any) => {
        if (shouldFail) {
          throw new Error('credential is unusable');
        }

        return <any>{ type: 'postgres', password: ctx.securityContext.token };
      },
    }, { token: 'token-a' });

    const first = await driverFactory('default');

    shouldFail = true;

    // Each refusal is a changed context; between them, a request on the very
    // context the driver was built from, which matches the fingerprint and so
    // returns without ever asking the factory.
    for (const token of ['token-b', 'token-c']) {
      // eslint-disable-next-line no-await-in-loop
      await request({ token }, `req-${token}`);
      // eslint-disable-next-line no-await-in-loop
      expect(await driverFactory('default')).toBe(first);

      clock.advance(4 * 60 * 1000);

      // eslint-disable-next-line no-await-in-loop
      await request({ token: 'token-a' }, `req-hit-${token}`);
      // eslint-disable-next-line no-await-in-loop
      expect(await driverFactory('default')).toBe(first);
    }

    // Third refusal: the count survived the interleaved hits, so the bound is
    // reached and the caller sees the factory's own error.
    await request({ token: 'token-d' }, 'req-d');

    await expect(driverFactory('default')).rejects.toThrow('credential is unusable');

    await new Promise(process.nextTick);
    expect((<FakeDriver>first).release).toHaveBeenCalled();
  });

  // Replacing a driver cannot move a deadline the factory keeps re-asserting.
  // Honouring one would find the new driver stale the moment its suppression
  // window closed, for the life of the process.
  test('ignores a lifetime that has already elapsed', async () => {
    const { core, driverFactory, request, logged } = await createCore({
      driverFactory: (ctx: any) => (<any>{
        type: 'postgres',
        password: ctx.securityContext.token,
        // A factory passing the provider's expiry straight through, on a
        // credential whose refresh has already stopped.
        expiresAt: Date.now() - 60 * 1000,
      }),
    }, { token: 'token-a' });

    const first = await driverFactory('default');

    expect(logged('Driver lifetime ignored')).toHaveLength(1);

    // Past any suppression window: an honoured deadline would rebuild here, and
    // again on every window after it.
    clock.advancePastRebuildInterval();
    await request({ token: 'token-a' }, 'req-2');

    expect(await driverFactory('default')).toBe(first);

    clock.advancePastRebuildInterval();
    await request({ token: 'token-a' }, 'req-3');

    expect(await driverFactory('default')).toBe(first);
    expect(core.builtDrivers).toHaveLength(1);
    expect(logged('Rebuilding driver')).toHaveLength(0);
  });

  // The deadline is resolved again on every probe, and probes run whenever the
  // security context changes. Reporting per call would trade the churn the
  // guard removes for a warning on every query that arrives with a fresh JWT.
  test('reports an ignored lifetime once, not once per probe', async () => {
    const { core, driverFactory, request, logged } = await createCore({
      // Ignores the context: the configuration never changes, only the elapsed
      // deadline it keeps re-asserting.
      driverFactory: () => (<any>{
        type: 'postgres',
        password: 'service-account',
        expiresAt: Date.now() - 60 * 1000,
      }),
    }, { token: 'token-a' });

    const first = await driverFactory('default');

    // Each of these changes the fingerprint, so each one probes and each one
    // carries the same unusable deadline over.
    for (const token of ['token-b', 'token-c', 'token-d', 'token-e']) {
      // eslint-disable-next-line no-await-in-loop
      await request({ token }, `req-${token}`);
      // eslint-disable-next-line no-await-in-loop
      expect(await driverFactory('default')).toBe(first);
    }

    expect(logged('Driver lifetime ignored')).toHaveLength(1);
    expect(core.builtDrivers).toHaveLength(1);
  });

  // A lifetime shorter than the replacement interval is the same loop as an
  // elapsed one: stale the moment the suppression window closes, every window,
  // for the life of the process.
  test('ignores a lifetime shorter than the replacement interval', async () => {
    const { core, driverFactory, request, logged } = await createCore({
      driverFactory: () => (<any>{
        type: 'postgres',
        password: 'sts-credential',
        // Genuinely in the future, and still too short for the rate limiter to
        // ever let this mechanism act on it.
        expiresAt: Date.now() + 10 * 1000,
      }),
    }, { token: 'token-a' });

    const first = await driverFactory('default');

    expect(logged('Driver lifetime ignored')).toHaveLength(1);

    // Past the deadline and past any suppression window, twice over.
    for (const requestId of ['req-2', 'req-3']) {
      clock.advancePastRebuildInterval();
      // eslint-disable-next-line no-await-in-loop
      await request({ token: 'token-a' }, requestId);
      // eslint-disable-next-line no-await-in-loop
      expect(await driverFactory('default')).toBe(first);
    }

    expect(core.builtDrivers).toHaveLength(1);
    expect(logged('Rebuilding driver')).toHaveLength(0);
  });

  // The lower bound judges what the factory states, not how much of an accepted
  // deadline is left. Re-judging on the carry-over would drop a good deadline as
  // it entered its final window, switching the lifetime off in exactly the
  // stretch it exists to cover.
  test('keeps a deadline that a probe lands inside the final window of', async () => {
    const expiresAt = Date.now() + 60 * 60 * 1000;
    const { core, driverFactory, request, logged } = await createCore({
      // Ignores the context, so the configuration compares equal and the probe
      // takes the carry-over path.
      driverFactory: () => (<any>{ type: 'postgres', password: 'static', expiresAt }),
    }, { token: 'token-a' });

    const first = await driverFactory('default');

    // A re-issued JWT ten seconds before the deadline: inside the replacement
    // interval, but this deadline was judged an hour ago and accepted.
    clock.advance(60 * 60 * 1000 - 10 * 1000);
    await request({ token: 'token-b' }, 'req-2');

    expect(await driverFactory('default')).toBe(first);
    expect(logged('Driver lifetime ignored')).toHaveLength(0);

    // And the deadline it kept still fires.
    clock.advance(11 * 1000);
    await request({ token: 'token-c' }, 'req-3');

    expect(await driverFactory('default')).not.toBe(first);
    expect(core.builtDrivers).toHaveLength(2);
  });

  // A deadline is excluded from the fingerprint, so an unchanged credential can
  // arrive with a moved one. If that new deadline cannot be honoured, the one
  // this driver was already held to still can — dropping to no deadline at all
  // is the failure the stated-not-aged rule exists to prevent.
  test('keeps the accepted deadline when a newly stated one cannot be honoured', async () => {
    const accepted = Date.now() + 60 * 60 * 1000;
    let expiresAt = accepted;
    const { core, driverFactory, request } = await createCore({
      driverFactory: () => (<any>{ type: 'postgres', password: 'static', expiresAt }),
    }, { token: 'token-a' });

    const first = await driverFactory('default');

    // Ten seconds before the accepted deadline, the factory re-states the same
    // credential with a slightly different deadline — inside the interval, so
    // unhonourable as a new lifetime.
    clock.advance(60 * 60 * 1000 - 10 * 1000);
    expiresAt = accepted + 5 * 1000;
    await request({ token: 'token-b' }, 'req-2');

    expect(await driverFactory('default')).toBe(first);

    // The accepted deadline still fires.
    clock.advance(11 * 1000);
    await request({ token: 'token-c' }, 'req-3');

    expect(await driverFactory('default')).not.toBe(first);
    expect(core.builtDrivers).toHaveLength(2);
  });

  // The other half of the lifetime contract: a deadline the rate limiter can
  // honour is still honoured, so widening the guard did not disable the feature.
  test('honours a lifetime longer than the replacement interval', async () => {
    let expiresAt = Date.now() + 60 * 60 * 1000;
    const { core, driverFactory, request } = await createCore({
      driverFactory: () => (<any>{ type: 'postgres', password: 'static', expiresAt }),
    }, { token: 'token-a' });

    const first = await driverFactory('default');

    clock.advance(61 * 60 * 1000);
    expiresAt = Date.now() + 60 * 60 * 1000;
    await request({ token: 'token-a' }, 'req-2');

    expect(await driverFactory('default')).not.toBe(first);
    expect(core.builtDrivers).toHaveLength(2);
  });
});
