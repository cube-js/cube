import { BaseDriver } from '@cubejs-backend/query-orchestrator';
import type { DriverFactoryByDataSource } from '@cubejs-backend/query-orchestrator';

import { CubejsServerCore } from '../../src';
import { OrchestratorApi, OrchestratorApiOptions } from '../../src/core/OrchestratorApi';

let onTestConnection: (driver: FakeDriver) => Promise<void> | void = () => undefined;
let onRelease: (driver: FakeDriver) => Promise<void> | void = () => undefined;

class FakeDriver extends BaseDriver {
  public tests = 0;

  public releases = 0;

  public constructor(public readonly builtFrom: Record<string, any>) {
    super({});
  }

  public async testConnection() {
    this.tests += 1;
    await onTestConnection(this);
  }

  public async query<R = unknown>(): Promise<R[]> {
    return [];
  }

  public async release() {
    this.releases += 1;
    await onRelease(this);
  }
}

class TestServerCore extends CubejsServerCore {
  public readonly driverFactories: DriverFactoryByDataSource[] = [];

  protected readonly apis: OrchestratorApi[] = [];

  /** Waits for the swaps requests started in the background. */
  public async swapsSettled() {
    await Promise.all(this.apis.map((api) => this.orchestratorDrivers.get(api)?.settled()));
  }

  protected createOrchestratorApi(getDriver: DriverFactoryByDataSource, options: OrchestratorApiOptions): OrchestratorApi {
    this.driverFactories.push(getDriver);

    const api = super.createOrchestratorApi(getDriver, options);
    this.apis.push(api);

    return api;
  }
}

const cores: CubejsServerCore[] = [];

function createCore(options: Record<string, unknown> = {}) {
  const logger = jest.fn();
  const driverFactory = jest.fn((ctx: any): any => ({ type: 'postgres', password: ctx.securityContext.token }));

  const core = new TestServerCore(<any>{
    apiSecret: 'secret',
    contextToOrchestratorId: () => 'ORCHESTRATOR_ID',
    driverFactory,
    driverVersion: (ctx: any) => ctx.securityContext.token,
    logger,
    ...options,
  });

  cores.push(core);

  // No token stands for the readiness probe, which passes an empty context
  const request = (token?: string) => core.getOrchestratorApi(
    <any>(token === undefined ? {} : { requestId: `request-${token}`, securityContext: { token } })
  );

  return {
    core,
    driverFactory,
    request,
    swapped: () => core.swapsSettled(),
    swapTo: async (token: string) => {
      const api = await request(token);
      await core.swapsSettled();

      return api;
    },
    driver: async (dataSource = 'default', preAggregations = false) => <FakeDriver>(
      await core.driverFactories[0](dataSource, preAggregations)
    ),
    logged: (message: string) => logger.mock.calls.filter(([msg]) => msg === message),
  };
}

const flush = () => new Promise(resolve => { setImmediate(resolve); });

async function waitFor(condition: () => boolean) {
  for (let i = 0; i < 100 && !condition(); i++) {
    await flush();
  }

  if (!condition()) {
    throw new Error('Timed out waiting for the condition');
  }
}

function gate() {
  let open: () => void;
  const opened = new Promise<void>(resolve => { open = resolve; });

  return { opened, open: () => open() };
}

describe('driverVersion', () => {
  let built: FakeDriver[];

  beforeEach(() => {
    built = [];
    onTestConnection = () => undefined;
    onRelease = () => undefined;

    jest.spyOn(CubejsServerCore, 'createDriver').mockImplementation((_type, options) => {
      const driver = new FakeDriver(<any>options);
      built.push(driver);

      return driver;
    });
  });

  afterEach(async () => {
    await Promise.all(cores.splice(0).map(core => core.releaseConnections()));
    jest.useRealTimers();
    jest.restoreAllMocks();
  });

  test('serves the old drivers while new ones are built and tested, then swaps to them; not for an unchanged version', async () => {
    const { request, swapTo, swapped, driver, driverFactory } = createCore();

    await request('a');
    const first = await driver();
    await swapTo('a');

    expect(await driver()).toBe(first);
    expect(driverFactory).toHaveBeenCalledTimes(1);

    const testing = gate();
    onTestConnection = (d) => (d.builtFrom.password === 'b' ? testing.opened : undefined);

    // Resolves while the new driver is still under test
    await request('b');
    await waitFor(() => built.length === 2);

    expect(driverFactory.mock.calls[1][0].securityContext).toEqual({ token: 'b' });
    expect(built[1].tests).toEqual(1);
    expect(await driver()).toBe(first);

    testing.open();
    await swapped();

    expect(await driver()).toBe(built[1]);
    expect(built[1].builtFrom).toMatchObject({ password: 'b' });
    expect(first.releases).toEqual(0);
  });

  test('concurrent requests with one new version share a single swap', async () => {
    const { request, swapTo, swapped, driver, driverFactory } = createCore();

    await swapTo('a');
    await driver();

    const apis = await Promise.all(Array.from({ length: 5 }, () => request('b')));
    await swapped();

    expect(new Set(apis).size).toEqual(1);
    expect(driverFactory).toHaveBeenCalledTimes(2);
    expect(await driver()).toBe(built[1]);
  });

  test('a failed swap keeps the old drivers and backs off before retrying', async () => {
    let now = Date.parse('2026-01-01T00:00:00Z');
    jest.spyOn(Date, 'now').mockImplementation(() => now);

    const { swapTo, driver, driverFactory, logged } = createCore();

    await swapTo('a');
    const first = await driver();

    onTestConnection = (d) => {
      if (d.builtFrom.password === 'b') {
        throw new Error('bad credential');
      }
    };

    await expect(swapTo('b')).resolves.toBeDefined();

    expect(logged('Driver Swap Error')[0][1].error).toContain('bad credential');
    expect(await driver()).toBe(first);
    expect(first.releases).toEqual(0);
    expect(built[1].releases).toEqual(1);

    // The old version stands, so its own requests do not swap back
    await swapTo('a');
    expect(driverFactory).toHaveBeenCalledTimes(2);

    now += 9 * 1000;
    await swapTo('b');
    await swapTo('b');

    expect(driverFactory).toHaveBeenCalledTimes(2);
    expect(logged('Driver Swap Backoff')).toHaveLength(1);
    expect(await driver()).toBe(first);

    onTestConnection = () => undefined;
    now += 1000;
    await swapTo('b');

    expect(driverFactory).toHaveBeenCalledTimes(3);
    expect(await driver()).toBe(built[2]);
  });

  test('two failing versions both back off', async () => {
    let now = Date.parse('2026-01-01T00:00:00Z');
    jest.spyOn(Date, 'now').mockImplementation(() => now);

    const { swapTo, driver, driverFactory } = createCore();

    await swapTo('a');
    await driver();

    onTestConnection = (d) => {
      if (d.builtFrom.password !== 'a') {
        throw new Error('bad credential');
      }
    };

    await swapTo('b');
    await swapTo('c');
    expect(driverFactory).toHaveBeenCalledTimes(3);

    now += 1000;
    await swapTo('b');
    await swapTo('c');
    expect(driverFactory).toHaveBeenCalledTimes(3);
  });

  const releaseDelays: Array<[string, Record<string, unknown>, string | undefined, number]> = [
    ['the default query timeout', {}, undefined, 600],
    ['CUBEJS_DB_QUERY_TIMEOUT', {}, '45s', 45],
    [
      'the longer of its queues\' execution timeouts',
      {
        orchestratorOptions: {
          queryCacheOptions: { queueOptions: { executionTimeout: 30 } },
          preAggregationsOptions: { queueOptions: { executionTimeout: 90 } },
        },
      },
      undefined,
      90,
    ],
  ];

  test.each(releaseDelays)('releases a replaced driver after %s', async (_name, options, env, seconds) => {
    jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate'] });

    const previousEnv = process.env.CUBEJS_DB_QUERY_TIMEOUT;

    if (env !== undefined) {
      process.env.CUBEJS_DB_QUERY_TIMEOUT = env;
    }

    try {
      const { swapTo, driver } = createCore(options);

      await swapTo('a');
      const first = await driver();
      await swapTo('b');

      jest.advanceTimersByTime(seconds * 1000 - 1);
      await flush();
      expect(first.releases).toEqual(0);

      jest.advanceTimersByTime(1);
      await flush();
      expect(first.releases).toEqual(1);
      expect(built[1].releases).toEqual(0);
    } finally {
      if (previousEnv === undefined) {
        delete process.env.CUBEJS_DB_QUERY_TIMEOUT;
      } else {
        process.env.CUBEJS_DB_QUERY_TIMEOUT = previousEnv;
      }
    }
  });

  test('releaseConnections releases replaced drivers without waiting for the query timeout', async () => {
    const { core, swapTo, driver } = createCore();

    await swapTo('a');
    const first = await driver();
    await swapTo('b');

    expect(first.releases).toEqual(0);

    await core.releaseConnections();

    expect(first.releases).toEqual(1);
  });

  const lookupOrders: Array<[string, boolean]> = [
    ['the data source first', false],
    ['its pre-aggregations first', true],
  ];

  test.each(lookupOrders)('a data source and its pre-aggregations share one driver, swapped together, asked for %s', async (_name, preAggregationsFirst) => {
    const { swapTo, driver, driverFactory } = createCore();

    await swapTo('a');
    const first = await driver('default', preAggregationsFirst);

    expect(await driver('default', !preAggregationsFirst)).toBe(first);
    expect(built).toHaveLength(1);

    await swapTo('b');

    expect(driverFactory).toHaveBeenCalledTimes(2);
    expect(await driver()).toBe(built[1]);
    expect(await driver('default', true)).toBe(built[1]);
  });

  test('separate pre-aggregation credentials keep a separate driver through a swap', async () => {
    process.env.CUBEJS_PRE_AGGREGATIONS_DB_HOST = 'pre-aggregations.example.com';

    try {
      const { swapTo, driver } = createCore();

      await swapTo('a');
      const regular = await driver();
      const preAggregations = await driver('default', true);

      expect(preAggregations).not.toBe(regular);

      await swapTo('b');

      expect(built).toHaveLength(4);
      expect(await driver()).not.toBe(await driver('default', true));
    } finally {
      delete process.env.CUBEJS_PRE_AGGREGATIONS_DB_HOST;
    }
  });

  test('a factory returning constructed drivers is swapped', async () => {
    const { swapTo, driver } = createCore({
      driverFactory: (ctx: any) => new FakeDriver({ password: ctx.securityContext.token }),
    });

    await swapTo('a');
    const first = await driver();
    await swapTo('b');
    const second = await driver();

    expect(second).not.toBe(first);
    expect(second.builtFrom).toEqual({ password: 'b' });
    expect(second.tests).toEqual(1);
  });

  test('a driver the factory hands out again is not released as replaced', async () => {
    const shared = new FakeDriver({ password: 'shared' });
    const { core, swapTo, driver } = createCore({ driverFactory: () => shared });

    await swapTo('a');
    await driver();
    await swapTo('b');

    expect(await driver()).toBe(shared);

    await core.releaseConnections();

    expect(shared.releases).toEqual(0);
  });

  // A factory that caches one driver per credential hands an earlier driver back
  function createCachingCore() {
    const instances: Record<string, FakeDriver> = {};

    return {
      instances,
      ...createCore({
        driverFactory: (ctx: any) => {
          const { token } = ctx.securityContext;
          instances[token] = instances[token] || new FakeDriver({ password: token });

          return instances[token];
        },
      }),
    };
  }

  test('a driver reinstalled by a later swap is not released by the earlier one', async () => {
    jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate'] });

    const { swapTo, driver, instances } = createCachingCore();

    await swapTo('a');
    await driver();
    await swapTo('b');

    jest.advanceTimersByTime(1000);
    await swapTo('a');

    expect(await driver()).toBe(instances.a);

    jest.advanceTimersByTime(600 * 1000);
    await flush();

    expect(instances.a.releases).toEqual(0);
    expect(instances.b.releases).toEqual(1);
  });

  test('a failed swap back to a driver awaiting release leaves it to its timer', async () => {
    jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate'] });

    const { swapTo, driver, instances, logged } = createCachingCore();

    await swapTo('a');
    await driver();
    await swapTo('b');

    onTestConnection = (d) => {
      if (d === instances.a) {
        throw new Error('bad credential');
      }
    };

    await swapTo('a');

    expect(logged('Driver Swap Error')).toHaveLength(1);
    expect(await driver()).toBe(instances.b);
    expect(instances.a.releases).toEqual(0);

    jest.advanceTimersByTime(600 * 1000);
    await flush();

    expect(instances.a.releases).toEqual(1);
  });

  test('a swap finishing after the orchestrator left the cache installs nothing', async () => {
    // Released apis then stay unmarked, so only the cache check can catch this
    jest.spyOn(OrchestratorApi.prototype, 'release').mockImplementation(async () => undefined);

    const { core, swapped, request, driver } = createCore();

    const original = await request('a');
    const first = await driver();

    const testing = gate();
    onTestConnection = (d) => (d.builtFrom.password === 'b' ? testing.opened : undefined);

    expect(await request('b')).toBe(original);
    await waitFor(() => built.length === 2);

    await core.releaseConnections();
    testing.open();
    await swapped();

    expect(built[1].releases).toEqual(1);
    expect(await driver()).toBe(first);
    expect(await request('b')).not.toBe(original);
  });

  test('a swap finishing after its orchestrator was released installs nothing', async () => {
    const { swapped, request, driver } = createCore();

    const original = await request('a');
    const first = await driver();

    const testing = gate();
    onTestConnection = (d) => (d.builtFrom.password === 'b' ? testing.opened : undefined);

    expect(await request('b')).toBe(original);
    await waitFor(() => built.length === 2);

    await original.release();
    testing.open();
    await swapped();

    expect(built[1].releases).toEqual(1);
    expect(await driver()).toBe(first);
  });

  test('a version asked for during a swap is swapped to by a request after it', async () => {
    const { swapped, swapTo, request, driver, logged } = createCore();

    await swapTo('a');
    await driver();

    const testing = gate();
    onTestConnection = (d) => (d.builtFrom.password === 'b' ? testing.opened : undefined);

    await request('b');
    await waitFor(() => built.length === 2);
    await request('c');
    await request('d');

    testing.open();
    await swapped();

    expect(await driver()).toMatchObject({ builtFrom: { password: 'b' } });

    await swapTo('d');

    expect(await driver()).toMatchObject({ builtFrom: { password: 'd' } });
    expect(built.map((d) => d.builtFrom.password)).toEqual(['a', 'b', 'd']);
    expect(logged('Driver Swap')).toHaveLength(2);
  });

  test('a failing release does not mask why the new driver was rejected', async () => {
    const { logged, swapTo, driver } = createCore();

    await swapTo('a');
    await driver();

    onTestConnection = (d) => {
      if (d.builtFrom.password === 'b') {
        throw new Error('token expired');
      }
    };
    onRelease = (d) => {
      if (d.builtFrom.password === 'b') {
        throw new Error('pool never connected');
      }
    };

    await swapTo('b');

    expect(logged('Driver Swap Error')).toHaveLength(1);
    expect(logged('Driver Swap Error')[0][1].error).toContain('token expired');
  });

  test('a data source first used after a swap is built from the new context', async () => {
    const { swapTo, driver, driverFactory } = createCore();

    await swapTo('a');
    await driver();
    await swapTo('b');

    const other = await driver('other');

    expect(other.builtFrom).toMatchObject({ password: 'b', dataSource: 'other' });
    expect(driverFactory.mock.calls[2][0]).toMatchObject({ dataSource: 'other', securityContext: { token: 'b' } });
  });

  test('a data source first used during a swap is swapped too', async () => {
    const { swapped, swapTo, request, driver } = createCore();

    await swapTo('a');
    await driver();

    const testing = gate();
    onTestConnection = (d) => (d.builtFrom.password === 'b' ? testing.opened : undefined);

    await request('b');
    await waitFor(() => built.length === 2);

    // Built from the old context, since the swap has not committed yet
    const stale = await driver('other');
    expect(stale.builtFrom).toMatchObject({ password: 'a' });

    testing.open();
    await swapped();

    expect((await driver('other')).builtFrom).toMatchObject({ password: 'b', dataSource: 'other' });
  });

  test('the first request after the readiness probe builds the drivers from its own context, without a swap', async () => {
    const { request, swapped, driver, logged } = createCore();

    const api = await request();

    expect(await request('a')).toBe(api);
    await swapped();

    expect(await driver()).toMatchObject({ builtFrom: { password: 'a' } });
    expect(built).toHaveLength(1);
    expect(logged('Driver Swap')).toHaveLength(0);
  });

  // A factory with a service-credential fallback builds drivers for the readiness probe too
  const probeFallbackFactory = (ctx: any) => ({ type: 'postgres', password: ctx.securityContext?.token ?? 'service' });

  test('a request with a version waits for its own drivers instead of those the probe built', async () => {
    const { request, driver } = createCore({ driverFactory: probeFallbackFactory });

    await request();
    expect(await driver()).toMatchObject({ builtFrom: { password: 'service' } });

    const testing = gate();
    onTestConnection = (d) => (d.builtFrom.password === 'a' ? testing.opened : undefined);

    const pending = request('a');
    await waitFor(() => built.length === 2);
    expect(await Promise.race([pending.then(() => 'served'), flush().then(() => 'waiting')])).toBe('waiting');

    testing.open();
    await pending;

    expect(await driver()).toMatchObject({ builtFrom: { password: 'a' } });
  });

  test('a request waiting on the probe-built drivers stops waiting once a version is installed', async () => {
    const { request, swapTo, driver } = createCore({ driverFactory: probeFallbackFactory });

    await request();
    await driver();

    const toA = gate();
    onTestConnection = (d) => (d.builtFrom.password === 'a' ? toA.opened : undefined);

    const servedA = request('a');
    await waitFor(() => built.length === 2);
    const servedB = request('b');

    toA.open();
    // b was asked for during a's swap, so it is served by a's drivers, not the probe's
    await Promise.all([servedA, servedB]);
    expect(await driver()).toMatchObject({ builtFrom: { password: 'a' } });

    await swapTo('b');
    expect(await driver()).toMatchObject({ builtFrom: { password: 'b' } });
  });

  test('a request whose own first swap fails gets its error, and one asked for meanwhile gets its own swap', async () => {
    const { request, swapped, driver } = createCore({ driverFactory: probeFallbackFactory });

    await request();
    await driver();

    const toA = gate();
    const toB = gate();
    onTestConnection = async (d) => {
      if (d.builtFrom.password === 'a') {
        await toA.opened;
        throw new Error('bad credential a');
      }
      if (d.builtFrom.password === 'b') {
        await toB.opened;
      }
    };

    const servedA = request('a');
    await waitFor(() => built.length === 2);
    const servedB = request('b');

    toA.open();
    await expect(servedA).rejects.toThrow('bad credential a');

    toB.open();
    await servedB;
    await swapped();
    expect(await driver()).toMatchObject({ builtFrom: { password: 'b' } });
  });

  test('a request waiting on another version\'s failed first swap starts none of its own once released', async () => {
    const { core, request, driver } = createCore({ driverFactory: probeFallbackFactory });

    await request();
    await driver();

    const toA = gate();
    onTestConnection = async (d) => {
      if (d.builtFrom.password === 'a') {
        await toA.opened;
        throw new Error('bad credential a');
      }
      // Fails too, so a swap to b on the released orchestrator, if one starts, ends instead of starting again
      if (d.builtFrom.password === 'b') {
        throw new Error('bad credential b');
      }
    };

    const servedA = request('a');
    await waitFor(() => built.length === 2);
    const servedB = request('b');
    expect(await Promise.race([servedB.then(() => 'served'), flush().then(() => 'waiting')])).toBe('waiting');

    await core.releaseConnections();
    toA.open();
    await Promise.all([servedA, servedB]);

    // A swap on the released orchestrator would log in with b's credential only to discard the driver
    expect(built.some((d) => d.builtFrom.password === 'b')).toBe(false);
  });

  test('a retry asked for during another swap is waited for, not failed with the earlier error', async () => {
    let now = Date.parse('2026-01-01T00:00:00Z');
    jest.spyOn(Date, 'now').mockImplementation(() => now);

    const { request, driver } = createCore({ driverFactory: probeFallbackFactory });

    await request();
    await driver();

    let failA = true;
    const toB = gate();
    onTestConnection = async (d) => {
      if (d.builtFrom.password === 'a' && failA) {
        throw new Error('bad credential a');
      }
      if (d.builtFrom.password === 'b') {
        await toB.opened;
      }
    };

    await expect(request('a')).rejects.toThrow('bad credential a');

    now += 11 * 1000;
    failA = false;
    const servedB = request('b');
    await waitFor(() => built.length === 3);
    const servedA = request('a');

    expect(await Promise.race([servedA.then(() => 'served', () => 'failed'), flush().then(() => 'waiting')])).toBe('waiting');

    toB.open();
    await Promise.all([servedA, servedB]);
  });

  test('a version still backing off during another swap keeps its own error', async () => {
    const { request, driver } = createCore({ driverFactory: probeFallbackFactory });

    await request();
    await driver();

    const toB = gate();
    onTestConnection = async (d) => {
      if (d.builtFrom.password === 'c') {
        throw new Error('bad credential c');
      }
      if (d.builtFrom.password === 'b') {
        await toB.opened;
        throw new Error('bad credential b');
      }
    };

    await expect(request('c')).rejects.toThrow('bad credential c');

    const servedB = request('b');
    await waitFor(() => built.length === 3);

    await expect(request('c')).rejects.toThrow('bad credential c');

    toB.open();
    await expect(servedB).rejects.toThrow('bad credential b');
  });

  test('a request with a version fails rather than use the drivers the probe built', async () => {
    const { request, driver } = createCore({ driverFactory: probeFallbackFactory });

    await request();
    const probeDriver = await driver();

    onTestConnection = (d) => {
      if (d.builtFrom.password === 'a') {
        throw new Error('bad credential');
      }
    };

    await expect(request('a')).rejects.toThrow('bad credential');
    // Backing off, it still refuses rather than serve the probe's driver
    await expect(request('a')).rejects.toThrow('bad credential');
    expect(await driver()).toBe(probeDriver);
  });

  test('a throwing logger does not leave a failed swap rejected', async () => {
    const logger = jest.fn((message: string) => {
      if (message === 'Driver Swap Error') {
        throw new Error('logger down');
      }
    });
    const { core, request, swapTo, driver } = createCore({ logger });

    await swapTo('a');
    const first = await driver();

    onTestConnection = (d) => {
      if (d.builtFrom.password === 'b') {
        throw new Error('bad credential');
      }
    };

    await request('b');
    await expect(core.swapsSettled()).resolves.toBeUndefined();
    expect(await driver()).toBe(first);
  });

  test('a driverVersion error fails a request that carries a security context', async () => {
    const { swapTo, request, driver, driverFactory } = createCore({
      driverVersion: (ctx: any) => {
        if (ctx.securityContext.token === 'broken') {
          throw new Error('token store unavailable');
        }

        return ctx.securityContext.token;
      },
    });

    await swapTo('a');
    const first = await driver();

    await expect(request('broken')).rejects.toThrow('token store unavailable');

    expect(await driver()).toBe(first);
    expect(driverFactory).toHaveBeenCalledTimes(1);
  });

  test('a driverVersion error without a security context does not swap or fail the request', async () => {
    const { swapTo, request, driver, driverFactory, logged } = createCore();

    await swapTo('a');
    const first = await driver();

    await expect(request()).resolves.toBeDefined();

    expect(await driver()).toBe(first);
    expect(driverFactory).toHaveBeenCalledTimes(1);
    expect(logged('Driver Version Error')).toHaveLength(1);
  });
});
