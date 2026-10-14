import { BaseDriver } from '@cubejs-backend/query-orchestrator';
import type { DriverFactoryByDataSource } from '@cubejs-backend/query-orchestrator';

import { CubejsServerCore } from '../../src';
import { OrchestratorApi, OrchestratorApiOptions } from '../../src/core/OrchestratorApi';

let onTestConnection: (driver: FakeDriver) => Promise<void> | void = () => undefined;

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
  }
}

class TestServerCore extends CubejsServerCore {
  public readonly driverFactories: DriverFactoryByDataSource[] = [];

  protected createOrchestratorApi(getDriver: DriverFactoryByDataSource, options: OrchestratorApiOptions): OrchestratorApi {
    this.driverFactories.push(getDriver);

    return super.createOrchestratorApi(getDriver, options);
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

  return {
    core,
    driverFactory,
    // No token stands for the readiness probe, which passes an empty context
    request: (token?: string) => core.getOrchestratorApi(
      <any>(token === undefined ? {} : { requestId: `request-${token}`, securityContext: { token } })
    ),
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

  test('swaps to drivers built from the new context once tested, and not for an unchanged version', async () => {
    const { request, driver, driverFactory } = createCore();

    await request('a');
    const first = await driver();
    await request('a');

    expect(await driver()).toBe(first);
    expect(driverFactory).toHaveBeenCalledTimes(1);

    const testing = gate();
    onTestConnection = (d) => (d.builtFrom.password === 'b' ? testing.opened : undefined);

    const swapping = request('b');
    await waitFor(() => built.length === 2);

    expect(driverFactory.mock.calls[1][0].securityContext).toEqual({ token: 'b' });
    expect(built[1].tests).toEqual(1);
    // Still under test, so queries must keep getting the old driver
    expect(await Promise.race([driver(), flush().then(() => 'not ready')])).toBe(first);

    testing.open();
    await swapping;

    expect(await driver()).toBe(built[1]);
    expect(built[1].builtFrom).toMatchObject({ password: 'b' });
    expect(first.releases).toEqual(0);
  });

  test('concurrent requests with one new version share a single swap', async () => {
    const { request, driver, driverFactory } = createCore();

    await request('a');
    await driver();

    const apis = await Promise.all(Array.from({ length: 5 }, () => request('b')));

    expect(new Set(apis).size).toEqual(1);
    expect(driverFactory).toHaveBeenCalledTimes(2);
    expect(await driver()).toBe(built[1]);
  });

  test('a failed swap keeps the old drivers and backs off before retrying', async () => {
    let now = Date.parse('2026-01-01T00:00:00Z');
    jest.spyOn(Date, 'now').mockImplementation(() => now);

    const { request, driver, driverFactory, logged } = createCore();

    await request('a');
    const first = await driver();

    onTestConnection = (d) => {
      if (d.builtFrom.password === 'b') {
        throw new Error('bad credential');
      }
    };

    await expect(request('b')).rejects.toThrow('bad credential');

    expect(await driver()).toBe(first);
    expect(first.releases).toEqual(0);
    expect(built[1].releases).toEqual(1);

    // The old version stands, so its own requests do not swap back
    await request('a');
    expect(driverFactory).toHaveBeenCalledTimes(2);

    now += 9 * 1000;
    await expect(request('b')).resolves.toBeDefined();
    await expect(request('b')).resolves.toBeDefined();

    expect(driverFactory).toHaveBeenCalledTimes(2);
    expect(logged('Driver Swap Backoff')).toHaveLength(1);
    expect(await driver()).toBe(first);

    onTestConnection = () => undefined;
    now += 1000;
    await request('b');

    expect(driverFactory).toHaveBeenCalledTimes(3);
    expect(await driver()).toBe(built[2]);
  });

  const releaseDelays: Array<[string, Record<string, unknown>, string | undefined, number]> = [
    ['the default', {}, undefined, 600],
    ['driverSwapReleaseDelay', { driverSwapReleaseDelay: 30 }, undefined, 30],
    ['CUBEJS_DRIVER_SWAP_RELEASE_DELAY', {}, '45', 45],
  ];

  test.each(releaseDelays)('releases a replaced driver after %s delay', async (_name, options, env, seconds) => {
    jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate'] });

    const previousEnv = process.env.CUBEJS_DRIVER_SWAP_RELEASE_DELAY;

    if (env !== undefined) {
      process.env.CUBEJS_DRIVER_SWAP_RELEASE_DELAY = env;
    }

    try {
      const { request, driver } = createCore(options);

      await request('a');
      const first = await driver();
      await request('b');

      jest.advanceTimersByTime(seconds * 1000 - 1);
      await flush();
      expect(first.releases).toEqual(0);

      jest.advanceTimersByTime(1);
      await flush();
      expect(first.releases).toEqual(1);
      expect(built[1].releases).toEqual(0);
    } finally {
      if (previousEnv === undefined) {
        delete process.env.CUBEJS_DRIVER_SWAP_RELEASE_DELAY;
      } else {
        process.env.CUBEJS_DRIVER_SWAP_RELEASE_DELAY = previousEnv;
      }
    }
  });

  test('releaseConnections releases replaced drivers without waiting for the delay', async () => {
    const { core, request, driver } = createCore();

    await request('a');
    const first = await driver();
    await request('b');

    expect(first.releases).toEqual(0);

    await core.releaseConnections();

    expect(first.releases).toEqual(1);
  });

  test('a data source and its pre-aggregation alias are swapped together', async () => {
    const { request, driver, driverFactory } = createCore();

    await request('a');
    const first = await driver();
    expect(await driver('default', true)).toBe(first);

    await request('b');

    expect(driverFactory).toHaveBeenCalledTimes(2);
    expect(await driver()).toBe(built[1]);
    expect(await driver('default', true)).toBe(built[1]);
  });

  test('a factory returning constructed drivers is swapped', async () => {
    const { request, driver } = createCore({
      driverFactory: (ctx: any) => new FakeDriver({ password: ctx.securityContext.token }),
    });

    await request('a');
    const first = await driver();
    await request('b');
    const second = await driver();

    expect(second).not.toBe(first);
    expect(second.builtFrom).toEqual({ password: 'b' });
    expect(second.tests).toEqual(1);
  });

  test('a driver the factory hands out again is not released as replaced', async () => {
    const shared = new FakeDriver({ password: 'shared' });
    const { core, request, driver } = createCore({ driverFactory: () => shared });

    await request('a');
    await driver();
    await request('b');

    expect(await driver()).toBe(shared);

    await core.releaseConnections();

    expect(shared.releases).toEqual(0);
  });

  test('a data source first used after a swap is built from the new context', async () => {
    const { request, driver, driverFactory } = createCore();

    await request('a');
    await driver();
    await request('b');

    const other = await driver('other');

    expect(other.builtFrom).toMatchObject({ password: 'b', dataSource: 'other' });
    expect(driverFactory.mock.calls[2][0]).toMatchObject({ dataSource: 'other', securityContext: { token: 'b' } });
  });

  test('a data source first used during a swap is swapped too', async () => {
    const { request, driver } = createCore();

    await request('a');
    await driver();

    const testing = gate();
    onTestConnection = (d) => (d.builtFrom.password === 'b' ? testing.opened : undefined);

    const swapping = request('b');
    await waitFor(() => built.length === 2);

    // Built from the old context, since the swap has not committed yet
    const stale = await driver('other');
    expect(stale.builtFrom).toMatchObject({ password: 'a' });

    testing.open();
    await swapping;

    expect((await driver('other')).builtFrom).toMatchObject({ password: 'b', dataSource: 'other' });
  });

  test('swaps closer together than 5s are reported as ping-pong', async () => {
    let now = Date.parse('2026-01-01T00:00:00Z');
    jest.spyOn(Date, 'now').mockImplementation(() => now);

    const { request, driver, logged } = createCore();

    await request('a');
    await driver();

    await request('b');
    now += 5 * 1000;
    await request('a');

    expect(logged('Driver Swap Ping-Pong')).toHaveLength(0);

    now += 4 * 1000;
    await request('b');

    expect(logged('Driver Swap Ping-Pong')).toHaveLength(1);
    expect(await driver()).toMatchObject({ builtFrom: { password: 'b' } });
  });

  test('a throwing driverVersion does not swap and does not fail the request', async () => {
    const { request, driver, driverFactory, logged } = createCore();

    await request('a');
    const first = await driver();

    await expect(request()).resolves.toBeDefined();

    expect(await driver()).toBe(first);
    expect(driverFactory).toHaveBeenCalledTimes(1);
    expect(logged('Driver Version Error')).toHaveLength(1);
  });
});
