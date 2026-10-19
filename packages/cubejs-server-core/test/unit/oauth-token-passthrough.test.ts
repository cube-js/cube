import { BaseDriver } from '@cubejs-backend/query-orchestrator';
import type { DriverFactoryByDataSource } from '@cubejs-backend/query-orchestrator';

import { CubejsServerCore } from '../../src';
import { OrchestratorApi, OrchestratorApiOptions } from '../../src/core/OrchestratorApi';

class FakeDriver extends BaseDriver {
  /** Every call, in order: `test`, or the token handed over (`-` for none). */
  public readonly events: string[] = [];

  public constructor(public readonly builtFrom: Record<string, unknown> = {}) {
    super({});
  }

  public async testConnection(): Promise<void> {
    this.events.push('test');
  }

  public async query<R = unknown>(): Promise<R[]> {
    return [];
  }

  public override setOAuthToken(token: string | undefined): void {
    this.events.push(`token:${token ?? '-'}`);
  }

  public get tokens(): string[] {
    return this.events.filter((event) => event.startsWith('token:')).map((event) => event.slice('token:'.length));
  }

  public get tests(): number {
    return this.events.filter((event) => event === 'test').length;
  }
}

class NoPassthroughDriver extends BaseDriver {
  public async testConnection(): Promise<void> {
    return undefined;
  }

  public async query<R = unknown>(): Promise<R[]> {
    return [];
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
  const contextToOAuthToken = jest.fn((ctx: any) => ctx.securityContext?.token);
  const core = new TestServerCore(<any>{
    apiSecret: 'secret',
    contextToOrchestratorId: () => 'ORCHESTRATOR_ID',
    driverFactory: () => ({ type: 'postgres' }),
    contextToOAuthToken,
    ...options,
  });

  cores.push(core);

  return {
    core,
    contextToOAuthToken,
    // No token stands for a context without a user, such as the readiness probe's
    request: (token?: string) => core.getOrchestratorApi(
      <any>(token === undefined ? { securityContext: {} } : { requestId: `request-${token}`, securityContext: { token } })
    ),
    driver: async (dataSource = 'default', preAggregations = false) => <FakeDriver>(
      await core.driverFactories[0](dataSource, preAggregations)
    ),
  };
}

describe('OAuth token passthrough', () => {
  let created: FakeDriver[];

  beforeEach(() => {
    created = [];
    jest.spyOn(CubejsServerCore, 'createDriver').mockImplementation((_type, options) => {
      const driver = new FakeDriver(<any>options);
      created.push(driver);

      return driver;
    });
  });

  afterEach(async () => {
    await Promise.all(cores.splice(0).map((core) => core.releaseConnections()));
    delete process.env.CUBEJS_DB_OAUTH_TOKEN_PASSTHROUGH;
    jest.restoreAllMocks();
  });

  test('hands the driver the token before testing it, then the newest token on each lookup', async () => {
    process.env.CUBEJS_DB_OAUTH_TOKEN_PASSTHROUGH = 'true';
    const { request, driver } = createCore();

    await request('a');
    const first = await driver();

    expect(first.events).toEqual(['token:a', 'test']);

    await request('b');
    const second = await driver();

    expect(second).toBe(first);
    expect(first.tokens).toEqual(['a', 'b']);
  });

  test('a context without a token puts the driver back on its own credentials, even after a token', async () => {
    process.env.CUBEJS_DB_OAUTH_TOKEN_PASSTHROUGH = 'true';
    const { request, driver, contextToOAuthToken } = createCore();

    await request('a');
    await driver();
    await request();

    expect((await driver()).tokens).toEqual(['a', '-']);
    expect(contextToOAuthToken).toHaveBeenCalledWith(expect.objectContaining({ dataSource: 'default' }));
  });

  test('is off by default: the token hook is never called', async () => {
    const { request, driver, contextToOAuthToken } = createCore();

    await request('a');

    expect((await driver()).tokens).toEqual([]);
    expect(contextToOAuthToken).not.toHaveBeenCalled();
  });

  test('a driverFactory turns it on or off per data source, over the env', async () => {
    process.env.CUBEJS_DB_OAUTH_TOKEN_PASSTHROUGH = 'true';
    const { request, driver } = createCore({
      driverFactory: ({ dataSource }: any) => ({ type: 'postgres', oauthTokenPassthrough: dataSource === 'users' }),
    });

    await request('a');

    expect((await driver('users')).tokens).toEqual(['a']);
    expect((await driver('default')).tokens).toEqual([]);
    expect((await driver('users')).builtFrom).not.toHaveProperty('oauthTokenPassthrough');
  });

  const preAggregationSetups: Array<[string, Record<string, unknown>]> = [
    ['the env', {}],
    ['a driverFactory', { driverFactory: () => ({ type: 'postgres', oauthTokenPassthrough: true }) }],
  ];

  test.each(preAggregationSetups)('with passthrough on through %s, pre-aggregations get their own driver and no token', async (_name, options) => {
    if (!('driverFactory' in options)) {
      process.env.CUBEJS_DB_OAUTH_TOKEN_PASSTHROUGH = 'true';
    }
    const { request, driver } = createCore(options);

    await request('a');
    const regular = await driver();
    const preAggregations = await driver('default', true);

    expect(preAggregations).not.toBe(regular);
    expect(await driver('default', true)).toBe(preAggregations);
    expect(regular.tokens).toEqual(['a']);
    expect(preAggregations.tokens).toEqual([]);
  });

  test('pre-aggregation lookups that come first build one driver, not one per lookup', async () => {
    const { request, driver } = createCore({
      driverFactory: () => ({ type: 'postgres', oauthTokenPassthrough: true }),
    });

    await request('a');
    const first = await driver('default', true);

    expect(await driver('default', true)).toBe(first);
    expect(created).toEqual([first]);
    expect(first.tokens).toEqual([]);
  });

  test('refuses a pre-aggregation lookup of a shared driver that holds a token', async () => {
    process.env.CUBEJS_DB_OAUTH_TOKEN_PASSTHROUGH = 'true';
    const shared = new FakeDriver();
    const { request, driver } = createCore({ driverFactory: () => shared });

    await request('a');
    await driver();

    await expect(driver('default', true)).rejects.toThrow(/can't be built with a passed-through OAuth token/);
  });

  test('health checks skip a driver while it holds a token, and test it on its own credentials otherwise', async () => {
    process.env.CUBEJS_DB_OAUTH_TOKEN_PASSTHROUGH = 'true';
    const { request, driver } = createCore();

    const userApi = await request('a');
    const passthrough = await driver();
    userApi.addDataSeenSource('default');

    await userApi.testConnection();
    expect(passthrough.tests).toEqual(1);

    // The readiness probe's own context carries no token
    const probeApi = await request();
    await probeApi.testConnection();

    expect(passthrough.tests).toEqual(2);
    expect(passthrough.tokens).toEqual(['a', 'a', '-']);
  });

  test('fails a lookup whose token hook throws, rather than use the driver\'s own credentials', async () => {
    process.env.CUBEJS_DB_OAUTH_TOKEN_PASSTHROUGH = 'true';
    const { request, driver } = createCore({
      contextToOAuthToken: () => {
        throw new Error('no token for this user');
      },
    });

    await request('a');

    await expect(driver()).rejects.toThrow('no token for this user');
  });

  const misconfigurations: Array<[string, Record<string, unknown>, RegExp]> = [
    ['contextToOAuthToken', { contextToOAuthToken: undefined }, /contextToOAuthToken is not configured/],
    ['contextToOrchestratorId', { contextToOrchestratorId: undefined }, /contextToOrchestratorId is not configured/],
    ['a driver that supports it', { driverFactory: () => new NoPassthroughDriver() }, /NoPassthroughDriver does not support it/],
  ];

  test.each(misconfigurations)('refuses passthrough without %s', async (_name, options, error) => {
    process.env.CUBEJS_DB_OAUTH_TOKEN_PASSTHROUGH = 'true';
    const { request, driver } = createCore(options);

    await request('a');

    await expect(driver()).rejects.toThrow(error);
  });
});
