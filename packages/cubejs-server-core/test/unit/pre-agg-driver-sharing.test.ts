/* eslint-disable @typescript-eslint/no-explicit-any */
import type { BaseDriver } from '@cubejs-backend/query-orchestrator';

import { CubejsServerCore } from '../../src/core/server';

/**
 * Whether a pre-aggregation build gets its own driver, or reuses the query
 * driver, decides which preamble the build actually runs — and the version key
 * records the pre-aggregation one either way. Get the sharing wrong and a
 * pre-aggregation is keyed on a preamble that never ran, which is exactly the
 * pair of bugs this branch fixed (the driver resolved the regular preamble for
 * builds, and separately the two factory keys aliased to one promise whenever
 * the API happened to ask first — order-dependent, so intermittent).
 *
 * The preamble is deliberately NOT a credential: it is excluded from
 * `hasPreAggregationsEnvVars`, so setting one alone must not swing credentials
 * into the pre-aggregation namespace. That exclusion is what makes a separate
 * check necessary here — without it the keys would share a driver.
 *
 * These tests drive the real factory closure from `getOrchestratorApi` rather
 * than re-deriving the predicate: `createOrchestratorApi` receives it as its
 * first argument, so capturing it there is the seam.
 */
type ResolveCall = { dataSource?: string, preAggregations?: boolean, preAggregationsSqlPreamble?: boolean };

class ServerCoreOpen extends CubejsServerCore {
  public capturedFactory!: (dataSource?: string, preAggregations?: boolean) => Promise<BaseDriver>;

  public readonly resolveCalls: ResolveCall[] = [];

  public constructor() {
    // Same construction rules as the warning suite: no `dbType` (removed in
    // v1.7.0, now throws) and no refresh timer (a live scheduler outlives the
    // suite and crashes the worker on the way out).
    super({ apiSecret: 'secret', scheduledRefreshTimer: false } as any);
    this.logger = (() => { /* silence the conflict warning */ }) as any;
  }

  protected createOrchestratorApi(getDriver: any, options: any): any {
    this.capturedFactory = getDriver;
    return super.createOrchestratorApi(getDriver, options);
  }

  // Stand in for a real driver so the factory's testConnection/release path is
  // satisfied; each call returns a distinct object so identity tells us whether
  // two factory keys resolved to the same driver.
  public async resolveDriver(context: any): Promise<BaseDriver> {
    this.resolveCalls.push({
      dataSource: context.dataSource,
      preAggregations: context.preAggregations,
      preAggregationsSqlPreamble: context.preAggregationsSqlPreamble,
    });

    return {
      testConnection: async () => undefined,
      release: async () => undefined,
      setLogger: () => undefined,
    } as unknown as BaseDriver;
  }
}

const PREAMBLE_KEYS = [
  'CUBEJS_DB_SQL_PREAMBLE',
  'CUBEJS_PRE_AGGREGATIONS_DB_SQL_PREAMBLE',
];

describe('pre-aggregation driver sharing and the SQL preamble', () => {
  let core: ServerCoreOpen;

  const clearEnv = () => PREAMBLE_KEYS.forEach(k => delete process.env[k]);

  beforeEach(async () => {
    clearEnv();
    core = new ServerCoreOpen();
    // Building the orchestrator is what constructs (and hands us) the factory.
    await core.getOrchestratorApi({ requestId: 'test' } as any);
  });

  afterEach(async () => {
    clearEnv();
    await core.shutdown();
  });

  const queryDriver = () => core.capturedFactory('default', false);
  const buildDriver = () => core.capturedFactory('default', true);

  test('with no preamble at all, a build reuses the query driver', async () => {
    const forQuery = await queryDriver();
    const forBuild = await buildDriver();

    expect(forBuild).toBe(forQuery);
    expect(core.resolveCalls).toHaveLength(1);
  });

  test('with one preamble for both, a build still reuses the query driver', async () => {
    process.env.CUBEJS_DB_SQL_PREAMBLE = 'SET a = 1';

    const forQuery = await queryDriver();
    const forBuild = await buildDriver();

    // The build runs the same preamble, so a second connection pool would buy
    // nothing.
    expect(forBuild).toBe(forQuery);
    expect(core.resolveCalls).toHaveLength(1);
  });

  // The regression the branch fixed. Asking query-first is the order that used
  // to alias the two keys onto one promise.
  test('a differing pre-aggregation preamble gives the build its own driver', async () => {
    process.env.CUBEJS_DB_SQL_PREAMBLE = 'SET a = 1';
    process.env.CUBEJS_PRE_AGGREGATIONS_DB_SQL_PREAMBLE = 'SET a = 2';

    const forQuery = await queryDriver();
    const forBuild = await buildDriver();

    expect(forBuild).not.toBe(forQuery);
    expect(core.resolveCalls).toHaveLength(2);
  });

  // The reverse order, for completeness. Note this direction cannot regress the
  // same way: the aliasing only ever happens on the query-path branch, so a
  // build that asks first gets its own promise no matter what the predicate
  // says. It is here to pin that asymmetry, not as a second guard on the
  // predicate — the query-first test above is the one that fails if the
  // preamble term is dropped.
  test('...and the build still gets its own driver when it asks first', async () => {
    process.env.CUBEJS_DB_SQL_PREAMBLE = 'SET a = 1';
    process.env.CUBEJS_PRE_AGGREGATIONS_DB_SQL_PREAMBLE = 'SET a = 2';

    const forBuild = await buildDriver();
    const forQuery = await queryDriver();

    expect(forBuild).not.toBe(forQuery);
    expect(core.resolveCalls).toHaveLength(2);
  });

  // A build-only preamble differs from the (absent) query one, so it also earns
  // its own driver — and must not drag credentials into the pre-agg namespace.
  test('a build-only preamble separates the drivers without swinging credentials', async () => {
    process.env.CUBEJS_PRE_AGGREGATIONS_DB_SQL_PREAMBLE = 'SET a = 2';

    const forQuery = await queryDriver();
    const forBuild = await buildDriver();

    expect(forBuild).not.toBe(forQuery);

    const build = core.resolveCalls.find(c => c.preAggregationsSqlPreamble);
    expect(build).toBeDefined();
    // "This driver serves builds" is true...
    expect(build!.preAggregationsSqlPreamble).toBe(true);
    // ...while "resolve credentials from the pre-aggregation namespace" is not.
    // Collapsing these two into one flag is what left builds without a host.
    expect(build!.preAggregations).toBe(false);
  });

  // Normalization has to happen on both sides of the comparison, or a re-indented
  // value splits the pool for no behavioural difference.
  test('a merely re-indented preamble is the same preamble', async () => {
    process.env.CUBEJS_DB_SQL_PREAMBLE = 'SET a = 1';
    process.env.CUBEJS_PRE_AGGREGATIONS_DB_SQL_PREAMBLE = '  SET a = 1\n';

    const forQuery = await queryDriver();
    const forBuild = await buildDriver();

    expect(forBuild).toBe(forQuery);
    expect(core.resolveCalls).toHaveLength(1);
  });
});
