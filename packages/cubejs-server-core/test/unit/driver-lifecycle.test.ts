import { BaseDriver } from '@cubejs-backend/query-orchestrator';

import { CubejsServerCore } from '../../src/core/server';
import { OrchestratorApi } from '../../src/core/OrchestratorApi';

class CubejsServerCoreOpen extends CubejsServerCore {
  public getOrchestratorApi = super.getOrchestratorApi;
}

class TestDriver extends BaseDriver {
  public released = 0;

  public tested = 0;

  public constructor(public readonly name: string) {
    super();
  }

  public async testConnection() {
    this.tested += 1;
  }

  public async release() {
    this.released += 1;
  }

  public async query(): Promise<any> {
    return [];
  }
}

const context = { requestId: 'test', authInfo: null, securityContext: null } as any;

const noop = () => {
  // noop
};

describe('driver lifecycle', () => {
  let created: TestDriver[] = [];

  beforeEach(() => {
    created = [];
    process.env.CUBEJS_DB_TYPE = 'postgres';
  });

  afterEach(() => {
    delete process.env.CUBEJS_DB_TYPE;
    delete process.env.CUBEJS_PRE_AGGREGATIONS_DB_HOST;
    delete process.env.CUBEJS_DS_ANALYTICS_PRE_AGGREGATIONS_DB_HOST;
  });

  /**
   * A core whose driver construction is stubbed, so every resolution yields a
   * fresh identifiable driver without needing a database. Going through
   * `resolveDriver` (rather than a `driverFactory` option) keeps the real
   * pre-aggregation-credential code path in play — `isCustomDriverFactory()`
   * would otherwise force `usePreAgg` off.
   */
  const createCore = () => {
    const core = new CubejsServerCoreOpen(<any>{ apiSecret: 'secret', logger: noop });

    (core as any).resolveDriver = async ({ dataSource }: any) => {
      const driver = new TestDriver(dataSource);
      created.push(driver);
      return driver;
    };

    return core;
  };

  const getApi = async (core: CubejsServerCoreOpen): Promise<any> => core.getOrchestratorApi(context);

  /** What the runtime does: the query path resolves one, pre-aggregations the other. */
  const requestQueryAndPreAggDrivers = async (api: any, dataSource = 'default') => {
    await api.driverFactory(dataSource);
    await api.driverFactory(dataSource, true);
  };

  describe('with a dedicated pre-aggregation connection', () => {
    beforeEach(() => {
      process.env.CUBEJS_PRE_AGGREGATIONS_DB_HOST = 'preagg-host';
    });

    // The pre-aggregation driver is cached under a key of its own, which the
    // release path used to have no way of reaching.
    test('releases the pre-aggregation driver, not only the query driver', async () => {
      const api = await getApi(createCore());

      await requestQueryAndPreAggDrivers(api);

      expect(created).toHaveLength(2);

      await api.release();

      expect(created.map(d => d.released)).toEqual([1, 1]);
    });

    test('connection-tests the pre-aggregation driver too', async () => {
      const api = await getApi(createCore());

      await requestQueryAndPreAggDrivers(api);

      created.forEach((driver) => { driver.tested = 0; });

      await api.testConnection();

      expect(created.map(d => d.tested)).toEqual([1, 1]);
    });

    test('releases a named data source pre-aggregation driver', async () => {
      process.env.CUBEJS_DS_ANALYTICS_PRE_AGGREGATIONS_DB_HOST = 'preagg-host';

      const api = await getApi(createCore());

      await requestQueryAndPreAggDrivers(api, 'analytics');

      expect(created).toHaveLength(2);

      await api.release();

      expect(created.map(d => d.released)).toEqual([1, 1]);
    });
  });

  describe('without a dedicated pre-aggregation connection', () => {
    // One connection serves both, so both requests must land on one driver —
    // whichever arrives first. A refresh worker builds pre-aggregations before
    // serving any query, which is the order that used to build two drivers.
    test.each([
      ['query first', [false, true]],
      ['pre-aggregation first', [true, false]],
    ])('builds a single driver (%s)', async (_name, order) => {
      const api = await getApi(createCore());

      for (const preAggregations of order) {
        await api.driverFactory('default', preAggregations);
      }

      expect(created).toHaveLength(1);

      await api.release();

      expect(created[0].released).toEqual(1);
    });

    test('connection-tests the one driver that serves both', async () => {
      const api = await getApi(createCore());

      await requestQueryAndPreAggDrivers(api);

      created.forEach((driver) => { driver.tested = 0; });

      await api.testConnection();

      // One connection, so one round-trip — not one per request that shares it.
      expect(created).toHaveLength(1);
      expect(created[0].tested).toEqual(1);
    });
  });

  // The standalone readiness probe announces a data source and then tests it,
  // precisely so the first connection is opened and verified before traffic
  // arrives. Testing only drivers that already exist would report healthy
  // against an unreachable database.
  // A custom `driverFactory` takes precedence over pre-aggregation env vars, so
  // both requests resolve to the same credentials — and must therefore resolve
  // to one driver. Keying the cache on the caller's flag instead of on the
  // credentials actually used gave this configuration two identical pools.
  describe('with a custom driverFactory overriding pre-aggregation env vars', () => {
    test('builds and releases a single driver', async () => {
      process.env.CUBEJS_PRE_AGGREGATIONS_DB_HOST = 'preagg-host';

      const core = new CubejsServerCoreOpen(<any>{
        apiSecret: 'secret',
        logger: noop,
        driverFactory: () => {
          const driver = new TestDriver(`custom#${created.length + 1}`);
          created.push(driver);
          return driver;
        },
      });

      const api = await getApi(core);

      await requestQueryAndPreAggDrivers(api);

      expect(created).toHaveLength(1);

      await api.release();

      expect(created[0].released).toEqual(1);
    });
  });

  describe('readiness probe on a fresh server', () => {
    test('tests an announced data source that has not been queried yet', async () => {
      const api = await getApi(createCore());

      // Nothing has queried `analytics`; the probe announces it and expects the
      // connection test to force and verify the connection.
      api.addDataSeenSource('analytics');

      await api.testConnection();

      // Resolving it is what opens the connection, and the factory verifies it
      // on construction — so a probe that never resolves it cannot report on it.
      const analyticsDriver = created.find(driver => driver.name === 'analytics');

      expect(analyticsDriver).toBeDefined();
      expect(analyticsDriver!.tested).toBeGreaterThanOrEqual(1);
    });

    // A pre-aggregation build can be the first thing to touch a data source. On
    // dedicated credentials that is a different database, so it is no evidence
    // about the primary connection the probe was announced for.
    test('still tests the primary connection when only the pre-aggregation one was built', async () => {
      process.env.CUBEJS_PRE_AGGREGATIONS_DB_HOST = 'preagg-host';

      const api = await getApi(createCore());

      await api.driverFactory('default', true);

      expect(created).toHaveLength(1);

      api.addDataSeenSource('default');

      await api.testConnection();

      // The query connection had to be built and tested to be reported on.
      expect(created).toHaveLength(2);
      expect(created[1].tested).toBeGreaterThanOrEqual(1);
    });

    test('reports the failure when that connection is unreachable', async () => {
      const core = createCore();
      (core as any).resolveDriver = async () => { throw new Error('connection refused'); };

      const api = await getApi(core);

      api.addDataSeenSource('default');

      await expect(api.testConnection()).rejects.toThrow('connection refused');
    });
  });

  // The record is keyed by what was asked for, which the schema bounds. Keying
  // it by the promise handed back grows it once per call — an `async` factory
  // returns a new promise even when it answers from its own cache — and every
  // connection test then re-requests each entry, so it doubles per probe.
  describe('tracking is bounded', () => {
    test('repeated requests and probes do not accumulate entries', async () => {
      const api = await getApi(createCore());

      for (let i = 0; i < 5; i += 1) {
        await api.driverFactory('default');
      }

      expect(api.requestedDrivers.size).toEqual(1);

      await api.testConnection();
      await api.testConnection();

      expect(api.requestedDrivers.size).toEqual(1);
      expect(created).toHaveLength(1);
    });
  });

  describe('release does not build drivers', () => {
    test('a data source that was never queried is not constructed', async () => {
      const api = await getApi(createCore());

      await api.release();

      expect(created).toHaveLength(0);
    });

    // Releasing used to go back through the factory to obtain the driver it
    // then closed. After a failed resolution the cache entry is empty, so that
    // call would build a fresh driver — and open a connection — purely to
    // close it.
    test('a data source whose driver failed to resolve is not rebuilt', async () => {
      const core = createCore();
      let attempts = 0;

      (core as any).resolveDriver = async () => {
        attempts += 1;
        throw new Error('connection refused');
      };

      const api = await getApi(core);

      await expect(api.driverFactory('default')).rejects.toThrow('connection refused');
      expect(attempts).toEqual(1);

      // Announced as well, so the old release path — which went back through the
      // factory for every seen data source — would resolve it a second time and
      // build the driver it meant to close.
      api.addDataSeenSource('default');

      await api.release();

      expect(attempts).toEqual(1);
      expect(created).toHaveLength(0);
    });

    // Teardown runs to completion, then reports: shutdown exits non-zero on a
    // failed release, and a reset must not reload over state it could not free.
    test('one driver refusing to close does not strand the others', async () => {
      process.env.CUBEJS_PRE_AGGREGATIONS_DB_HOST = 'preagg-host';

      const api = await getApi(createCore());

      await requestQueryAndPreAggDrivers(api);

      created[0].release = async () => { throw new Error('release failed'); };

      await expect(api.release()).rejects.toThrow('release failed');

      expect(created[1].released).toEqual(1);
    });
  });

  describe('rollupOnlyMode', () => {
    test('tests only the external driver', async () => {
      const externalDriver = new TestDriver('external');
      const api = new OrchestratorApi(
        (async () => new TestDriver('internal')) as any,
        noop,
        <any>{
          rollupOnlyMode: true,
          externalDriverFactory: async () => externalDriver,
          contextToDbType: async () => 'postgres',
          contextToExternalDbType: () => 'cubestore',
        },
      );

      await api.testConnection();

      expect(externalDriver.tested).toEqual(1);
    });
  });
});
