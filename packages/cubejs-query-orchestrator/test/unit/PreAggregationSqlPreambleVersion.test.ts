/* eslint-disable @typescript-eslint/no-explicit-any */
// Real behaviour throughout, except that `getEnv` can be made to fail once — the
// only way to exercise a non-`assertDataSource` failure inside the env lookup.
jest.mock('@cubejs-backend/shared', () => {
  const actual = jest.requireActual('@cubejs-backend/shared');

  return { ...actual, getEnv: jest.fn(actual.getEnv) };
});

// eslint-disable-next-line import/first
import {
  getPreAggregationSqlPreamble,
  getStructureVersion,
  PreAggregationLoadCache,
  PreAggregationLoader,
  PreAggregations,
  QueryCache,
} from '../../src';

const basePreAggregation = () => ({
  dataSource: 'default',
  loadSql: ['CREATE TABLE stb_pre_aggregations.orders AS SELECT * FROM orders', []],
  indexesSql: [],
  preAggregationsSchema: 'stb_pre_aggregations',
});

describe('pre-aggregation SQL preamble in the version key', () => {
  beforeEach(() => {
    delete process.env.CUBEJS_DB_SQL_PREAMBLE;
    delete process.env.CUBEJS_PRE_AGGREGATIONS_DB_SQL_PREAMBLE;
    delete process.env.CUBEJS_DATASOURCES;
    delete process.env.CUBEJS_DS_ANALYTICS_DB_SQL_PREAMBLE;
  });

  afterEach(() => {
    delete process.env.CUBEJS_DB_SQL_PREAMBLE;
    delete process.env.CUBEJS_PRE_AGGREGATIONS_DB_SQL_PREAMBLE;
    delete process.env.CUBEJS_DATASOURCES;
    delete process.env.CUBEJS_DS_ANALYTICS_DB_SQL_PREAMBLE;
  });

  test('no preamble configured leaves the structure version unchanged', () => {
    // The baseline every existing deployment is on: adding this feature must not
    // re-key pre-aggregations for anyone who never sets a preamble.
    const withoutPreamble = getStructureVersion(basePreAggregation());

    process.env.CUBEJS_DB_SQL_PREAMBLE = '';
    expect(getStructureVersion(basePreAggregation())).toEqual(withoutPreamble);

    // A whitespace-only value is "no preamble" to every driver, so it must not
    // re-key either — a trailing newline out of a ConfigMap would otherwise
    // rebuild every pre-aggregation on upgrade.
    process.env.CUBEJS_DB_SQL_PREAMBLE = '   \n  ';
    expect(getStructureVersion(basePreAggregation())).toEqual(withoutPreamble);
  });

  test('reformatting a preamble does not change the structure version', () => {
    process.env.CUBEJS_DB_SQL_PREAMBLE = 'SET a = 1';
    const tight = getStructureVersion(basePreAggregation());

    process.env.CUBEJS_DB_SQL_PREAMBLE = '  SET a = 1\n';
    expect(getStructureVersion(basePreAggregation())).toEqual(tight);
  });

  test('an undeclared data source yields no preamble instead of throwing', () => {
    // This runs inside version computation and inside the builder for the
    // "no partitions were built" message; throwing would swap an actionable
    // error for a confusing one about CUBEJS_DATASOURCES.
    process.env.CUBEJS_DATASOURCES = 'default,analytics';
    process.env.CUBEJS_DB_SQL_PREAMBLE = 'SET a = 1';
    const undeclared = { ...basePreAggregation(), dataSource: 'nope' };

    expect(() => getPreAggregationSqlPreamble(undeclared)).not.toThrow();
    expect(getPreAggregationSqlPreamble(undeclared)).toBeUndefined();
    expect(() => getStructureVersion(undeclared)).not.toThrow();
  });

  // Swallowing every failure would drop the preamble from the key for reasons
  // that are not the undeclared-data-source case, and a key without it serves
  // tables built under a different preamble — the wrongness the key prevents.
  test('a failure other than an undeclared data source propagates', () => {
    // Any failure inside the env lookup that is NOT `assertDataSource` rejecting
    // an undeclared data source. Swallowing it would drop the preamble from the
    // key and serve tables built under a different one.
    process.env.CUBEJS_DB_SQL_PREAMBLE = 'SET a = 1';
    const shared = jest.requireMock('@cubejs-backend/shared');
    shared.getEnv.mockImplementationOnce(() => {
      throw new Error('something else went wrong entirely');
    });

    expect(() => getPreAggregationSqlPreamble({ ...basePreAggregation(), dataSource: 'default' }))
      .toThrow('something else went wrong entirely');
  });

  // The key participates for every driver, but only eight drivers apply the
  // option. On any other data source a preamble is a no-op that still rebuilds
  // every pre-aggregation — cost with no effect, and previously no signal.
  describe('the unsupported-driver warning', () => {
    let warn: jest.SpyInstance;

    beforeEach(() => {
      warn = jest.spyOn(console, 'warn').mockImplementation(() => { /* captured */ });
      delete process.env.CUBEJS_DB_TYPE;
    });

    afterEach(() => {
      warn.mockRestore();
      delete process.env.CUBEJS_DB_TYPE;
    });

    // Each case needs its own data source: the warning is emitted once per
    // data source, so reusing one would let an earlier case suppress a later.
    const forDbType = (dbType: string, dataSource: string) => {
      process.env.CUBEJS_DB_TYPE = dbType;
      process.env.CUBEJS_DB_SQL_PREAMBLE = 'SET a = 1';

      return getPreAggregationSqlPreamble({ ...basePreAggregation(), dataSource });
    };

    test('warns when the driver does not apply the preamble', () => {
      expect(forDbType('clickhouse', 'ds_clickhouse')).toEqual('SET a = 1');
      expect(warn).toHaveBeenCalledTimes(1);
      expect(warn.mock.calls[0][0]).toContain('clickhouse');
      expect(warn.mock.calls[0][0]).toContain('does not apply it');
    });

    test('stays quiet for a driver that applies it', () => {
      expect(forDbType('bigquery', 'ds_bigquery')).toEqual('SET a = 1');
      expect(warn).not.toHaveBeenCalled();
    });

    // The version functions run on every query, so an unconditional log floods.
    test('warns once per data source, not once per call', () => {
      forDbType('clickhouse', 'ds_repeat');
      forDbType('clickhouse', 'ds_repeat');
      forDbType('clickhouse', 'ds_repeat');

      expect(warn).toHaveBeenCalledTimes(1);
    });

    test('stays quiet when no preamble is configured', () => {
      process.env.CUBEJS_DB_TYPE = 'clickhouse';
      getPreAggregationSqlPreamble({ ...basePreAggregation(), dataSource: 'ds_nopreamble' });

      expect(warn).not.toHaveBeenCalled();
    });

    test('stays quiet when no db type is declared to check against', () => {
      process.env.CUBEJS_DB_SQL_PREAMBLE = 'SET a = 1';
      getPreAggregationSqlPreamble({ ...basePreAggregation(), dataSource: 'ds_nodbtype' });

      expect(warn).not.toHaveBeenCalled();
    });
  });

  test('setting a preamble changes the structure version', () => {
    const before = getStructureVersion(basePreAggregation());

    process.env.CUBEJS_DB_SQL_PREAMBLE = 'CREATE TEMP FUNCTION median(x FLOAT64) AS (x)';
    const after = getStructureVersion(basePreAggregation());

    expect(after).not.toEqual(before);
  });

  test('changing the preamble changes the structure version again', () => {
    process.env.CUBEJS_DB_SQL_PREAMBLE = 'CREATE TEMP FUNCTION median(x FLOAT64) AS (x)';
    const first = getStructureVersion(basePreAggregation());

    process.env.CUBEJS_DB_SQL_PREAMBLE = 'CREATE TEMP FUNCTION median(x FLOAT64) AS (x * 2)';
    const second = getStructureVersion(basePreAggregation());

    expect(second).not.toEqual(first);
  });

  test('the pre-agg-specific preamble is what the version keys on', () => {
    process.env.CUBEJS_DB_SQL_PREAMBLE = 'SET regular = 1';
    const inherited = getStructureVersion(basePreAggregation());

    process.env.CUBEJS_PRE_AGGREGATIONS_DB_SQL_PREAMBLE = 'SET preagg = 1';
    const explicit = getStructureVersion(basePreAggregation());

    // Builds run under the pre-agg preamble, so that is the value that must key
    // the table — not the query-path one it overrides.
    expect(explicit).not.toEqual(inherited);
    expect(getPreAggregationSqlPreamble(basePreAggregation())).toEqual('SET preagg = 1');
  });

  test('the pre-agg preamble inherits the default when unset', () => {
    process.env.CUBEJS_DB_SQL_PREAMBLE = 'SET regular = 1';

    expect(getPreAggregationSqlPreamble(basePreAggregation())).toEqual('SET regular = 1');
  });

  test('the preamble is resolved per data source', () => {
    process.env.CUBEJS_DATASOURCES = 'default,analytics';
    process.env.CUBEJS_DB_SQL_PREAMBLE = 'SET regular = 1';
    process.env.CUBEJS_DS_ANALYTICS_DB_SQL_PREAMBLE = 'SET analytics = 1';

    expect(getPreAggregationSqlPreamble({ ...basePreAggregation(), dataSource: 'analytics' }))
      .toEqual('SET analytics = 1');
    expect(getStructureVersion({ ...basePreAggregation(), dataSource: 'analytics' }))
      .not.toEqual(getStructureVersion(basePreAggregation()));
  });

  test('a description with no dataSource resolves the default preamble', () => {
    // QueryOrchestrator defaults dataSource to 'default' before computing
    // structure versions for the pre-aggregations listing; a description that
    // reaches here without one must not throw or silently skip the preamble.
    process.env.CUBEJS_DB_SQL_PREAMBLE = 'SET regular = 1';
    const { dataSource: _dataSource, ...withoutDataSource } = basePreAggregation();

    expect(getPreAggregationSqlPreamble(withoutDataSource)).toEqual('SET regular = 1');
  });

  test('PreAggregations.structureVersion agrees with the loader path', () => {
    // The pre-aggregations API listing filters version entries by comparing its
    // own structure version against the built table's. If this static and
    // getStructureVersion disagreed, the listing would stop matching real tables.
    process.env.CUBEJS_DB_SQL_PREAMBLE = 'CREATE TEMP FUNCTION median(x FLOAT64) AS (x)';
    const preAggregation = basePreAggregation();

    expect(PreAggregations.structureVersion(preAggregation)).toEqual(getStructureVersion(preAggregation));
  });

  describe('contentVersion', () => {
    // The content version is a separate array from the structure version, and it
    // is the one the non-waitForRenew path compares. A preamble change that only
    // moved the structure version would still serve the stale table there, so
    // this half needs its own coverage.
    const buildLoader = () => {
      const driver: any = {
        query: async () => [],
        tablesSchema: async () => ({}),
      };
      const driverFactory = async () => driver;
      const queryCache: any = new QueryCache(
        'TEST',
        driverFactory as any,
        () => {
          /* no logging in tests */
        },
        {
          cacheAndQueueDriver: 'memory',
          queueOptions: async () => ({ executionTimeout: 1, concurrency: 2 }),
        },
      );
      const preAggregations: any = new PreAggregations(
        'TEST',
        driverFactory as any,
        () => {
          /* no logging in tests */
        },
        queryCache,
        { queueOptions: async () => ({ executionTimeout: 1, concurrency: 2 }) },
      );
      const loadCache: any = new PreAggregationLoadCache(
        driverFactory as any,
        queryCache,
        preAggregations,
        { dataSource: 'default' },
      );

      return new PreAggregationLoader(
        driverFactory as any,
        () => {
          /* no logging in tests */
        },
        queryCache,
        preAggregations,
        basePreAggregation(),
        [],
        loadCache,
        { requestId: 'content-version' },
      ) as any;
    };

    test('setting a preamble changes the content version', () => {
      const loader = buildLoader();
      const before = loader.contentVersion([]);

      process.env.CUBEJS_DB_SQL_PREAMBLE = 'CREATE TEMP FUNCTION median(x FLOAT64) AS (x)';
      const after = loader.contentVersion([]);

      expect(after).not.toEqual(before);
    });

    test('changing the preamble changes the content version again', () => {
      const loader = buildLoader();

      process.env.CUBEJS_DB_SQL_PREAMBLE = 'CREATE TEMP FUNCTION median(x FLOAT64) AS (x)';
      const first = loader.contentVersion([]);

      process.env.CUBEJS_DB_SQL_PREAMBLE = 'CREATE TEMP FUNCTION median(x FLOAT64) AS (x * 2)';
      const second = loader.contentVersion([]);

      expect(second).not.toEqual(first);
    });

    test('no preamble configured leaves the content version unchanged', () => {
      const loader = buildLoader();
      const withoutPreamble = loader.contentVersion([]);

      process.env.CUBEJS_DB_SQL_PREAMBLE = '';
      expect(loader.contentVersion([])).toEqual(withoutPreamble);
    });

    test('the invalidation keys still participate alongside the preamble', () => {
      // The preamble is pushed before the invalidation keys; appending it must
      // not displace them from the array.
      process.env.CUBEJS_DB_SQL_PREAMBLE = 'CREATE TEMP FUNCTION median(x FLOAT64) AS (x)';
      const loader = buildLoader();

      expect(loader.contentVersion([['key', 1]])).not.toEqual(loader.contentVersion([['key', 2]]));
    });
  });
});
