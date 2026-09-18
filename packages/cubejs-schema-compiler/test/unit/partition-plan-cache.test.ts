import { FROM_PARTITION_RANGE, TO_PARTITION_RANGE, QueryDateRange } from '@cubejs-backend/shared';
import { PreAggregationDescription, PreAggregationPartitionRangeLoader } from '@cubejs-backend/query-orchestrator';
import { CompilerCache } from '../../src/compiler/CompilerCache';

const preAggregation = (timezone: string): PreAggregationDescription => ({
  preAggregationsSchema: 'test',
  type: 'rollup',
  preAggregationId: 'Events.byDay',
  tableName: 'test.events_by_day',
  priority: 0,
  dataSource: 'default',
  external: false,
  timezone,
  granularity: 'day',
  partitionGranularity: 'day',
  timestampFormat: 'YYYY-MM-DDTHH:mm:ss.SSS',
  timestampPrecision: 3,
  expandedPartition: false,
  matchedTimeDimensionDateRange: null!,
  unionWithSourceData: null!,
  indexesSql: [],
  invalidateKeyQueries: [],
  partitionInvalidateKeyQueries: [['SELECT 1', []]],
  preAggregationStartEndQueries: [['SELECT MIN(ts)', []], ['SELECT MAX(ts)', []]],
  previewSql: ['SELECT * FROM test.events_by_day', []],
  structureVersionLoadSql: ['SELECT 1', []],
  loadSql: ['CREATE TABLE test.events_by_day AS SELECT * FROM events WHERE ts >= ? AND ts <= ?', [FROM_PARTITION_RANGE, TO_PARTITION_RANGE]],
  sql: ['SELECT * FROM events WHERE ts >= ? AND ts <= ?', [FROM_PARTITION_RANGE, TO_PARTITION_RANGE]],
});

test('CompilerCache retains only the last partition plan per identity across hundreds of updates', async () => {
  const compiler = new CompilerCache({ maxQueryCacheSize: 100, maxQueryCacheAge: 60 });
  const baseQuery = { measures: ['Events.count'] };
  const query = compiler.getQueryCache(baseQuery);
  const entries = new Map<string, unknown>();
  const createEntry = jest.fn();
  const compilerCacheFn = <T>(key: string[], fn: () => T): T => query.cache(['expandPartitions', ...key], () => {
    const entry = fn();
    createEntry(key);
    entries.set(JSON.stringify(key), entry);
    return entry;
  });
  // Range loading is mocked; planning uses the real loader and compiler cache.
  const loaders = ['UTC', 'Europe/Paris'].map(timezone => new PreAggregationPartitionRangeLoader(
    async () => null!, jest.fn(), null!, null!, preAggregation(timezone), [], null!,
    { compilerCacheFn, maxPartitions: 10000, maxSourceRowLimit: 10000 },
  ));
  const bounds = loaders.map(loader => jest.spyOn(loader, 'loadBuildRange'));
  let lastPlans: PreAggregationDescription[][] = [];
  let lastRange: QueryDateRange = ['2024-01-01T00:00:00.000', '2024-01-02T12:00:00.000'];

  for (let i = 0; i < 300; i++) {
    lastRange = [lastRange[0], new Date(Date.UTC(2024, 0, 2, 12, 0, i)).toISOString().slice(0, -1)];

    for (const spy of bounds) {
      spy.mockResolvedValue(lastRange);
    }
    const plans = await Promise.all(loaders.map(loader => loader.partitionPreAggregations()));

    for (const [index, plan] of plans.entries()) {
      expect(plan).toHaveLength(2);
      expect(plan).not.toBe(lastPlans[index]);
      expect(plan[1].buildRangeEnd).toBe(lastRange[1]);
    }
    lastPlans = plans;
  }

  expect(compiler.getQueryCache(baseQuery)).toBe(query);
  expect(createEntry).toHaveBeenCalledTimes(2);
  expect(entries.size).toBe(2);
  expect([...entries.keys()].map(key => JSON.parse(key)[0])).toEqual(['partitionPlan', 'partitionPlan']);
  expect([...entries.values()]).toEqual(lastPlans.map(descriptions => ({
    rangeKey: JSON.stringify(lastRange), descriptions,
  })));

  for (const [index, loader] of loaders.entries()) {
    expect(await loader.partitionPreAggregations()).toBe(lastPlans[index]);
  }
});
