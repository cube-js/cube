import { PreAggregationPartitionRangeLoader } from '@cubejs-backend/query-orchestrator';
import { FROM_PARTITION_RANGE, TO_PARTITION_RANGE } from '@cubejs-backend/shared';
import { CompilerCache } from '../../src/compiler/CompilerCache';

// Reproduction for https://github.com/cube-js/cube/issues/11860
//
// The refresh scheduler expands partitions through
// `compilerApi.compilerCacheFn(requestId, baseQuery, ['expandPartitions'])`, and `baseQuery` is
// the same on every run for a given pre-aggregation and timezone. `partitionPreAggregations()`
// caches the whole list of partition descriptions under `['partitions', JSON.stringify(buildRange)]`.
// When the build range moves with time (`buildRangeStart: { sql: 'SELECT NOW() - INTERVAL 3 YEAR' }`)
// every refresh-key renewal produces a new build range, hence a new cache key, and the previous
// lists are never released: the `QueryCache` storage has no size limit and no eviction, and the
// `CompilerCache` LRU entry holding it never expires because the scheduler reads it on every run.

const ROLLUPS = 3;
const TIMEZONES = ['UTC', 'Europe/Paris'];
const RENEWALS = 12; // 3 hours worth of 15 minute refresh keys

function preAggregationDescription(index: number, timezone: string): any {
  const tableName = `pre_aggregations.rollup${index}`;
  const params = [FROM_PARTITION_RANGE, TO_PARTITION_RANGE];
  // A realistic rollup query is a few KB, which is what makes the retained lists heavy
  const sql = `SELECT ${'col, '.repeat(800)}x FROM t WHERE ts >= ? AND ts <= ?`;

  return {
    preAggregationId: `Cube${index}.rollup`,
    tableName,
    timezone,
    dataSource: 'default',
    type: 'rollup',
    partitionGranularity: 'day',
    timestampFormat: 'YYYY-MM-DDTHH:mm:ss.SSS',
    timestampPrecision: 3,
    loadSql: [`CREATE TABLE ${tableName} AS ${sql}`, params, {}],
    sql: [sql, params, {}],
    invalidateKeyQueries: [['SELECT FLOOR(UNIX_TIMESTAMP() / 900)', [], { renewalThreshold: 90 }]],
  };
}

/**
 * `buildRangeStart: { sql: 'SELECT NOW() - INTERVAL 3 YEAR' }`: the value is renewed together with
 * the refresh key, so it moves on every renewal.
 */
function movingBuildRange(renewal: number): [string, string] {
  const now = new Date(Date.UTC(2026, 8, 12) + renewal * 15 * 60 * 1000 + 7000);
  const start = new Date(now);
  start.setUTCFullYear(now.getUTCFullYear() - 3);

  return [`${start.toISOString().slice(0, 19)}.000`, '2026-10-01T00:00:00.000'];
}

async function expandPartitions(compilerCache: CompilerCache, renewal: number): Promise<void> {
  for (let index = 0; index < ROLLUPS; index++) {
    for (const timezone of TIMEZONES) {
      const preAggregation = preAggregationDescription(index, timezone);
      // As built by RefreshScheduler.refreshPreAggregation(): `baseQuery` is stable across runs
      const baseQuery = { timezone, preAggregationId: preAggregation.preAggregationId };
      const compilerCacheFn = (subKey, cacheFn) => compilerCache
        .getQueryCache(baseQuery)
        .cache(['expandPartitions'].concat(subKey), cacheFn);

      const loader = new PreAggregationPartitionRangeLoader(
        null as any,
        () => undefined,
        null as any,
        null as any,
        preAggregation,
        [],
        null as any,
        { maxPartitions: 10000, maxSourceRowLimit: 10000, compilerCacheFn },
      );
      // Stubbed so that the test needs no database: the scheduler gets this value from the
      // buildRangeStart/buildRangeEnd queries, renewed along with the refresh key.
      (loader as any).loadBuildRange = async () => movingBuildRange(renewal);

      await loader.partitionPreAggregations();
    }
  }
}

function cachedPartitionLists(compilerCache: CompilerCache): number {
  let lists = 0;

  for (const [, queryCache] of (compilerCache as any).queryCache.entries()) {
    const partitions = (queryCache as any).storage?.expandPartitions?.partitions;
    if (partitions) {
      lists += Object.keys(partitions).length;
    }
  }

  return lists;
}

describe('CompilerCache expandPartitions', () => {
  it('keeps a bounded number of expanded partition lists when the build range moves', async () => {
    const compilerCache = new CompilerCache({ maxQueryCacheSize: undefined, maxQueryCacheAge: undefined });

    for (let renewal = 1; renewal <= RENEWALS; renewal++) {
      await expandPartitions(compilerCache, renewal);
    }

    // One list per pre-aggregation and timezone is expected. Today every renewal adds one more:
    // RENEWALS * ROLLUPS * TIMEZONES lists are retained, each holding ~1100 partition descriptions.
    expect(cachedPartitionLists(compilerCache)).toBeLessThanOrEqual(ROLLUPS * TIMEZONES.length);
  });
});
