import R from 'ramda';
import crypto from 'crypto';
import { getEnv, } from '@cubejs-backend/shared';

import { BaseDriver, InlineTable, } from '@cubejs-backend/base-driver';
import { CubeStoreDriver } from '@cubejs-backend/cubestore-driver';
import { LRUCache } from 'lru-cache';

import { PreAggTableToTempTable, Query, QueryBody, QueryCache, QueryWithParams } from './QueryCache';
import { DriverFactory, DriverFactoryByDataSource } from './DriverFactory';
import { QueryQueue } from './QueryQueue';
import { CacheAndQueryDriverType } from './QueryOrchestrator';
import { PreAggregationPartitionRangeLoader } from './PreAggregationPartitionRangeLoader';
import { PreAggregationLoader } from './PreAggregationLoader';
import { PreAggregationLoadCache } from './PreAggregationLoadCache';

/// Name of the inline table containing the lambda rows.
export const LAMBDA_TABLE_PREFIX = 'lambda';

function encodeTimeStamp(time) {
  return Math.floor(time / 1000).toString(32);
}

function decodeTimeStamp(time) {
  return parseInt(time, 32) * 1000;
}

export function version(cacheKey) {
  let result = '';

  const hashCharset = 'abcdefghijklmnopqrstuvwxyz012345';
  const digestBuffer = crypto.createHash('md5').update(JSON.stringify(cacheKey)).digest();

  let residue = 0;
  let shiftCounter = 0;

  for (let i = 0; i < 5; i++) {
    const byte = digestBuffer.readUInt8(i);
    shiftCounter += 8;
    // eslint-disable-next-line operator-assignment,no-bitwise
    residue = (byte << (shiftCounter - 8)) | residue;
    // eslint-disable-next-line no-bitwise
    while (residue >> 5) {
      result += hashCharset.charAt(residue % 32);
      shiftCounter -= 5;
      // eslint-disable-next-line operator-assignment,no-bitwise
      residue = residue >> 5;
    }
  }

  result += hashCharset.charAt(residue % 32);

  return result;
}

// Returns the oldest timestamp, if any.
export function getLastUpdatedAtTimestamp(
  timestamps: (number | undefined)[]
): number | undefined {
  timestamps = timestamps.filter(t => t !== undefined);
  if (timestamps.length === 0) {
    return undefined;
  } else {
    return Math.min(...timestamps);
  }
}

export function getStructureVersion(preAggregation) {
  const versionArray = [preAggregation.structureVersionLoadSql || preAggregation.loadSql];
  if (preAggregation.indexesSql?.length) {
    versionArray.push(preAggregation.indexesSql);
  }
  if (preAggregation.streamOffset) {
    versionArray.push(preAggregation.streamOffset);
  }
  if (preAggregation.outputColumnTypes) {
    versionArray.push(preAggregation.outputColumnTypes);
  }

  return version(versionArray.length === 1 ? versionArray[0] : versionArray);
}

export type VersionEntry = {
  'table_name': string,
  'content_version': string,
  'structure_version': string,
  'last_updated_at': number,
  'build_range_end'?: string,
  'naming_version'?: number
};

export type VersionEntriesObj = {
  versionEntries: VersionEntry[],
  byStructure: { [key: string]: VersionEntry },
  byContent: { [key: string]: VersionEntry },
  byTableName: { [key: string]: VersionEntry },
};

export type InvalidationKeys = unknown[];

export type TableCacheEntry = {
  // eslint-disable-next-line camelcase
  table_name?: string;
  TABLE_NAME?: string;
  // eslint-disable-next-line camelcase
  build_range_end?: string;
};

export type QueryDateRange = [string, string];

export type PartitionRanges = {
  buildRange: QueryDateRange,
  partitionRanges: QueryDateRange[],
};

type IndexDescription = {
  sql: QueryWithParams;
  indexName: string;
};

type PreAggJob = {
  request: string,
  context: { securityContext: any },
  preagg: string,
  table: string,
  target: string,
  structure: string,
  content: string,
  updated: string,
  key: any[],
  status: string;
  timezone: string,
  dataSource: string,
};

export type LoadPreAggregationResult = {
  targetTableName: string;
  refreshKeyValues: any[];
  lastUpdatedAt: number;
  buildRangeEnd: string;
  lambdaTable?: InlineTable;
  queryKey?: any[];
  rollupLambdaId?: string;
  partitionRange?: QueryDateRange;
};

export type PreAggregationTableToTempTable = [string, LoadPreAggregationResult];

export type LambdaOptions = {
  maxSourceRows: number
};

export type LambdaQuery = {
  sqlAndParams: QueryWithParams,
  cacheKeyQueries: any[],
};

export type PreAggregationDescription = {
  preAggregationsSchema: string;
  type: 'rollup' | 'originalSql';
  preAggregationId: string;
  priority: number;
  dataSource: string;
  external: boolean;
  previewSql: QueryWithParams;
  timezone: string;
  indexesSql: IndexDescription[];
  invalidateKeyQueries: QueryWithParams[];
  partitionInvalidateKeyQueries: QueryWithParams[];
  structureVersionLoadSql: QueryWithParams;
  sql: QueryWithParams;
  loadSql: QueryWithParams;
  tableName: string;
  matchedTimeDimensionDateRange: QueryDateRange;
  granularity: string;
  partitionGranularity: string;
  preAggregationStartEndQueries: [QueryWithParams, QueryWithParams];
  timestampFormat: string;
  timestampPrecision: number;
  expandedPartition: boolean;
  unionWithSourceData: LambdaOptions;
  buildRangeStart?: string;
  buildRangeEnd?: string;
  updateWindowSeconds?: number;
  sealAt?: string;
  rollupLambdaId?: string;
  lastRollupLambda?: boolean;
};

export const tablesToVersionEntries = (schema, tables: TableCacheEntry[]): VersionEntry[] => R.sortBy(
  table => -table.last_updated_at,
  tables.map(table => {
    const match = (table.table_name || table.TABLE_NAME).match(/(.+)_(.+)_(.+)_(.+)/);

    if (!match) {
      return null;
    }

    const entity: any = {
      table_name: `${schema}.${match[1]}`,
      content_version: match[2],
      structure_version: match[3],
    };

    if (match[4].length < 13) {
      entity.last_updated_at = decodeTimeStamp(match[4]);
      entity.naming_version = 2;
    } else {
      entity.last_updated_at = parseInt(match[4], 10);
    }

    if (table.build_range_end) {
      entity.build_range_end = table.build_range_end;
    }

    return entity;
  }).filter(R.identity)
);

type PreAggregationsOptions = {
  maxPartitions: number;
  maxSourceRowLimit: number;
  preAggregationsSchemaCacheExpire?: number;
  loadCacheQueueOptions?: any;
  queueOptions?: (dataSource: string) => Promise<{
    concurrency: number;
    continueWaitTimeout?: number;
    executionTimeout?: number;
    orphanedTimeout?: number;
    heartBeatInterval?: number;
  }>;
  cubeStoreDriverFactory?: () => Promise<CubeStoreDriver>;
  continueWaitTimeout?: number;
  cacheAndQueueDriver?: CacheAndQueryDriverType;
  skipExternalCacheAndQueue?: boolean;
};

type PreAggregationQueryBody = QueryBody & {
  preAggregationsLoadCacheByDataSource?: {
    [key: string]: PreAggregationLoadCache,
  };
};

export class PreAggregations {
  public options: PreAggregationsOptions;

  public externalDriverFactory: DriverFactory;

  public structureVersionPersistTime: any;

  private readonly touchTablePersistTime: number;

  public readonly dropPreAggregationsWithoutTouch: boolean;

  private readonly usedTablePersistTime: number;

  private readonly externalRefresh: boolean;

  private readonly loadCacheQueue: Record<string, QueryQueue> = {};

  private readonly queue: Record<string, QueryQueue> = {};

  private readonly getQueueEventsBus: any;

  private readonly touchCache: LRUCache<string, true>;

  public constructor(
    private readonly redisPrefix: string,
    private readonly driverFactory: DriverFactoryByDataSource,
    private readonly logger: any,
    private readonly queryCache: QueryCache,
    options,
  ) {
    this.options = options || {};

    this.externalDriverFactory = options.externalDriverFactory;
    this.structureVersionPersistTime = options.structureVersionPersistTime || 60 * 60 * 24 * 30;
    this.touchTablePersistTime = options.touchTablePersistTime || getEnv('touchPreAggregationTimeout');
    this.dropPreAggregationsWithoutTouch = options.dropPreAggregationsWithoutTouch || getEnv('dropPreAggregationsWithoutTouch');
    this.usedTablePersistTime = options.usedTablePersistTime || getEnv('dbQueryTimeout');
    this.externalRefresh = options.externalRefresh;
    this.getQueueEventsBus = options.getQueueEventsBus;
    this.touchCache = new LRUCache({
      max: getEnv('touchPreAggregationCacheMaxCount'),
      ttl: getEnv('touchPreAggregationCacheMaxAge') * 1000,
      allowStale: false,
      updateAgeOnGet: false
    });
  }

  protected tablesUsedRedisKey(tableName: string): string {
    // TODO add dataSource?
    return this.queryCache.getKey('SQL_PRE_AGGREGATIONS_TABLES_USED', tableName);
  }

  protected tablesTouchRedisKey(tableName: string): string {
    // TODO add dataSource?
    return this.queryCache.getKey('SQL_PRE_AGGREGATIONS_TABLES_TOUCH', tableName);
  }

  protected refreshEndReachedKey() {
    // TODO add dataSource?
    return this.queryCache.getKey('SQL_PRE_AGGREGATIONS_REFRESH_END_REACHED', '');
  }

  public async addTableUsed(tableName: string): Promise<void> {
    await this.queryCache.getCacheDriver().set(
      this.tablesUsedRedisKey(tableName),
      true,
      this.usedTablePersistTime
    );
  }

  public async tablesUsed() {
    return (await this.queryCache.getCacheDriver().keysStartingWith(this.tablesUsedRedisKey('')))
      .map(k => k.replace(this.tablesUsedRedisKey(''), ''));
  }

  public async updateLastTouch(tableName: string): Promise<void> {
    if (this.touchCache.has(tableName)) {
      return;
    }

    try {
      this.touchCache.set(tableName, true);

      await this.queryCache.getCacheDriver().set(
        this.tablesTouchRedisKey(tableName),
        new Date().getTime(),
        this.touchTablePersistTime
      );
    } catch (e: unknown) {
      this.touchCache.delete(tableName);

      throw e;
    }
  }

  public async tablesTouched() {
    return (await this.queryCache.getCacheDriver().keysStartingWith(this.tablesTouchRedisKey('')))
      .map(k => k.replace(this.tablesTouchRedisKey(''), ''));
  }

  public async updateRefreshEndReached() {
    return this.queryCache.getCacheDriver().set(this.refreshEndReachedKey(), new Date().getTime(), this.touchTablePersistTime);
  }

  public async getRefreshEndReached(): Promise<number> {
    return this.queryCache.getCacheDriver().get(this.refreshEndReachedKey());
  }

  /**
   * Determines whether the partition table already exists or not.
   */
  public async isPartitionExist(
    request: string,
    external: boolean,
    dataSource = 'default',
    schema: string,
    table: string,
    key: any,
    token: string,
  ): Promise<[boolean, string]> {
    // fetching tables
    const loadCache = new PreAggregationLoadCache(
      () => this.driverFactory(dataSource),
      this.queryCache,
      this,
      {
        requestId: request,
        dataSource,
        tablePrefixes: external ? null : [schema],
      }
    );
    let tables: any[] = await loadCache.fetchTables(<PreAggregationDescription>{
      external,
      preAggregationsSchema: schema,
    });
    tables = tables.filter(row => `${schema}.${row.table_name}` === table);

    // fetching query result
    const { queueDriver } = this.queue[dataSource];
    const conn = await queueDriver.createConnection();
    const result = await conn.getResult(key);
    queueDriver.release(conn);

    // calculating status
    let status: string;
    if (tables.length === 1) {
      status = 'done';
    } else {
      status = result?.error
        ? `failure: ${result.error}`
        : 'missing_partition';
    }

    // updating jobs cache if needed
    if (result) {
      const preAggJob: PreAggJob = await this
        .queryCache
        .getCacheDriver()
        .get(`PRE_AGG_JOB_${token}`);

      await this
        .queryCache
        .getCacheDriver()
        .set(
          `PRE_AGG_JOB_${token}`,
          {
            ...preAggJob,
            status,
          },
          86400,
        );
    }

    // returning response
    return [true, status];
  }

  public async loadAllPreAggregationsIfNeeded(
    queryBody: PreAggregationQueryBody,
  ): Promise<{
    preAggregationsTablesToTempTables: PreAggTableToTempTable[],
    values: null | string[],
  }> {
    const preAggregations = queryBody.preAggregations || [];

    const loadCacheByDataSource = queryBody.preAggregationsLoadCacheByDataSource || {};

    const getLoadCacheByDataSource = (
      dataSource = 'default',
      preAggregationSchema: string,
    ) => {
      if (!loadCacheByDataSource[`${dataSource}_${preAggregationSchema}`]) {
        loadCacheByDataSource[`${dataSource}_${preAggregationSchema}`] =
          new PreAggregationLoadCache(
            () => this.driverFactory(dataSource),
            this.queryCache,
            this,
            {
              requestId: queryBody.requestId,
              dataSource,
              tablePrefixes:
                // Can't reuse tablePrefixes for shared refresh scheduler cache
                !queryBody.preAggregationsLoadCacheByDataSource ?
                  preAggregations
                    .filter(p => (p.dataSource || 'default') === dataSource)
                    .map(p => p.tableName.split('.')[1]) : null
            }
          );
      }
      return loadCacheByDataSource[`${dataSource}_${preAggregationSchema}`];
    };

    let queryParamsReplacement = null;

    const preAggregationsTablesToTempTablesPromise =
      preAggregations.map((p: PreAggregationDescription, i) => (preAggregationsTablesToTempTables) => {
        const loader = new PreAggregationPartitionRangeLoader(
          () => this.driverFactory(p.dataSource || 'default'),
          this.logger,
          this.queryCache,
          this,
          p,
          preAggregationsTablesToTempTables,
          getLoadCacheByDataSource(p.dataSource, p.preAggregationsSchema),
          {
            maxPartitions: this.options.maxPartitions,
            maxSourceRowLimit: this.options.maxSourceRowLimit,
            isJob: queryBody.isJob,
            waitForRenew: queryBody.renewQuery,
            // TODO workaround to avoid continuous waiting on building pre-aggregation dependencies
            forceBuild: i === preAggregations.length - 1 ? queryBody.forceBuildPreAggregations : false,
            requestId: queryBody.requestId,
            metadata: queryBody.metadata,
            orphanedTimeout: queryBody.orphanedTimeout,
            lambdaQuery: (queryBody.lambdaQueries ?? {})[p.preAggregationId],
            externalRefresh: this.externalRefresh
          },
        );

        const preAggregationPromise = async () => {
          const loadResult = await loader.loadPreAggregations();
          const usedPreAggregation = {
            ...loadResult,
            type: p.type,
          };
          await this.addTableUsed(usedPreAggregation.targetTableName);

          if (i === preAggregations.length - 1 && queryBody.values) {
            queryParamsReplacement = await loader.replaceQueryBuildRangeParams(
              queryBody.values,
            );
          }

          return [p.tableName, usedPreAggregation];
        };

        return preAggregationPromise().then(res => preAggregationsTablesToTempTables.concat([res]));
      }).reduce((promise, fn) => promise.then(fn), Promise.resolve([]));

    return preAggregationsTablesToTempTablesPromise.then(preAggregationsTablesToTempTables => ({
      preAggregationsTablesToTempTables,
      values: queryParamsReplacement
    }));
  }

  /**
   * Determines whether range queries for the preAggregations from the
   * queryBody were cached or not.
   */
  public async checkPartitionsBuildRangeCache(queryBody) {
    const preAggregations = queryBody.preAggregations || [];
    return Promise.all(
      preAggregations.map(async (preAggregation) => {
        const { preAggregationStartEndQueries } = preAggregation;
        const invalidate =
          preAggregation?.invalidateKeyQueries[0]
            ? preAggregation.invalidateKeyQueries[0].slice(0, 2)
            : false;
        const isCached = preAggregation.partitionGranularity
          ? (
            await Promise.all(
              preAggregationStartEndQueries.map(([query, values]) => (
                this.queryCache.resultFromCacheIfExists({
                  query,
                  values,
                  invalidate,
                })
              ))
            )
          ).every((res: any) => res?.data)
          : true;
        return {
          preAggregation,
          isCached,
        };
      })
    );
  }

  public async expandPartitionsInPreAggregations(queryBody: Query): Promise<Query> {
    const preAggregations = queryBody.preAggregations || [];

    const loadCacheByDataSource = queryBody.preAggregationsLoadCacheByDataSource || {};

    const getLoadCacheByDataSource = (dataSource = 'default', preAggregationSchema) => {
      if (!loadCacheByDataSource[`${dataSource}_${preAggregationSchema}`]) {
        loadCacheByDataSource[`${dataSource}_${preAggregationSchema}`] =
          new PreAggregationLoadCache(
            () => this.driverFactory(dataSource),
            this.queryCache,
            this,
            {
              requestId: queryBody.requestId,
              dataSource,
            }
          );
      }

      return loadCacheByDataSource[`${dataSource}_${preAggregationSchema}`];
    };

    const expandedPreAggregations: PreAggregationDescription[][] = await Promise.all(preAggregations.map(p => {
      const loader = new PreAggregationPartitionRangeLoader(
        () => this.driverFactory(p.dataSource || 'default'),
        this.logger,
        this.queryCache,
        this,
        p,
        [],
        getLoadCacheByDataSource(p.dataSource, p.preAggregationsSchema),
        {
          maxPartitions: this.options.maxPartitions,
          maxSourceRowLimit: this.options.maxSourceRowLimit,
          waitForRenew: queryBody.renewQuery,
          requestId: queryBody.requestId,
          externalRefresh: this.externalRefresh,
          compilerCacheFn: queryBody.compilerCacheFn,
        },
      );

      return loader.partitionPreAggregations();
    }));

    expandedPreAggregations.forEach((preAggs) => preAggs.forEach(p => {
      p.expandedPartition = true;
    }));

    return {
      ...queryBody,
      preAggregations: expandedPreAggregations.flat(),
      groupedPartitionPreAggregations: expandedPreAggregations
    };
  }

  public async getQueue(dataSource: string = 'default') {
    if (!this.queue[dataSource]) {
      const queueOptions = await this.options.queueOptions(dataSource);
      if (!this.queue[dataSource]) {
        this.queue[dataSource] = QueryCache.createQueue(
          `SQL_PRE_AGGREGATIONS_${this.redisPrefix}_${dataSource}`,
          () => this.driverFactory(dataSource),
          (client, q) => {
            const {
              preAggregation, preAggregationsTablesToTempTables, newVersionEntry, requestId, invalidationKeys, buildRangeEnd
            } = q;
            const loader = new PreAggregationLoader(
              () => this.driverFactory(dataSource),
              this.logger,
              this.queryCache,
              this,
              preAggregation,
              preAggregationsTablesToTempTables,
              new PreAggregationLoadCache(
                () => this.driverFactory(dataSource),
                this.queryCache,
                this,
                {
                  requestId,
                  dataSource,
                },
              ),
              { requestId, externalRefresh: this.externalRefresh, buildRangeEnd }
            );
            return loader.refresh(newVersionEntry, invalidationKeys, client);
          },
          {
            concurrency: 1,
            logger: this.logger,
            cacheAndQueueDriver: this.options.cacheAndQueueDriver,
            cubeStoreDriverFactory: this.options.cubeStoreDriverFactory,
            // Centralized continueWaitTimeout that can be overridden in queueOptions
            continueWaitTimeout: this.options.continueWaitTimeout,
            ...queueOptions,
            getQueueEventsBus: this.getQueueEventsBus,
          }
        );
      }
    }
    return this.queue[dataSource];
  }

  /**
   * Returns registered queries queues hash table.
   */
  public getQueues(): {[dataSource: string]: QueryQueue} {
    return this.queue;
  }

  public getLoadCacheQueue(dataSource: string = 'default') {
    if (!this.loadCacheQueue[dataSource]) {
      this.loadCacheQueue[dataSource] = QueryCache.createQueue(
        `SQL_PRE_AGGREGATIONS_CACHE_${this.redisPrefix}_${dataSource}`,
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        () => <BaseDriver> {},
        (_, q) => {
          const {
            preAggregation,
            requestId
          } = q;
          const loadCache = new PreAggregationLoadCache(
            () => this.driverFactory(dataSource),
            this.queryCache,
            this,
            {
              requestId,
              dataSource,
            }
          );
          return loadCache.fetchTables(preAggregation);
        },
        {
          getQueueEventsBus: this.getQueueEventsBus,
          concurrency: 4,
          logger: this.logger,
          cacheAndQueueDriver: this.options.cacheAndQueueDriver,
          cubeStoreDriverFactory: this.options.cubeStoreDriverFactory,
          ...this.options.loadCacheQueueOptions
        }
      );
    }
    return this.loadCacheQueue[dataSource];
  }

  public static preAggregationQueryCacheKey(preAggregation) {
    return preAggregation.tableName;
  }

  public static targetTableName(versionEntry): string {
    if (versionEntry.naming_version === 2) {
      return `${versionEntry.table_name}_${versionEntry.content_version}_${versionEntry.structure_version}_${versionEntry.last_updated_at === '*' ? versionEntry.last_updated_at : encodeTimeStamp(versionEntry.last_updated_at)}`;
    }

    return `${versionEntry.table_name}_${versionEntry.content_version}_${versionEntry.structure_version}_${versionEntry.last_updated_at}`;
  }

  public static noPreAggregationPartitionsBuiltMessage(preAggregations: PreAggregationDescription[]): string {
    const expectedTableNames = preAggregations.map(p => PreAggregations.targetTableName({
      table_name: p.tableName,
      structure_version: getStructureVersion(p),
      content_version: '*',
      last_updated_at: '*',
      naming_version: 2,
    }));
    return 'No pre-aggregation partitions were built yet for the pre-aggregation serving this query and ' +
      'this API instance wasn\'t set up to build pre-aggregations. ' +
      'Please make sure your refresh worker is configured correctly, running, pre-aggregation tables are built and ' +
      'all pre-aggregation refresh settings like timezone match. ' +
      `Expected table name patterns: ${expectedTableNames.join(', ')}`;
  }

  public static structureVersion(preAggregation) {
    return getStructureVersion(preAggregation);
  }

  public async getVersionEntries(preAggregations: PreAggregationDescription[], requestId): Promise<VersionEntry[][]> {
    const loadCacheByDataSource = {};

    const getLoadCacheByDataSource = (preAggregationSchema, dataSource = 'default') => {
      if (!loadCacheByDataSource[`${dataSource}_${preAggregationSchema}`]) {
        loadCacheByDataSource[`${dataSource}_${preAggregationSchema}`] =
          new PreAggregationLoadCache(
            () => this.driverFactory(dataSource),
            this.queryCache,
            this,
            {
              requestId,
              dataSource,
            }
          );
      }

      return loadCacheByDataSource[`${dataSource}_${preAggregationSchema}`];
    };

    const firstByCacheKey = {};
    const data: VersionEntry[][] = await Promise.all(
      preAggregations.map(
        async preAggregation => {
          const { dataSource, preAggregationsSchema } = preAggregation;
          const cacheKey = getLoadCacheByDataSource(preAggregationsSchema, dataSource).tablesCachePrefixKey(preAggregation);
          if (!firstByCacheKey[cacheKey]) {
            firstByCacheKey[cacheKey] = getLoadCacheByDataSource(preAggregationsSchema, dataSource).getVersionEntries(preAggregation);
            const res = await firstByCacheKey[cacheKey];
            return res.versionEntries;
          }

          return null;
        }
      )
    );
    return data.filter(res => res);
  }

  public async getQueueState(dataSource: string) {
    const queue = await this.getQueue(dataSource);
    return queue.getQueries();
  }

  public async cancelQueriesFromQueue(queryKeys: string[], dataSource: string) {
    const queue = await this.getQueue(dataSource);
    return Promise.all(queryKeys.map(queryKey => queue.cancelQuery(queryKey as any, null)));
  }
}
