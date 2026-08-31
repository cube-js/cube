import crypto from 'crypto';
import csvWriter from 'csv-write-stream';
import { LRUCache } from 'lru-cache';
import { pipeline } from 'stream';
import {
  AsyncDebounce,
  getEnv,
  MaybeCancelablePromise,
  streamToArray,
  CacheMode,
  LoggerFn,
} from '@cubejs-backend/shared';
import { CubeStoreCacheDriver, CubeStoreDriver } from '@cubejs-backend/cubestore-driver';
import {
  BaseDriver,
  InlineTables,
  CacheDriverInterface,
  TableStructure,
  DriverInterface, QueryKey,
  QueuePriority,
} from '@cubejs-backend/base-driver';

import { QueryQueue, QueryQueueOptions } from './QueryQueue';
import { ContinueWaitError } from './ContinueWaitError';
import { LocalCacheDriver } from './LocalCacheDriver';
import { DriverFactory, DriverFactoryByDataSource } from './DriverFactory';
import { LoadPreAggregationResult, PreAggregationDescription } from './PreAggregations';
import {
  getCacheHash,
  extractRequestUUID,
  evaluateLocalRefreshKey,
  isValidLocalRefreshKey,
} from './utils';
import { CacheAndQueryDriverType, MetadataOperationType } from './QueryOrchestrator';

export type CacheQueryResultOptions = {
  renewalThreshold?: number,
  renewalKey?: any,
  priority?: number,
  external?: boolean,
  requestId?: string,
  dataSource: string,
  waitForRenew?: boolean,
  forceNoCache?: boolean,
  useInMemory?: boolean,
  useCsvQuery?: boolean,
  lambdaTypes?: TableStructure,
  persistent?: boolean,
  primaryQuery?: boolean,
  renewCycle?: boolean,
};

/**
 * Deliberately narrow: the cache key, the renewal key and the renewal threshold are derived inside
 * `cacheRefreshKeyResult`, so no caller can store an entry under a key it later looks up by another.
 */
export type RefreshKeyCacheOptions =
  Pick<CacheQueryResultOptions, 'priority' | 'requestId' | 'waitForRenew' | 'dataSource'>;

/**
 * Everything needed to evaluate an `every` based refreshKey without touching a
 * database: `FLOOR((utcOffset + unixTimestamp - dayOffset) / interval)`.
 */
export type LocalRefreshKeyDescriptor = {
  interval: number;
  utcOffset: number;
  dayOffset: number;
  cron?: boolean;
};

type QueryOptions = {
  external?: boolean;
  renewalThreshold?: number;
  updateWindowSeconds?: number;
  renewalThresholdOutsideUpdateWindow?: number;
  incremental?: boolean;
  localRefreshKey?: LocalRefreshKeyDescriptor;
};

export type QueryWithParams = [
  sql: string,
  params: string[],
  options?: QueryOptions
];

export type LoadRefreshKeyOptions = {
  requestId?: string;
  skipRefreshKeyWaitForRenew?: boolean;
  dataSource: string
};

export type Query = {
  requestId?: string;
  dataSource: string;
  preAggregations?: PreAggregationDescription[];
  groupedPartitionPreAggregations?: PreAggregationDescription[][];
  preAggregationsLoadCacheByDataSource?: any;
  cacheMode?: CacheMode;
  compilerCacheFn?: <T>(subKey: string[], cacheFn: () => T) => T;
};

export type QueryBody = {
  dataSource?: string;
  persistent?: boolean;
  query?: string;
  values?: string[];
  loadRefreshKeysOnly?: boolean;
  scheduledRefresh?: boolean;
  cacheMode?: CacheMode;
  requestId?: string;
  external?: boolean;
  isJob?: boolean;
  forceNoCache?: boolean;
  preAggregations?: PreAggregationDescription[];
  groupedPartitionPreAggregations?: PreAggregationDescription[][];
  aliasNameToMember?: {
    [alias: string]: string;
  };
  preAggregationsLoadCacheByDataSource?: {
    [key: string]: any;
  };
  [key: string]: any;
};

/**
 * Temp (partition/lambda) table definition.
 */
export type TempTable = LoadPreAggregationResult;

/**
 * Pre-aggregation table (stored in the first element) to temp table
 * definition (stored in the second element) link.
 */
export type PreAggTableToTempTable = [
  string, // common table name (without suffix)
  TempTable,
];

export type PreAggTableToTempTableNames = [string, { targetTableName: string; }];

export type CacheKeyItem = string | string[] | boolean | QueryWithParams | QueryWithParams[] | undefined;

export type CacheKey =
  [CacheKeyItem, CacheKeyItem] |
  [CacheKeyItem, CacheKeyItem, CacheKeyItem] |
  [CacheKeyItem, CacheKeyItem, CacheKeyItem, CacheKeyItem];

export type CacheEntry = {
  time: number;
  result: any;
  renewalKey?: string;
  requestId?: string;
};

export enum CacheAction {
  ServeCached = 'serve-cached',
  RefreshSameRequest = 'refresh-same-request',
  RefreshBackground = 'refresh-background',
  WaitForRenew = 'wait-for-renew',
}

type CacheOperationContext = {
  cacheKey: CacheKey;
  redisKey: string;
  renewalKey?: string;
  expiration: number;
  spanId: string;
  options: CacheQueryResultOptions;
  log: (message: string, extra?: Record<string, any>) => void;
};

export interface QueryCacheOptions {
  refreshKeyRenewalThreshold?: number;
  localRefreshKey?: boolean;
  externalQueueOptions?: any;
  externalDriverFactory?: DriverFactory;
  backgroundRenew?: Boolean;
  queueOptions?: (dataSource: string) => Promise<{
    concurrency: number;
    continueWaitTimeout?: number;
    executionTimeout?: number;
    orphanedTimeout?: number;
    heartBeatInterval?: number;
  }>;
  cubeStoreDriverFactory?: () => Promise<CubeStoreDriver>,
  continueWaitTimeout?: number;
  cacheAndQueueDriver: CacheAndQueryDriverType;
  maxInMemoryCacheEntries?: number;
  skipExternalCacheAndQueue?: boolean;
}

export class QueryCache {
  protected readonly cacheDriver: CacheDriverInterface;

  protected queue: { [dataSource: string]: QueryQueue } = {};

  protected externalQueue: QueryQueue | null = null;

  protected memoryCache: LRUCache<string, CacheEntry>;

  protected static readonly IN_MEMORY_CACHE_DISABLE_PERIOD = 5 * 60 * 1000;

  protected readonly localRefreshKeyEnabled: boolean;

  public constructor(
    protected readonly cachePrefix: string,
    protected readonly driverFactory: DriverFactoryByDataSource,
    protected readonly logger: LoggerFn,
    public readonly options: QueryCacheOptions
  ) {
    switch (options.cacheAndQueueDriver || 'memory') {
      case 'memory':
        this.cacheDriver = new LocalCacheDriver();
        break;
      case 'cubestore':
        if (!options.cubeStoreDriverFactory) {
          throw new Error('cubeStoreDriverFactory is a required option for Cube Store cache driver');
        }

        this.cacheDriver = new CubeStoreCacheDriver(
          options.cubeStoreDriverFactory
        );
        break;
      default:
        throw new Error(`Unknown cache driver: ${options.cacheAndQueueDriver}`);
    }

    this.memoryCache = new LRUCache<string, CacheEntry>({
      max: options.maxInMemoryCacheEntries || 10000
    });
    this.localRefreshKeyEnabled = options.localRefreshKey ?? getEnv('refreshKeyLocalTime');
  }

  /**
   * Whether interval based refresh keys are answered from this instance clock instead of being
   * run as queries and cached.
   */
  public isLocalRefreshKeyActive(): boolean {
    return this.localRefreshKeyEnabled && !this.options.refreshKeyRenewalThreshold;
  }

  public localRefreshKeyResult(queryOptions?: QueryOptions): [{ refresh_key: string }] | null {
    if (!this.localRefreshKeyEnabled || !isValidLocalRefreshKey(queryOptions?.localRefreshKey)) {
      return null;
    }

    // `refreshKeyRenewalThreshold` throttles how often the SQL result is re-read, and that is
    // also what bounds how often the key advances: a value cached for a day advances daily,
    // whatever `every` says. A locally evaluated key has no cache entry to age out, so the only
    // way to keep honouring the override is to leave these keys on the SQL path.
    // TODO: support the two together by snapping the local value to the threshold instead of
    // falling back to a query.
    if (!this.isLocalRefreshKeyActive()) {
      return null;
    }

    return evaluateLocalRefreshKey(<LocalRefreshKeyDescriptor>queryOptions?.localRefreshKey);
  }

  public getCacheDriver(): CacheDriverInterface {
    return this.cacheDriver;
  }

  public getKey(catalog: string, key: string): string {
    return `${this.cachePrefix}#${catalog}:${key}`;
  }

  /**
   * Generates from the `queryBody` the final `sql` query and push it to
   * the queue. Returns promise which will be resolved by the different
   * objects, depend on the original `queryBody` object. For the
   * persistent queries returns the `stream.Writable` instance.
   *
   * @throw Error
   */
  public async cachedQueryResult(
    queryBody: QueryBody,
    preAggregationsTablesToTempTables: PreAggTableToTempTable[],
  ) {
    const query = QueryCache.replacePreAggregationTableNamesInSql(
      queryBody.query,
      preAggregationsTablesToTempTables,
    );

    const inlineTables = preAggregationsTablesToTempTables.flatMap(
      ([_, preAggregation]) => (
        preAggregation.lambdaTable ? [preAggregation.lambdaTable] : []
      )
    );

    let queuePriority: QueuePriority = QueuePriority.Interactive;

    if (Number.isInteger(queryBody.queuePriority)) {
      queuePriority = queryBody.queuePriority;
    }

    const forceNoCache = queryBody.forceNoCache || (queryBody.cacheMode === 'no-cache') || false;

    const { values } = queryBody;

    const cacheKeyQueries = this
      .cacheKeyQueriesFrom(queryBody)
      .map((queryAndParams) => QueryCache.replacePreAggregationTableNames(
        queryAndParams,
        preAggregationsTablesToTempTables,
      ));

    const renewalThreshold = queryBody.cacheKeyQueries?.renewalThreshold;

    const expireSecs = this.getExpireSecs(queryBody);

    const cacheKey = QueryCache.queryCacheKey(queryBody);

    if (
      !cacheKeyQueries ||
      queryBody.external && this.options.skipExternalCacheAndQueue ||
      queryBody.persistent
    ) {
      if (queryBody.persistent) {
        // stream will be returned here
        return this.queryWithRetryAndRelease(
          query,
          values,
          {
            cacheKey,
            priority: queuePriority,
            external: queryBody.external,
            requestId: queryBody.requestId,
            persistent: queryBody.persistent,
            dataSource: queryBody.dataSource,
            useCsvQuery: queryBody.useCsvQuery,
            lambdaTypes: queryBody.lambdaTypes,
            aliasNameToMember: queryBody.aliasNameToMember,
          }
        );
      } else {
        return {
          data: await this.queryWithRetryAndRelease(
            query,
            values,
            {
              cacheKey: [query, values],
              external: queryBody.external,
              requestId: queryBody.requestId,
              dataSource: queryBody.dataSource,
              persistent: queryBody.persistent,
              inlineTables,
            }
          ),
        };
      }
    }

    if (queryBody.cacheMode === 'must-revalidate') {
      this.logger('Requested renew', { cacheKey, requestId: queryBody.requestId });
      return this.renewQuery(
        query,
        values,
        cacheKeyQueries,
        expireSecs,
        cacheKey,
        renewalThreshold,
        {
          forceNoCache,
          external: queryBody.external,
          requestId: queryBody.requestId,
          dataSource: queryBody.dataSource,
          persistent: queryBody.persistent,
          skipRefreshKeyWaitForRenew: true,
        }
      );
    }

    if (!this.options.backgroundRenew && queryBody.cacheMode !== 'stale-while-revalidate') {
      const result = await this.renewQuery(
        query,
        values,
        cacheKeyQueries,
        expireSecs,
        cacheKey,
        renewalThreshold,
        {
          forceNoCache,
          external: queryBody.external,
          requestId: queryBody.requestId,
          dataSource: queryBody.dataSource,
          persistent: queryBody.persistent,
          skipRefreshKeyWaitForRenew: true,
        }
      );

      // Keep the cycle after the foreground renewal: concurrent passes race on a cold cache.
      // It remains necessary when skipRefreshKeyWaitForRenew serves a stale key from a warm cache.
      this.startRenewCycle(
        query,
        values,
        cacheKeyQueries,
        expireSecs,
        cacheKey,
        renewalThreshold,
        {
          external: queryBody.external,
          requestId: queryBody.requestId,
          dataSource: queryBody.dataSource,
          persistent: queryBody.persistent,
        }
      );

      return result;
    }

    this.logger('Background fetch', { cacheKey, requestId: queryBody.requestId });

    const mainPromise = this.cacheQueryResult(
      query,
      values,
      cacheKey,
      expireSecs,
      {
        priority: queuePriority,
        forceNoCache,
        external: queryBody.external,
        requestId: queryBody.requestId,
        dataSource: queryBody.dataSource,
        persistent: queryBody.persistent,
      }
    );

    if (!forceNoCache) {
      this.startRenewCycle(
        query,
        values,
        cacheKeyQueries,
        expireSecs,
        cacheKey,
        renewalThreshold,
        {
          external: queryBody.external,
          requestId: queryBody.requestId,
          dataSource: queryBody.dataSource,
          persistent: queryBody.persistent,
        }
      );
    }

    return {
      data: await mainPromise,
      lastRefreshTime: await this.lastRefreshTime(cacheKey)
    };
  }

  private getExpireSecs(queryBody: QueryBody): number {
    return queryBody.expireSecs || 24 * 3600;
  }

  private cacheKeyQueriesFrom(queryBody: QueryBody): QueryWithParams[] {
    return queryBody.cacheKeyQueries?.queries ||
      queryBody.cacheKeyQueries ||
      [];
  }

  public static queryCacheKey(queryBody: QueryBody): CacheKey {
    const key: CacheKey = [
      queryBody.query,
      queryBody.values,
      (queryBody.preAggregations || []).map(p => p.loadSql)
    ];
    if (queryBody.invalidate) {
      key.push(queryBody.invalidate);
    }
    // @ts-ignore
    key.persistent = queryBody.persistent;
    return key;
  }

  /**
   * Identity of a refresh key query: the SQL, its params, and where it runs. `external` and
   * `dataSource` are every dimension `cacheQueryResult` routes on; the rest of the options element
   * is policy applied to the result rather than part of it, and `replacePartitionSqlAndParams`
   * recomputes `renewalThreshold` from `new Date()`, so covering it would make the key drift within
   * a single request.
   */
  public static refreshKeyIdentity(
    sqlQuery: QueryWithParams,
    dataSource: string,
  ): [string, string[], boolean, string] {
    const [query, values, options] = sqlQuery;
    // Both spellings of each default have to collapse to one key: producers write "source database"
    // as `false` or as an absent option, and `getQueue` resolves an absent `dataSource` to `default`.
    return [query, values, !!options?.external, dataSource || 'default'];
  }

  /**
   * The `invalidate` discriminator of the partition build range cache, written by
   * `PreAggregationPartitionRangeLoader.loadRangeQuery` and read by
   * `PreAggregations.checkPartitionsBuildRangeCache`. Derived here so the two cannot drift apart —
   * when each spelled it out itself, the read stopped finding what the write had stored.
   */
  public static buildRangeInvalidateKey(
    preAggregation: { invalidateKeyQueries?: QueryWithParams[], dataSource?: string },
  ): [string, string[], boolean, string] | false {
    const keyQuery = preAggregation.invalidateKeyQueries?.[0];
    return keyQuery ? QueryCache.refreshKeyIdentity(keyQuery, preAggregation.dataSource) : false;
  }

  public async cacheRefreshKeyResult(
    sqlQuery: QueryWithParams,
    expiration: number,
    options: RefreshKeyCacheOptions,
  ) {
    const [query, values, queryOptions] = sqlQuery;

    // A locally evaluated key is free: nothing to cache, no queue to wait on.
    const local = this.localRefreshKeyResult(queryOptions);
    if (local) {
      return local;
    }

    const cacheKey = QueryCache.refreshKeyIdentity(sqlQuery, options.dataSource);

    return this.cacheQueryResult(query, values, cacheKey, expiration, {
      ...options,
      renewalThreshold: this.options.refreshKeyRenewalThreshold
        || queryOptions?.renewalThreshold || 2 * 60,
      renewalKey: cacheKey,
      useInMemory: true,
      external: cacheKey[2],
    });
  }

  public refreshKeyCacheKey(sqlQuery: QueryWithParams, dataSource: string): string {
    return this.queryCacheKey(QueryCache.refreshKeyIdentity(sqlQuery, dataSource));
  }

  public static extractRequestUUID(requestId: string): string {
    return extractRequestUUID(requestId);
  }

  public static replacePreAggregationTableNamesInSql(
    sql: string,
    preAggregationsTablesToTempTables: PreAggTableToTempTableNames[],
  ): string {
    // Single-pass replacement with longest-first alternation: sequential
    // per-name replacement would corrupt names that are prefixes of other
    // names (e.g. `name1` vs `name10`) and rescan already inserted target
    // names, which contain the source name as a prefix
    const sorted = [...preAggregationsTablesToTempTables]
      .sort(([a], [b]) => b.length - a.length);

    if (!sorted.length) {
      return sql;
    }

    const replacements = new Map(
      sorted.map(([tableName, { targetTableName }]) => [tableName, targetTableName])
    );
    const replaceRegex = new RegExp(
      sorted
        .map(([tableName]) => tableName.replace(/([/,!\\^${}[\]().*+?|<>\-&])/g, '\\$&'))
        .join('|'),
      'g'
    );

    return sql.replace(replaceRegex, (match) => replacements.get(match) as string);
  }

  public static replacePreAggregationTableNames(
    queryAndParams: QueryWithParams,
    preAggregationsTablesToTempTables: PreAggTableToTempTableNames[],
  ): QueryWithParams {
    const [sql, params, queryOptions] = queryAndParams;

    return [
      QueryCache.replacePreAggregationTableNamesInSql(sql, preAggregationsTablesToTempTables),
      params,
      queryOptions,
    ];
  }

  /**
   * Determines queue type, resolves `QueryQueue` instance and runs the
   * `executeInQueue` method passing incoming `query` into it. Resolves
   * promise with the `executeInQueue` method result for the not persistent
   * queries and with the `stream.Writable` instance for the persistent.
   */
  public async queryWithRetryAndRelease(
    query: string | QueryWithParams,
    values: string[],
    {
      cacheKey,
      dataSource,
      external,
      priority,
      requestId,
      spanId,
      inlineTables,
      useCsvQuery,
      lambdaTypes,
      persistent,
      aliasNameToMember,
    }: {
      cacheKey: CacheKey,
      dataSource: string,
      external: boolean,
      priority?: number,
      requestId?: string,
      spanId?: string,
      inlineTables?: InlineTables,
      useCsvQuery?: boolean,
      lambdaTypes?: TableStructure,
      persistent?: boolean,
      aliasNameToMember?: { [alias: string]: string },
    }
  ) {
    const queue = external
      ? this.getExternalQueue()
      : await this.getQueue(dataSource);

    const _query = {
      queryKey: cacheKey,
      query,
      values,
      requestId,
      inlineTables,
      useCsvQuery,
      lambdaTypes,
    };

    const opt = {
      stageQueryKey: cacheKey,
      requestId,
      spanId,
    };

    if (!persistent) {
      return queue.executeInQueue('query', cacheKey as QueryKey, _query, priority, opt);
    } else {
      return queue.executeInQueue('stream', cacheKey as QueryKey, {
        ..._query,
        aliasNameToMember,
      }, priority, opt);
    }
  }

  public async getQueue(dataSource = 'default') {
    if (!this.queue[dataSource]) {
      const queueOptions = await this.options.queueOptions(dataSource);
      if (!this.queue[dataSource]) {
        this.queue[dataSource] = QueryCache.createQueue(
          `SQL_QUERY_${this.cachePrefix}_${dataSource}`,
          () => this.driverFactory(dataSource),
          (client, req) => {
            this.logger('Executing SQL', { ...req });
            if (req.useCsvQuery) {
              return this.csvQuery(client, req);
            } else {
              return client.query(req.query, req.values, req);
            }
          },
          {
            logger: this.logger,
            cacheAndQueueDriver: this.options.cacheAndQueueDriver,
            cubeStoreDriverFactory: this.options.cubeStoreDriverFactory,
            // Centralized continueWaitTimeout that can be overridden in queueOptions
            continueWaitTimeout: this.options.continueWaitTimeout,
            ...queueOptions,
          }
        );
      }
    }
    return this.queue[dataSource];
  }

  protected async csvQuery(client, q) {
    const headers = q.lambdaTypes.map(c => c.name);
    const writer = csvWriter({
      headers,
      sendHeaders: false,
    });
    let tableData;
    try {
      if (client.stream) {
        tableData = await client.stream(q.query, q.values, q);
        const errors = [];
        await pipeline(tableData.rowStream, writer, (err) => {
          if (err) {
            errors.push(err);
          }
        });
        if (errors.length > 0) {
          throw new Error(`Lambda query errors ${errors.join(', ')}`);
        }
      } else {
        tableData = await client.downloadQueryResults(q.query, q.values, q);
        tableData.rows.forEach(
          row => writer.write(row)
        );
        writer.end();
      }
    } finally {
      if (tableData?.release) {
        await tableData.release();
      }
    }
    const lines = await streamToArray(writer);
    const rowCount = lines.length;
    const csvRows = lines.join('');
    return {
      types: q.lambdaTypes,
      csvRows,
      rowCount,
    };
  }

  public getExternalQueue() {
    if (!this.externalQueue) {
      this.externalQueue = QueryCache.createQueue(
        `SQL_QUERY_EXT_${this.cachePrefix}`,
        this.options.externalDriverFactory,
        (client, q) => {
          this.logger('Executing SQL', {
            ...q
          });
          return client.query(q.query, q.values, q);
        },
        {
          logger: this.logger,
          cacheAndQueueDriver: this.options.cacheAndQueueDriver,
          cubeStoreDriverFactory: this.options.cubeStoreDriverFactory,
          // Centralized continueWaitTimeout that can be overridden in queueOptions
          continueWaitTimeout: this.options.continueWaitTimeout,
          skipQueue: this.options.skipExternalCacheAndQueue,
          ...this.options.externalQueueOptions
        }
      );
    }
    return this.externalQueue;
  }

  public static createQueue(
    redisPrefix: string,
    clientFactory: DriverFactory,
    executeFn: (client: BaseDriver, req: any) => any,
    options: Omit<QueryQueueOptions, 'queryHandlers' | 'cancelHandlers'>
  ): QueryQueue {
    const queue: any = new QueryQueue(redisPrefix, {
      queryHandlers: {
        metadata: async (req, _setCancelHandle) => {
          const client = await clientFactory();
          const { operation } = req;
          const params = req.params || {};

          switch (operation) {
            case MetadataOperationType.GET_SCHEMAS:
              queue.logger('Getting datasource schemas', { dataSource: req.dataSource, requestId: req.requestId });
              return client.getSchemas();
            case MetadataOperationType.GET_TABLES_FOR_SCHEMAS:
              queue.logger('Getting tables for schemas', {
                dataSource: req.dataSource,
                schemaCount: params.schemas?.length || 0,
                requestId: req.requestId
              });
              return client.getTablesForSpecificSchemas(params.schemas);
            case MetadataOperationType.GET_COLUMNS_FOR_TABLES:
              queue.logger('Getting columns for tables', {
                dataSource: req.dataSource,
                tableCount: params.tables?.length || 0,
                requestId: req.requestId
              });
              return client.getColumnsForSpecificTables(params.tables);
            default:
              throw new Error(`Unknown metadata operation: ${operation}`);
          }
        },
        query: async (req, setCancelHandle) => {
          const client = await clientFactory();

          const resultPromise = executeFn(client, req);
          let handle;
          if (resultPromise.cancel) {
            queue.cancelHandlerCounter += 1;
            handle = queue.cancelHandlerCounter;
            queue.handles[handle] = resultPromise;
            await setCancelHandle(handle);
          }
          const result = await resultPromise;
          if (handle) {
            delete queue.handles[handle];
          }
          return result;
        },
      },
      streamHandler: async (req, target) => {
        queue.logger('Streaming SQL', { ...req });
        await (new Promise((resolve, reject) => {
          let logged = false;
          Promise
            .all([clientFactory()])
            .then(([client]) => (<DriverInterface>client).stream(req.query, req.values, { highWaterMark: getEnv('dbQueryStreamHighWaterMark'), requestId: req.requestId }))
            .then((source) => {
              const cleanup = async (error) => {
                if (source.release) {
                  const toRelease = source.release;
                  delete source.release;
                  await toRelease();
                }
                if (error && !target.destroyed) {
                  target.destroy(error);
                }
                if (!logged && target.destroyed) {
                  logged = true;
                  if (error) {
                    queue.logger('Streaming done with error', {
                      query: req.query,
                      query_values: req.values,
                      error,
                    });
                    reject(error);
                  } else {
                    queue.logger('Streaming successfully completed', {
                      requestId: req.requestId,
                    });
                    resolve(req.requestId);
                  }
                }
              };

              source.rowStream.once('end', () => cleanup(undefined));
              source.rowStream.once('error', cleanup);
              source.rowStream.once('close', () => cleanup(undefined));

              target.once('end', () => cleanup(undefined));
              target.once('error', cleanup);
              target.once('close', () => cleanup(undefined));

              source.rowStream.pipe(target);
            })
            .catch((reason) => {
              target.emit('error', reason);
              resolve(reason);
            });
        }));
      },
      cancelHandlers: {
        metadata: async (req) => {
          if (req.cancelHandler && queue.handles[req.cancelHandler]) {
            await queue.handles[req.cancelHandler].cancel();
            delete queue.handles[req.cancelHandler];
          }
        },
        query: async (req) => {
          if (req.cancelHandler && queue.handles[req.cancelHandler]) {
            await queue.handles[req.cancelHandler].cancel();
            delete queue.handles[req.cancelHandler];
          }
        },
        stream: async (req) => {
          req.queryKey.persistent = true;
          const queryKeyHash = queue.redisHash(req.queryKey);
          if (queue.streams.has(queryKeyHash)) {
            queue.streams.get(queryKeyHash).destroy();
          }
        },
      },
      logger: (msg, params) => options.logger(msg, params),
      ...options
    });
    queue.cancelHandlerCounter = 0;
    queue.handles = {};
    return queue;
  }

  /**
   * Returns registered queries queues hash table.
   */
  public getQueues(): {[dataSource: string]: QueryQueue} {
    return this.queue;
  }

  public startRenewCycle(
    query: string,
    values: string[],
    cacheKeyQueries: QueryWithParams[],
    expireSecs: number,
    cacheKey: CacheKey,
    renewalThreshold: any,
    options: {
      requestId?: string,
      skipRefreshKeyWaitForRenew?: boolean,
      external?: boolean,
      dataSource: string,
      persistent?: boolean,
    }
  ) {
    this.renewQuery(
      query,
      values,
      cacheKeyQueries,
      expireSecs,
      cacheKey,
      renewalThreshold,
      {
        ...options,
        renewCycle: true
      },
    ).catch(e => {
      if (!(e instanceof ContinueWaitError)) {
        this.logger('Error while renew cycle', {
          query, query_values: values, error: e.stack || e, requestId: options.requestId
        });
      }
    });
  }

  public renewQuery(
    query: string,
    values: string[],
    cacheKeyQueries: QueryWithParams[],
    expireSecs: number,
    cacheKey: CacheKey,
    renewalThreshold: any,
    options: {
      requestId?: string,
      skipRefreshKeyWaitForRenew?: boolean,
      external?: boolean,
      forceNoCache?: boolean,
      dataSource: string,
      useCsvQuery?: boolean,
      lambdaTypes?: TableStructure,
      persistent?: boolean,
      renewCycle?: boolean,
    }
  ) {
    options = options || { dataSource: 'default' };
    return Promise.all(
      this.loadRefreshKeys(cacheKeyQueries, expireSecs, options),
    )
      .catch(e => {
        if (e instanceof ContinueWaitError) {
          throw e;
        }
        this.logger('Error fetching cache key queries', { error: e.stack || e, requestId: options.requestId });
        return [];
      })
      .then(async cacheKeyQueryResults => (
        {
          data: await this.cacheQueryResult(
            query,
            values,
            cacheKey,
            expireSecs,
            {
              renewalThreshold: renewalThreshold || 6 * 60 * 60,
              renewalKey: cacheKeyQueryResults && [
                cacheKeyQueries,
                cacheKeyQueryResults,
                this.queryCacheKey([query, values]),
              ],
              waitForRenew: true,
              forceNoCache: options.forceNoCache,
              external: options.external,
              requestId: options.requestId,
              dataSource: options.dataSource,
              useCsvQuery: options.useCsvQuery,
              lambdaTypes: options.lambdaTypes,
              persistent: options.persistent,
              primaryQuery: true,
              renewCycle: options.renewCycle,
            }
          ),
          refreshKeyValues: cacheKeyQueryResults,
          lastRefreshTime: await this.lastRefreshTime(cacheKey)
        }
      ));
  }

  public async loadRefreshKeysFromQuery(query: Query) {
    return Promise.all(
      this.loadRefreshKeys(
        this.cacheKeyQueriesFrom(query),
        this.getExpireSecs(query),
        {
          requestId: query.requestId,
          dataSource: query.dataSource,
        }
      )
    );
  }

  public loadRefreshKeys(
    cacheKeyQueries: QueryWithParams[],
    expireSecs: number,
    options: LoadRefreshKeyOptions
  ) {
    return cacheKeyQueries.map((q) => this.loadRefreshKey(q, expireSecs, options));
  }

  @AsyncDebounce()
  public async loadRefreshKey(q: QueryWithParams, expireSecs: number, options: LoadRefreshKeyOptions) {
    return this.cacheRefreshKeyResult(
      q,
      expireSecs,
      {
        waitForRenew: !options.skipRefreshKeyWaitForRenew,
        requestId: options.requestId,
        dataSource: options.dataSource,
      },
    );
  }

  public withLock = <T = any>(
    key: string,
    ttl: number,
    callback: () => MaybeCancelablePromise<T>,
  ) => this.cacheDriver.withLock(`lock:${key}`, callback, ttl, true);

  protected static decideCacheAction(
    entry: CacheEntry,
    renewedAgo: number,
    options: CacheQueryResultOptions,
    renewalKey?: string,
  ): CacheAction {
    const { renewalThreshold } = options;
    const isExpired = !renewalThreshold || !entry.time || renewedAgo > renewalThreshold * 1000;
    const isKeyMismatch = !!renewalKey && entry.renewalKey !== renewalKey;

    if (!isExpired && !isKeyMismatch) {
      return CacheAction.ServeCached;
    }

    const isSameRequest = options.requestId && entry.requestId &&
      QueryCache.extractRequestUUID(entry.requestId) === QueryCache.extractRequestUUID(options.requestId);

    // A client polling through continue-wait re-enters with the same requestId, so rejecting
    // the result it just wrote would restart the fetch on every poll and never converge while
    // the refreshKey keeps moving. Background renew opts out: fresh data is all it exists for.
    if (isSameRequest && !options.renewCycle) {
      return CacheAction.RefreshSameRequest;
    }

    // Without a refreshKey there is nothing to refresh against, so an elapsed threshold alone never triggers a fetch.
    if (!renewalKey) {
      return CacheAction.ServeCached;
    }

    return options.waitForRenew ? CacheAction.WaitForRenew : CacheAction.RefreshBackground;
  }

  protected static isMemoryEntryUsable(
    entry: CacheEntry,
    renewedAgo: number,
    expiration: number,
    renewalThreshold?: number,
    renewalKey?: string,
  ): boolean {
    if (renewedAgo > expiration * 1000 || renewedAgo > QueryCache.IN_MEMORY_CACHE_DISABLE_PERIOD) {
      return false;
    }

    if (!renewalKey) {
      return true;
    }

    // Near expiry an in-memory entry races with refreshes carrying a different refreshKey value.
    return !!renewalThreshold &&
      !!entry.time &&
      renewedAgo + QueryCache.IN_MEMORY_CACHE_DISABLE_PERIOD <= renewalThreshold * 1000 &&
      entry.renewalKey === renewalKey;
  }

  protected cacheOperationContext(
    cacheKey: CacheKey,
    expiration: number,
    options: CacheQueryResultOptions,
  ): CacheOperationContext {
    const spanId = crypto.randomBytes(16).toString('hex');
    const logContext = {
      cacheKey,
      requestId: options.requestId,
      spanId,
      primaryQuery: options.primaryQuery,
      renewCycle: options.renewCycle,
    };

    const redisKey = this.queryCacheKey(cacheKey);

    return {
      cacheKey,
      redisKey,
      // Refresh key entries renew against their own key, so hashing it a second time is wasted work
      renewalKey: options.renewalKey && (
        options.renewalKey === cacheKey ? redisKey : this.queryCacheKey(options.renewalKey)
      ),
      expiration,
      spanId,
      options,
      log: (message, extra) => this.logger(message, extra ? { ...logContext, ...extra } : logContext),
    };
  }

  protected fetchAndCacheQuery(
    query: string | QueryWithParams,
    values: string[],
    ctx: CacheOperationContext,
  ) {
    const { cacheKey, redisKey, renewalKey, expiration, spanId, options } = ctx;

    return this.queryWithRetryAndRelease(query, values, {
      cacheKey,
      priority: options.priority,
      external: options.external,
      requestId: options.requestId,
      spanId,
      persistent: options.persistent,
      dataSource: options.dataSource,
      useCsvQuery: options.useCsvQuery,
      lambdaTypes: options.lambdaTypes,
    }).then(res => {
      const entry = {
        time: (new Date()).getTime(),
        result: res,
        renewalKey,
        requestId: options.requestId,
      };

      return this
        .cacheDriver
        .set(redisKey, entry, expiration)
        .then(({ bytes }) => {
          ctx.log('Renewed');
          this.logger('Outgoing network usage', {
            service: 'cache',
            requestId: options.requestId,
            spanId,
            bytes,
            cacheKey,
          });
          return res;
        });
    }).catch(e => {
      if (!(e instanceof ContinueWaitError)) {
        ctx.log('Dropping Cache', { error: e.stack || e });
        this.cacheDriver.remove(redisKey)
          .catch(err => this.logger('Error removing key', {
            cacheKey,
            spanId,
            error: err.stack || err,
            requestId: options.requestId
          }));
      }
      throw e;
    });
  }

  protected fetchAndCacheQueryInBackground(
    query: string | QueryWithParams,
    values: string[],
    ctx: CacheOperationContext,
  ): void {
    this.fetchAndCacheQuery(query, values, ctx).catch(e => {
      if (!(e instanceof ContinueWaitError)) {
        ctx.log('Error renewing', { error: e.stack || e });
      }
    });
  }

  protected getFromMemoryCache(ctx: CacheOperationContext): CacheEntry | null {
    const { redisKey, renewalKey, expiration, options } = ctx;
    const entry = this.memoryCache.get(redisKey);

    if (!entry) {
      return null;
    }

    const renewedAgo = (new Date()).getTime() - entry.time;

    if (!QueryCache.isMemoryEntryUsable(entry, renewedAgo, expiration, options.renewalThreshold, renewalKey)) {
      this.memoryCache.delete(redisKey);
      return null;
    }

    ctx.log('Found in memory cache entry', {
      time: entry.time,
      renewedAgo,
      renewalKey: entry.renewalKey,
      newRenewalKey: renewalKey,
      renewalThreshold: options.renewalThreshold,
    });

    return entry;
  }

  protected storeInMemoryCache(entry: CacheEntry, renewedAgo: number, ctx: CacheOperationContext): void {
    const { useInMemory, renewalThreshold } = ctx.options;

    if (useInMemory && !!renewalThreshold &&
      renewedAgo + QueryCache.IN_MEMORY_CACHE_DISABLE_PERIOD <= renewalThreshold * 1000) {
      this.memoryCache.set(ctx.redisKey, entry);
    }
  }

  public async cacheQueryResult(
    query: string | QueryWithParams,
    values: string[],
    cacheKey: CacheKey,
    expiration: number,
    options: CacheQueryResultOptions,
  ) {
    options = options || { dataSource: 'default' };

    const ctx = this.cacheOperationContext(cacheKey, expiration, options);
    const { renewalThreshold } = options;

    if (options.forceNoCache) {
      ctx.log('Force no cache for');
      return this.fetchAndCacheQuery(query, values, ctx);
    }

    let entry: CacheEntry | null = options.useInMemory ? this.getFromMemoryCache(ctx) : null;

    if (!entry) {
      entry = await this.cacheDriver.get(ctx.redisKey);
    }

    if (!entry) {
      ctx.log('Missing cache for');
      return this.fetchAndCacheQuery(query, values, ctx);
    }

    const renewedAgo = (new Date()).getTime() - entry.time;

    ctx.log('Found cache entry', {
      time: entry.time,
      renewedAgo,
      renewalKey: entry.renewalKey,
      newRenewalKey: ctx.renewalKey,
      renewalThreshold,
    });

    switch (QueryCache.decideCacheAction(entry, renewedAgo, options, ctx.renewalKey)) {
      case CacheAction.WaitForRenew:
        ctx.log('Waiting for renew', { renewalThreshold });
        return this.fetchAndCacheQuery(query, values, ctx);
      case CacheAction.RefreshSameRequest:
        ctx.log('Same request cache hit (background refresh)', { renewalThreshold });
        this.fetchAndCacheQueryInBackground(query, values, ctx);
        break;
      case CacheAction.RefreshBackground:
        ctx.log('Renewing existing key', { renewalThreshold });
        this.fetchAndCacheQueryInBackground(query, values, ctx);
        break;
      default:
        break;
    }

    ctx.log('Using cache for');
    this.storeInMemoryCache(entry, renewedAgo, ctx);

    return entry.result;
  }

  protected async lastRefreshTime(cacheKey) {
    const cachedValue = await this.cacheDriver.get(this.queryCacheKey(cacheKey));
    return cachedValue && new Date(cachedValue.time);
  }

  public async resultFromCacheIfExists(queryBody) {
    const cacheKey = QueryCache.queryCacheKey(queryBody);
    const cachedValue = await this.cacheDriver.get(this.queryCacheKey(cacheKey));
    if (cachedValue) {
      return {
        data: cachedValue.result,
        lastRefreshTime: new Date(cachedValue.time)
      };
    }
    return null;
  }

  public queryCacheKey(cacheKey: CacheKey): string {
    return this.getKey('SQL_QUERY_RESULT', getCacheHash(cacheKey) as any);
  }

  public async cleanup() {
    return this.cacheDriver.cleanup();
  }

  public async testConnection() {
    return this.cacheDriver.testConnection();
  }
}
