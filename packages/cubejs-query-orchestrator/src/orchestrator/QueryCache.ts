import crypto from 'crypto';
import csvWriter from 'csv-write-stream';
import { LRUCache } from 'lru-cache';
import { pipeline } from 'stream';
import { AsyncDebounce, getEnv, MaybeCancelablePromise, streamToArray } from '@cubejs-backend/shared';
import { CubeStoreCacheDriver, CubeStoreDriver } from '@cubejs-backend/cubestore-driver';
import {
  BaseDriver,
  InlineTables,
  CacheDriverInterface,
  TableStructure,
  DriverInterface, QueryKey,
} from '@cubejs-backend/base-driver';

import { QueryQueue, QueryQueueOptions } from './QueryQueue';
import { ContinueWaitError } from './ContinueWaitError';
import { LocalCacheDriver } from './LocalCacheDriver';
import { DriverFactory, DriverFactoryByDataSource } from './DriverFactory';
import { LoadPreAggregationResult, PreAggregationDescription } from './PreAggregations';
import { getCacheHash } from './utils';
import { CacheAndQueryDriverType, MetadataOperationType } from './QueryOrchestrator';

type QueryOptions = {
  external?: boolean;
  renewalThreshold?: number;
  updateWindowSeconds?: number;
  renewalThresholdOutsideUpdateWindow?: number;
  incremental?: boolean;
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
  renewQuery?: boolean;
  compilerCacheFn?: <T>(subKey: string[], cacheFn: () => T) => T;
};

export type QueryBody = {
  dataSource?: string;
  persistent?: boolean;
  query?: string;
  values?: string[];
  continueWait?: boolean;
  renewQuery?: boolean;
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

export type CacheKeyItem = string | string[] | QueryWithParams | QueryWithParams[] | undefined;

export type CacheKey =
  [CacheKeyItem, CacheKeyItem] |
  [CacheKeyItem, CacheKeyItem, CacheKeyItem] |
  [CacheKeyItem, CacheKeyItem, CacheKeyItem, CacheKeyItem];

type CacheEntry = {
  time: number;
  result: any;
  renewalKey: string;
};

export interface QueryCacheOptions {
  refreshKeyRenewalThreshold?: number;
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

  public constructor(
    protected readonly cachePrefix: string,
    protected readonly driverFactory: DriverFactoryByDataSource,
    protected readonly logger: any,
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
  }

  /**
   * Returns cache driver instance.
   */
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
    const replacePreAggregationTableNames =
      (queryAndParams: string | QueryWithParams) => (
        QueryCache.replacePreAggregationTableNames(
          queryAndParams,
          preAggregationsTablesToTempTables,
        )
      );

    const query = replacePreAggregationTableNames(queryBody.query);

    const inlineTables = preAggregationsTablesToTempTables.flatMap(
      ([_, preAggregation]) => (
        preAggregation.lambdaTable ? [preAggregation.lambdaTable] : []
      )
    );

    let queuePriority = 10;

    if (Number.isInteger(queryBody.queuePriority)) {
      queuePriority = queryBody.queuePriority;
    }

    const forceNoCache = queryBody.forceNoCache || false;

    const { values } = queryBody;

    const cacheKeyQueries = this
      .cacheKeyQueriesFrom(queryBody)
      .map(replacePreAggregationTableNames);

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
        return this.queryWithRetryAndRelease(query, values, {
          cacheKey,
          priority: queuePriority,
          external: queryBody.external,
          requestId: queryBody.requestId,
          persistent: queryBody.persistent,
          dataSource: queryBody.dataSource,
          useCsvQuery: queryBody.useCsvQuery,
          lambdaTypes: queryBody.lambdaTypes,
          aliasNameToMember: queryBody.aliasNameToMember,
        });
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

    if (queryBody.renewQuery) {
      this.logger('Requested renew', { cacheKey, requestId: queryBody.requestId });
      return this.renewQuery(
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

    if (!this.options.backgroundRenew) {
      const resultPromise = this.renewQuery(
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
          skipRefreshKeyWaitForRenew: true,
        }
      );

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

      return resultPromise;
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

  protected static replaceAll(replaceThis, withThis, inThis) {
    withThis = withThis.replace(/\$/g, '$$$$');
    return inThis.replace(
      new RegExp(replaceThis.replace(/([/,!\\^${}[\]().*+?|<>\-&])/g, '\\$&'), 'g'),
      withThis
    );
  }

  public static replacePreAggregationTableNames(
    queryAndParams: string | QueryWithParams,
    preAggregationsTablesToTempTables: PreAggTableToTempTableNames[],
  ): string | QueryWithParams {
    const [keyQuery, params, queryOptions] = Array.isArray(queryAndParams)
      ? queryAndParams
      : [queryAndParams, []];
    const replacedKeyQuery: string = preAggregationsTablesToTempTables.reduce(
      (query, [tableName, { targetTableName }]) => (
        QueryCache.replaceAll(tableName, targetTableName, query)
      ),
      keyQuery
    );
    return Array.isArray(queryAndParams)
      ? [replacedKeyQuery, params, queryOptions]
      : replacedKeyQuery;
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
            .then(([client]) => (<DriverInterface>client).stream(req.query, req.values, { highWaterMark: getEnv('dbQueryStreamHighWaterMark') }))
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
    query: string | QueryWithParams,
    values: string[],
    cacheKeyQueries: (string | QueryWithParams)[],
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
    query: string | QueryWithParams,
    values: string[],
    cacheKeyQueries: (string | QueryWithParams)[],
    expireSecs: number,
    cacheKey: CacheKey,
    renewalThreshold: any,
    options: {
      requestId?: string,
      skipRefreshKeyWaitForRenew?: boolean,
      external?: boolean,
      dataSource: string,
      useCsvQuery?: boolean,
      lambdaTypes?: TableStructure,
      persistent?: boolean,
      renewCycle?: boolean,
    }
  ) {
    options = options || { dataSource: 'default' };
    return Promise.all(
      this.loadRefreshKeys(<QueryWithParams[]>cacheKeyQueries, expireSecs, options),
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
                this.queryRedisKey([query, values]),
              ],
              waitForRenew: true,
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
    const [query, values, queryOptions]: QueryWithParams = Array.isArray(q) ? q : [q, [], {}];

    return this.cacheQueryResult(
      query,
      values,
      [query, values],
      expireSecs,
      {
        renewalThreshold: this.options.refreshKeyRenewalThreshold || queryOptions?.renewalThreshold || 2 * 60,
        renewalKey: q,
        waitForRenew: !options.skipRefreshKeyWaitForRenew,
        requestId: options.requestId,
        dataSource: options.dataSource,
        useInMemory: true,
        external: queryOptions?.external,
      },
    );
  }

  public withLock = <T = any>(
    key: string,
    ttl: number,
    callback: () => MaybeCancelablePromise<T>,
  ) => this.cacheDriver.withLock(`lock:${key}`, callback, ttl, true);

  public async cacheQueryResult(
    query: string | QueryWithParams,
    values: string[],
    cacheKey: CacheKey,
    expiration: number,
    options: {
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
    }
  ) {
    const spanId = crypto.randomBytes(16).toString('hex');
    options = options || { dataSource: 'default' };
    const { renewalThreshold, primaryQuery, renewCycle } = options;
    const renewalKey = options.renewalKey && this.queryRedisKey(options.renewalKey);
    const redisKey = this.queryRedisKey(cacheKey);
    const fetchNew = () => (
      this.queryWithRetryAndRelease(query, values, {
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
        const result = {
          time: (new Date()).getTime(),
          result: res,
          renewalKey
        };
        return this
          .cacheDriver
          .set(redisKey, result, expiration)
          .then(({ bytes }) => {
            this.logger('Renewed', { cacheKey, requestId: options.requestId, spanId, primaryQuery, renewCycle });
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
          this.logger('Dropping Cache', {
            cacheKey,
            error: e.stack || e,
            requestId: options.requestId,
            spanId,
            primaryQuery,
            renewCycle
          });
          this.cacheDriver.remove(redisKey)
            .catch(err => this.logger('Error removing key', {
              cacheKey,
              spanId,
              error: err.stack || err,
              requestId: options.requestId
            }));
        }
        throw e;
      })
    );

    if (options.forceNoCache) {
      this.logger('Force no cache for', { cacheKey, requestId: options.requestId, spanId, primaryQuery, renewCycle });
      return fetchNew();
    }

    let res;

    const inMemoryCacheDisablePeriod = 5 * 60 * 1000;

    if (options.useInMemory) {
      const inMemoryValue = this.memoryCache.get(redisKey);
      if (inMemoryValue) {
        const renewedAgo = (new Date()).getTime() - inMemoryValue.time;

        if (
          renewalKey && (
            !renewalThreshold ||
            !inMemoryValue.time ||
            // Do not cache in memory in last 5 minutes of expiry.
            // Most likely it'll cause race condition of refreshing data with different refreshKey values.
            renewedAgo + inMemoryCacheDisablePeriod > renewalThreshold * 1000 ||
            inMemoryValue.renewalKey !== renewalKey
          ) || renewedAgo > expiration * 1000 || renewedAgo > inMemoryCacheDisablePeriod
        ) {
          this.memoryCache.delete(redisKey);
        } else {
          this.logger('Found in memory cache entry', {
            cacheKey,
            time: inMemoryValue.time,
            renewedAgo,
            renewalKey: inMemoryValue.renewalKey,
            newRenewalKey: renewalKey,
            renewalThreshold,
            requestId: options.requestId,
            spanId,
            primaryQuery,
            renewCycle
          });
          res = inMemoryValue;
        }
      }
    }

    if (!res) {
      res = await this.cacheDriver.get(redisKey);
    }

    if (res) {
      const parsedResult = res;
      const renewedAgo = (new Date()).getTime() - parsedResult.time;
      this.logger('Found cache entry', {
        cacheKey,
        time: parsedResult.time,
        renewedAgo,
        renewalKey: parsedResult.renewalKey,
        newRenewalKey: renewalKey,
        renewalThreshold,
        requestId: options.requestId,
        spanId,
        primaryQuery,
        renewCycle
      });
      if (
        renewalKey && (
          !renewalThreshold ||
          !parsedResult.time ||
          renewedAgo > renewalThreshold * 1000 ||
          parsedResult.renewalKey !== renewalKey
        )
      ) {
        if (options.waitForRenew) {
          this.logger('Waiting for renew', { cacheKey, renewalThreshold, requestId: options.requestId, spanId, primaryQuery, renewCycle });
          return fetchNew();
        } else {
          this.logger('Renewing existing key', { cacheKey, renewalThreshold, requestId: options.requestId, spanId, primaryQuery, renewCycle });
          fetchNew().catch(e => {
            if (!(e instanceof ContinueWaitError)) {
              this.logger('Error renewing', { cacheKey, error: e.stack || e, requestId: options.requestId, spanId, primaryQuery, renewCycle });
            }
          });
        }
      }
      this.logger('Using cache for', { cacheKey, requestId: options.requestId, spanId, primaryQuery, renewCycle });
      if (options.useInMemory && renewedAgo + inMemoryCacheDisablePeriod <= renewalThreshold * 1000) {
        this.memoryCache.set(redisKey, parsedResult);
      }
      return parsedResult.result;
    } else {
      this.logger('Missing cache for', { cacheKey, requestId: options.requestId, spanId, primaryQuery, renewCycle });
      return fetchNew();
    }
  }

  protected async lastRefreshTime(cacheKey) {
    const cachedValue = await this.cacheDriver.get(this.queryRedisKey(cacheKey));
    return cachedValue && new Date(cachedValue.time);
  }

  public async resultFromCacheIfExists(queryBody) {
    const cacheKey = QueryCache.queryCacheKey(queryBody);
    const cachedValue = await this.cacheDriver.get(this.queryRedisKey(cacheKey));
    if (cachedValue) {
      return {
        data: cachedValue.result,
        lastRefreshTime: new Date(cachedValue.time)
      };
    }
    return null;
  }

  public queryRedisKey(cacheKey: CacheKey): string {
    return this.getKey('SQL_QUERY_RESULT', getCacheHash(cacheKey) as any);
  }

  public async cleanup() {
    return this.cacheDriver.cleanup();
  }

  public async testConnection() {
    return this.cacheDriver.testConnection();
  }
}
