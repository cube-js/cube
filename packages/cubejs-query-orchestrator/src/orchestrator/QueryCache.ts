import crypto from 'crypto';
import LRUCache from 'lru-cache';
import { MaybeCancelablePromise } from '@cubejs-backend/shared';

import { QueryQueue } from './QueryQueue';
import { ContinueWaitError } from './ContinueWaitError';
import { RedisCacheDriver } from './RedisCacheDriver';
import { LocalCacheDriver } from './LocalCacheDriver';
import { CacheDriverInterface } from './cache-driver.interface';
import { DriverFactory, DriverFactoryByDataSource } from './DriverFactory';
import { BaseDriver } from '../driver';
import { PreAggregationDescription } from './PreAggregations';

type QueryOptions = {
  external?: boolean;
  renewalThreshold?: number;
  updateWindowSeconds?: number;
  renewalThresholdOutsideUpdateWindow?: number;
  incremental?: boolean;
};
export type QueryTuple = [sql: string, params: unknown[], options?: QueryOptions];
export type QueryWithParams = QueryTuple;
export type Query = {
  requestId?: string;
  dataSource: string;
  preAggregations?: PreAggregationDescription[];
  groupedPartitionPreAggregations?: PreAggregationDescription[][];
  preAggregationsLoadCacheByDataSource?: any;
  renewQuery?: boolean;
};

type CacheEntry = {
  time: number;
  result: any;
  renewalKey: string;
};

export class QueryCache {
  protected readonly cacheDriver: CacheDriverInterface;

  protected queue: { [dataSource: string]: QueryQueue } = {};

  protected externalQueue: QueryQueue | null = null;

  protected memoryCache: LRUCache<string, CacheEntry>;

  public constructor(
    protected readonly redisPrefix: string,
    protected readonly driverFactory: DriverFactoryByDataSource,
    protected readonly logger: any,
    public readonly options: {
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
      redisPool?: any;
      continueWaitTimeout?: number;
      cacheAndQueueDriver?: 'redis' | 'memory';
      maxInMemoryCacheEntries?: number;
      skipExternalCacheAndQueue?: boolean;
    } = {}
  ) {
    this.cacheDriver = options.cacheAndQueueDriver === 'redis' ?
      new RedisCacheDriver({ pool: options.redisPool }) :
      new LocalCacheDriver();
    this.memoryCache = new LRUCache<string, CacheEntry>({
      max: options.maxInMemoryCacheEntries || 10000
    });
  }

  /**
   * Force reconcile queue logic to be executed.
   */
  public async forceReconcile(datasource = 'default') {
    if (!this.externalQueue) {
      // We don't need to reconcile external queue, because Cube Store
      // uses its internal queue which managed separately.
      return;
    }
    const queue = await this.getQueue(datasource);
    if (queue) {
      await queue.reconcileQueue();
    }
  }

  public async cachedQueryResult(queryBody, preAggregationsTablesToTempTables) {
    const replacePreAggregationTableNames = (queryAndParams: QueryWithParams) => QueryCache
      .replacePreAggregationTableNames(
        queryAndParams, preAggregationsTablesToTempTables
      );

    const query = replacePreAggregationTableNames(queryBody.query);
    let queuePriority = 10;
    if (Number.isInteger(queryBody.queuePriority)) {
      // eslint-disable-next-line prefer-destructuring
      queuePriority = queryBody.queuePriority;
    }
    const forceNoCache = queryBody.forceNoCache || false;
    const { values } = queryBody;
    const cacheKeyQueries =
      this.cacheKeyQueriesFrom(queryBody).map(replacePreAggregationTableNames);

    const renewalThreshold = queryBody.cacheKeyQueries && queryBody.cacheKeyQueries.renewalThreshold;

    const expireSecs = this.getExpireSecs(queryBody);

    if (!cacheKeyQueries || queryBody.external && this.options.skipExternalCacheAndQueue) {
      return {
        data: await this.queryWithRetryAndRelease(query, values, {
          cacheKey: [query, values],
          external: queryBody.external,
          requestId: queryBody.requestId,
          dataSource: queryBody.dataSource
        }),
      };
    }

    const cacheKey = QueryCache.queryCacheKey(queryBody);

    if (queryBody.renewQuery) {
      this.logger('Requested renew', { cacheKey, requestId: queryBody.requestId });
      return this.renewQuery(query, values, cacheKeyQueries, expireSecs, cacheKey, renewalThreshold, {
        external: queryBody.external,
        requestId: queryBody.requestId,
        dataSource: queryBody.dataSource,
      });
    }

    if (!this.options.backgroundRenew) {
      const resultPromise = this.renewQuery(query, values, cacheKeyQueries, expireSecs, cacheKey, renewalThreshold, {
        external: queryBody.external,
        requestId: queryBody.requestId,
        dataSource: queryBody.dataSource,
        skipRefreshKeyWaitForRenew: true
      });

      this.startRenewCycle(query, values, cacheKeyQueries, expireSecs, cacheKey, renewalThreshold, {
        external: queryBody.external,
        requestId: queryBody.requestId,
        dataSource: queryBody.dataSource,
      });

      return resultPromise;
    }

    this.logger('Background fetch', { cacheKey, requestId: queryBody.requestId });

    const mainPromise = this.cacheQueryResult(
      query, values,
      cacheKey,
      expireSecs,
      {
        priority: queuePriority,
        forceNoCache,
        external: queryBody.external,
        requestId: queryBody.requestId,
        dataSource: queryBody.dataSource
      }
    );

    if (!forceNoCache) {
      this.startRenewCycle(query, values, cacheKeyQueries, expireSecs, cacheKey, renewalThreshold, {
        external: queryBody.external,
        requestId: queryBody.requestId,
        dataSource: queryBody.dataSource,
      });
    }

    return {
      data: await mainPromise,
      lastRefreshTime: await this.lastRefreshTime(cacheKey)
    };
  }

  private getExpireSecs(queryBody): number {
    return queryBody.expireSecs || 24 * 3600;
  }

  private cacheKeyQueriesFrom(queryBody): QueryWithParams[] {
    return queryBody.cacheKeyQueries && queryBody.cacheKeyQueries.queries ||
      queryBody.cacheKeyQueries ||
      [];
  }

  public static queryCacheKey(queryBody) {
    const key = [
      queryBody.query,
      queryBody.values,
      (queryBody.preAggregations || []).map(p => p.loadSql)
    ];
    if (queryBody.invalidate) {
      key.push(queryBody.invalidate);
    }
    return key;
  }

  protected static replaceAll(replaceThis, withThis, inThis) {
    withThis = withThis.replace(/\$/g, '$$$$');
    return inThis.replace(
      new RegExp(replaceThis.replace(/([/,!\\^${}[\]().*+?|<>\-&])/g, '\\$&'), 'g'),
      withThis
    );
  }

  public static replacePreAggregationTableNames(queryAndParams: QueryWithParams, preAggregationsTablesToTempTables) {
    const [keyQuery, params, queryOptions] = Array.isArray(queryAndParams) ? queryAndParams : [queryAndParams, []];
    const replacedKeqQuery = preAggregationsTablesToTempTables.reduce(
      (query, [tableName, { targetTableName }]) => QueryCache.replaceAll(tableName, targetTableName, query),
      keyQuery
    );
    return Array.isArray(queryAndParams) ? [replacedKeqQuery, params, queryOptions] : replacedKeqQuery;
  }

  public async queryWithRetryAndRelease(query, values, {
    priority, cacheKey, external, requestId, dataSource
  }: {
    priority?: number,
    cacheKey: object,
    external: boolean
    requestId?: string,
    dataSource: string
  }) {
    const queue = external
      ? this.getExternalQueue()
      : await this.getQueue(dataSource);
    return queue.executeInQueue('query', cacheKey, {
      queryKey: cacheKey, query, values, requestId
    }, priority, {
      stageQueryKey: cacheKey,
      requestId
    });
  }

  public async getQueue(dataSource: string = 'default') {
    if (!this.queue[dataSource]) {
      this.queue[dataSource] = QueryCache.createQueue(
        `SQL_QUERY_${this.redisPrefix}_${dataSource}`,
        () => this.driverFactory(dataSource),
        (client, q) => {
          this.logger('Executing SQL', {
            ...q
          });
          return client.query(q.query, q.values, q);
        },
        {
          logger: this.logger,
          cacheAndQueueDriver: this.options.cacheAndQueueDriver,
          redisPool: this.options.redisPool,
          // Centralized continueWaitTimeout that can be overridden in queueOptions
          continueWaitTimeout: this.options.continueWaitTimeout,
          ...(await this.options.queueOptions(dataSource)),
        }
      );
    }
    return this.queue[dataSource];
  }

  public getExternalQueue() {
    if (!this.externalQueue) {
      this.externalQueue = QueryCache.createQueue(
        `SQL_QUERY_EXT_${this.redisPrefix}`,
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
          redisPool: this.options.redisPool,
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
    executeFn: (client: BaseDriver, q: any) => any,
    options: Record<string, any> = {}
  ): QueryQueue {
    const queue: any = new QueryQueue(redisPrefix, {
      getQueueEventsBus: options.getQueueEventsBus,
      queryHandlers: {
        query: async (q, setCancelHandle) => {
          const client = await clientFactory();
          const resultPromise = executeFn(client, q);
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
        }
      },
      cancelHandlers: {
        query: async (q) => {
          if (q.cancelHandler && queue.handles[q.cancelHandler]) {
            await queue.handles[q.cancelHandler].cancel();
            delete queue.handles[q.cancelHandler];
          }
        }
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

  public startRenewCycle(query, values, cacheKeyQueries, expireSecs, cacheKey, renewalThreshold, options: {
    requestId?: string,
    skipRefreshKeyWaitForRenew?: boolean,
    external?: boolean,
    dataSource: string
  }) {
    this.renewQuery(
      query, values, cacheKeyQueries, expireSecs, cacheKey, renewalThreshold, options
    ).catch(e => {
      if (!(e instanceof ContinueWaitError)) {
        this.logger('Error while renew cycle', {
          query, query_values: values, error: e.stack || e, requestId: options.requestId
        });
      }
    });
  }

  public renewQuery(query, values, cacheKeyQueries, expireSecs, cacheKey, renewalThreshold, options: {
    requestId?: string,
    skipRefreshKeyWaitForRenew?: boolean,
    external?: boolean,
    dataSource: string
  }) {
    options = options || { dataSource: 'default' };
    return Promise.all(
      this.loadRefreshKeys(cacheKeyQueries, expireSecs, options)
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
            query, values,
            cacheKey,
            expireSecs,
            {
              renewalThreshold: renewalThreshold || 6 * 60 * 60,
              renewalKey: cacheKeyQueryResults && [
                cacheKeyQueries, cacheKeyQueryResults, this.queryRedisKey([query, values])
              ],
              waitForRenew: true,
              external: options.external,
              requestId: options.requestId,
              dataSource: options.dataSource
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
    options: {
      requestId?: string;
      skipRefreshKeyWaitForRenew?: boolean;
      dataSource: string
    }
  ) {
    return cacheKeyQueries.map((q) => {
      const [query, values, queryOptions]: QueryTuple = Array.isArray(q) ? q : [q, [], {}];

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
    });
  }

  public withLock = <T = any>(
    key: string,
    ttl: number,
    callback: () => MaybeCancelablePromise<T>,
  ) => this.cacheDriver.withLock(`lock:${key}`, callback, ttl, true);

  public async cacheQueryResult(query, values, cacheKey, expiration, options: {
    renewalThreshold?: number,
    renewalKey?: any,
    priority?: number,
    external?: boolean,
    requestId?: string,
    dataSource: string,
    waitForRenew?: boolean,
    forceNoCache?: boolean,
    useInMemory?: boolean,
  }) {
    options = options || { dataSource: 'default' };
    const { renewalThreshold } = options;
    const renewalKey = options.renewalKey && this.queryRedisKey(options.renewalKey);
    const redisKey = this.queryRedisKey(cacheKey);
    const fetchNew = () => (
      this.queryWithRetryAndRelease(query, values, {
        priority: options.priority,
        cacheKey,
        external: options.external,
        requestId: options.requestId,
        dataSource: options.dataSource
      }).then(res => {
        const result = {
          time: (new Date()).getTime(),
          result: res,
          renewalKey
        };
        return this.cacheDriver.set(redisKey, result, expiration)
          .then(({ bytes }) => {
            this.logger('Renewed', { cacheKey, requestId: options.requestId });
            this.logger('Outgoing network usage', {
              service: 'cache',
              requestId: options.requestId,
              bytes,
              cacheKey,
            });
            return res;
          });
      }).catch(e => {
        if (!(e instanceof ContinueWaitError)) {
          this.logger('Dropping Cache', { cacheKey, error: e.stack || e, requestId: options.requestId });
          this.cacheDriver.remove(redisKey)
            .catch(err => this.logger('Error removing key', {
              cacheKey,
              error: err.stack || err,
              requestId: options.requestId
            }));
        }
        throw e;
      })
    );

    if (options.forceNoCache) {
      this.logger('Force no cache for', { cacheKey, requestId: options.requestId });
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
          )
        ) {
          this.memoryCache.del(redisKey);
        } else {
          this.logger('Found in memory cache entry', {
            cacheKey,
            time: inMemoryValue.time,
            renewedAgo,
            renewalKey: inMemoryValue.renewalKey,
            newRenewalKey: renewalKey,
            renewalThreshold,
            requestId: options.requestId
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
        requestId: options.requestId
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
          this.logger('Waiting for renew', { cacheKey, renewalThreshold, requestId: options.requestId });
          return fetchNew();
        } else {
          this.logger('Renewing existing key', { cacheKey, renewalThreshold, requestId: options.requestId });
          fetchNew().catch(e => {
            if (!(e instanceof ContinueWaitError)) {
              this.logger('Error renewing', { cacheKey, error: e.stack || e, requestId: options.requestId });
            }
          });
        }
      }
      this.logger('Using cache for', { cacheKey, requestId: options.requestId });
      if (options.useInMemory && renewedAgo + inMemoryCacheDisablePeriod <= renewalThreshold * 1000) {
        this.memoryCache.set(redisKey, parsedResult);
      }
      return parsedResult.result;
    } else {
      this.logger('Missing cache for', { cacheKey, requestId: options.requestId });
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

  public queryRedisKey(cacheKey) {
    return `SQL_QUERY_RESULT_${this.redisPrefix}_${crypto.createHash('md5').update(JSON.stringify(cacheKey)).digest('hex')}`;
  }

  public async cleanup() {
    return this.cacheDriver.cleanup();
  }

  public async testConnection() {
    return this.cacheDriver.testConnection();
  }
}
