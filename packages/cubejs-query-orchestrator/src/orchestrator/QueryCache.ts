import crypto from 'crypto';

import { QueryQueue } from './QueryQueue';
import { ContinueWaitError } from './ContinueWaitError';
import { RedisCacheDriver } from './RedisCacheDriver';
import { LocalCacheDriver } from './LocalCacheDriver';
import { CacheDriverInterface } from './cache-driver.interface';
import { DriverFactory, DriverFactoryByDataSource } from './DriverFactory';
import { DynamoDBCacheDriver } from './DynamoDBCacheDriver';

export class QueryCache {
  protected readonly cacheDriver: CacheDriverInterface;

  protected queue: { [dataSource: string]: QueryQueue } = {};

  protected externalQueue: QueryQueue|null = null;

  public constructor(
    protected readonly redisPrefix: string,
    protected readonly driverFactory: DriverFactoryByDataSource,
    protected readonly logger: any,
    protected readonly options: {
      refreshKeyRenewalThreshold?: number;
      externalQueueOptions?: any;
      externalDriverFactory?: DriverFactory;
      backgroundRenew?: Boolean;
      queueOptions?: object | ((dataSource: String) => object);
      redisPool?: any;
      cacheAndQueueDriver?: 'redis' | 'memory' | 'dynamodb';
    } = {}
  ) {
    switch (options.cacheAndQueueDriver) {
      case 'redis':
        this.cacheDriver = new RedisCacheDriver({ pool: options.redisPool });
        break;
      case 'dynamodb':
        this.cacheDriver = new DynamoDBCacheDriver();
        break;
      case 'memory':
      default:
        this.cacheDriver = new LocalCacheDriver();
    }
  }

  public async cachedQueryResult(queryBody, preAggregationsTablesToTempTables) {
    const replacePreAggregationTableNames = (queryAndParams) => QueryCache.replacePreAggregationTableNames(
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
      (
        queryBody.cacheKeyQueries && queryBody.cacheKeyQueries.queries ||
        queryBody.cacheKeyQueries ||
        []
      ).map(replacePreAggregationTableNames);

    const renewalThreshold = queryBody.cacheKeyQueries && queryBody.cacheKeyQueries.renewalThreshold;
    const refreshKeyRenewalThresholds = queryBody.cacheKeyQueries &&
      queryBody.cacheKeyQueries.refreshKeyRenewalThresholds;

    const expireSecs = queryBody.expireSecs || 24 * 3600;

    if (!cacheKeyQueries) {
      return {
        data: await this.queryWithRetryAndRelease(query, values, {
          cacheKey: [query, values],
          external: queryBody.external,
          requestId: queryBody.requestId,
          dataSource: queryBody.dataSource
        })
      };
    }

    const cacheKey = QueryCache.queryCacheKey(queryBody);

    if (queryBody.renewQuery) {
      this.logger('Requested renew', { cacheKey, requestId: queryBody.requestId });
      return this.renewQuery(query, values, cacheKeyQueries, expireSecs, cacheKey, renewalThreshold, {
        external: queryBody.external,
        requestId: queryBody.requestId,
        dataSource: queryBody.dataSource,
        refreshKeyRenewalThresholds
      });
    }

    if (!this.options.backgroundRenew) {
      const resultPromise = this.renewQuery(query, values, cacheKeyQueries, expireSecs, cacheKey, renewalThreshold, {
        external: queryBody.external,
        requestId: queryBody.requestId,
        dataSource: queryBody.dataSource,
        refreshKeyRenewalThresholds,
        skipRefreshKeyWaitForRenew: true
      });

      this.startRenewCycle(query, values, cacheKeyQueries, expireSecs, cacheKey, renewalThreshold, {
        external: queryBody.external,
        requestId: queryBody.requestId,
        refreshKeyRenewalThresholds
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
        refreshKeyRenewalThresholds
      });
    }

    return {
      data: await mainPromise,
      lastRefreshTime: await this.lastRefreshTime(cacheKey)
    };
  }

  public static queryCacheKey(queryBody) {
    return [queryBody.query, queryBody.values, (queryBody.preAggregations || []).map(p => p.loadSql)];
  }

  protected static replaceAll(replaceThis, withThis, inThis) {
    withThis = withThis.replace(/\$/g, '$$$$');
    return inThis.replace(
      new RegExp(replaceThis.replace(/([/,!\\^${}[\]().*+?|<>\-&])/g, '\\$&'), 'g'),
      withThis
    );
  }

  public static replacePreAggregationTableNames(queryAndParams, preAggregationsTablesToTempTables) {
    const [keyQuery, params] = Array.isArray(queryAndParams) ? queryAndParams : [queryAndParams, []];
    const replacedKeqQuery = preAggregationsTablesToTempTables.reduce(
      (query, [tableName, { targetTableName }]) => QueryCache.replaceAll(tableName, targetTableName, query),
      keyQuery
    );
    return Array.isArray(queryAndParams) ? [replacedKeqQuery, params] : replacedKeqQuery;
  }

  public queryWithRetryAndRelease(query, values, {
    priority, cacheKey, external, requestId, dataSource
  }: {
    priority?: number,
    cacheKey: object,
    external: boolean
    requestId?: string,
    dataSource: string
  }) {
    const queue = external ? this.getExternalQueue() : this.getQueue(dataSource);
    return queue.executeInQueue('query', cacheKey, {
      queryKey: cacheKey, query, values, requestId
    }, priority, {
      stageQueryKey: cacheKey,
      requestId
    });
  }

  public getQueue(dataSource: string) {
    dataSource = dataSource || 'default';
    if (!this.queue[dataSource]) {
      this.queue[dataSource] = QueryCache.createQueue(
        `SQL_QUERY_${this.redisPrefix}_${dataSource}`,
        () => this.driverFactory(dataSource),
        (client, q) => {
          this.logger('Executing SQL', {
            ...q
          });
          return client.query(q.query, q.values, q);
        }, {
          logger: this.logger,
          cacheAndQueueDriver: this.options.cacheAndQueueDriver,
          redisPool: this.options.redisPool,
          ...(typeof this.options.queueOptions === 'function' ?
            this.options.queueOptions(dataSource) :
            this.options.queueOptions
          )
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
          ...this.options.externalQueueOptions
        }
      );
    }
    return this.externalQueue;
  }

  public static createQueue(redisPrefix, clientFactory: DriverFactory, executeFn, options) {
    options = options || {};
    const queue: any = new QueryQueue(redisPrefix, {
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

  public startRenewCycle(query, values, cacheKeyQueries, expireSecs, cacheKey, renewalThreshold, options) {
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
    refreshKeyRenewalThresholds?: Array<number>
    dataSource: string
  }) {
    options = options || { dataSource: 'default' };
    return Promise.all(
      cacheKeyQueries.map((q, i) => this.cacheQueryResult(
        Array.isArray(q) ? q[0] : q,
        Array.isArray(q) ? q[1] : [],
        q,
        expireSecs,
        {
          renewalThreshold:
            this.options.refreshKeyRenewalThreshold ||
            (options.refreshKeyRenewalThresholds || [])[i] ||
            2 * 60,
          renewalKey: q,
          waitForRenew: !options.skipRefreshKeyWaitForRenew,
          requestId: options.requestId,
          dataSource: options.dataSource
        }
      ))
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

  public cacheQueryResult(query, values, cacheKey, expiration, options: {
    renewalThreshold?: number,
    renewalKey?: object,
    priority?: number,
    external?: boolean,
    requestId?: string,
    dataSource: string,
    waitForRenew?: boolean,
    forceNoCache?: boolean,
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
          .then(() => {
            this.logger('Renewed', { cacheKey, requestId: options.requestId });
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

    return this.cacheDriver.get(redisKey).then(res => {
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
        return parsedResult.result;
      } else {
        this.logger('Missing cache for', { cacheKey, requestId: options.requestId });
        return fetchNew();
      }
    });
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
