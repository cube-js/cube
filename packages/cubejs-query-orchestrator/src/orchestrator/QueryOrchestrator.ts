import R from 'ramda';

import { QueryCache } from './QueryCache';
import { PreAggregations } from './PreAggregations';
import { RedisPool, RedisPoolOptions } from './RedisPool';

interface QueryOrchestratorOptions {
  cacheAndQueueDriver?: 'redis' | 'memory';
  externalDriverFactory?: any;
  redisPoolOptions?: RedisPoolOptions;
  queryCacheOptions?: any;
  preAggregationsOptions?: any;
  rollupOnlyMode?: boolean;
}

export class QueryOrchestrator {
  protected readonly queryCache: QueryCache;

  protected readonly preAggregations: PreAggregations;

  protected readonly redisPool: RedisPool|undefined;

  protected readonly rollupOnlyMode: boolean;

  public constructor(
    protected readonly redisPrefix: string,
    protected readonly driverFactory: any,
    protected readonly logger: any,
    options: QueryOrchestratorOptions = {}
  ) {
    this.rollupOnlyMode = options.rollupOnlyMode;

    const cacheAndQueueDriver = options.cacheAndQueueDriver || process.env.CUBEJS_CACHE_AND_QUEUE_DRIVER || (
      process.env.NODE_ENV === 'production' || process.env.REDIS_URL ? 'redis' : 'memory'
    );

    if (!['redis', 'memory'].includes(cacheAndQueueDriver)) {
      throw new Error('Only \'redis\' or \'memory\' are supported for cacheAndQueueDriver option');
    }

    const redisPool = cacheAndQueueDriver === 'redis' ? new RedisPool(options.redisPoolOptions) : undefined;
    const { externalDriverFactory } = options;

    this.queryCache = new QueryCache(
      this.redisPrefix, this.driverFactory, this.logger, {
        externalDriverFactory,
        cacheAndQueueDriver,
        redisPool,
        ...options.queryCacheOptions,
      }
    );

    this.preAggregations = new PreAggregations(
      this.redisPrefix, this.driverFactory, this.logger, this.queryCache, {
        externalDriverFactory,
        cacheAndQueueDriver,
        redisPool,
        ...options.preAggregationsOptions
      }
    );
  }

  public async fetchQuery(queryBody: any) {
    return this.preAggregations.loadAllPreAggregationsIfNeeded(queryBody)
      .then(async preAggregationsTablesToTempTables => {
        const usedPreAggregations = R.fromPairs(preAggregationsTablesToTempTables);
        if (this.rollupOnlyMode && Object.keys(usedPreAggregations).length === 0) {
          throw new Error('No pre-aggregation exists for that query');
        }
        if (!queryBody.query) {
          return {
            usedPreAggregations
          };
        }
        const result = await this.queryCache.cachedQueryResult(
          queryBody, preAggregationsTablesToTempTables
        );
        return {
          ...result,
          usedPreAggregations
        };
      });
  }

  public async queryStage(queryBody: any) {
    const queue = this.preAggregations.getQueue();
    const preAggregationsQueryStageState = await queue.fetchQueryStageState();
    const pendingPreAggregationIndex =
      (await Promise.all(
        (queryBody.preAggregations || [])
          .map(p => queue.getQueryStage(
            PreAggregations.preAggregationQueryCacheKey(p), 10, preAggregationsQueryStageState
          ))
      )).findIndex(p => !!p);
    if (pendingPreAggregationIndex === -1) {
      return this.queryCache.getQueue().getQueryStage(QueryCache.queryCacheKey(queryBody));
    }
    const preAggregation = queryBody.preAggregations[pendingPreAggregationIndex];
    const preAggregationStage = await queue.getQueryStage(
      PreAggregations.preAggregationQueryCacheKey(preAggregation), undefined, preAggregationsQueryStageState
    );
    if (!preAggregationStage) {
      return undefined;
    }
    const stageMessage =
      `Building pre-aggregation ${pendingPreAggregationIndex + 1}/${queryBody.preAggregations.length}`;
    if (preAggregationStage.stage.indexOf('queue') !== -1) {
      return { ...preAggregationStage, stage: `${stageMessage}: ${preAggregationStage.stage}` };
    } else {
      return { ...preAggregationStage, stage: stageMessage };
    }
  }

  public resultFromCacheIfExists(queryBody: any) {
    return this.queryCache.resultFromCacheIfExists(queryBody);
  }

  public async testConnections() {
    // @todo Possible, We will allow to use different drivers for cache and queue, dont forget to add both
    return this.queryCache.testConnection();
  }

  public async cleanup() {
    return this.queryCache.cleanup();
  }
}
