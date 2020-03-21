const R = require('ramda');
const QueryCache = require('./QueryCache');
const PreAggregations = require('./PreAggregations');
const RedisPool = require('./RedisPool');

class QueryOrchestrator {
  constructor(redisPrefix, driverFactory, logger, options) {
    options = options || {};
    this.redisPrefix = redisPrefix;
    this.driverFactory = driverFactory;
    this.logger = logger;
    const { externalDriverFactory } = options;
    const cacheAndQueueDriver = options.cacheAndQueueDriver || process.env.CUBEJS_CACHE_AND_QUEUE_DRIVER || (
      process.env.NODE_ENV === 'production' || process.env.REDIS_URL ? 'redis' : 'memory'
    );
    if (cacheAndQueueDriver !== 'redis' && cacheAndQueueDriver !== 'memory') {
      throw new Error(`Only 'redis' or 'memory' are supported for cacheAndQueueDriver option`);
    }
    const redisPool = cacheAndQueueDriver === 'redis' ? new RedisPool() : undefined;

    this.redisPool = redisPool;
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

  async fetchQuery(queryBody) {
    return this.preAggregations.loadAllPreAggregationsIfNeeded(queryBody)
      .then(async preAggregationsTablesToTempTables => {
        const usedPreAggregations = R.fromPairs(preAggregationsTablesToTempTables);
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

  async queryStage(queryBody) {
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

  resultFromCacheIfExists(queryBody) {
    return this.queryCache.resultFromCacheIfExists(queryBody);
  }

  async cleanup() {
    if (this.redisPool) {
      await this.redisPool.cleanup();
    }
  }
}

module.exports = QueryOrchestrator;
