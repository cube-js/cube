const QueryCache = require('./QueryCache');
const PreAggregations = require('./PreAggregations');

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

    this.queryCache = new QueryCache(
      this.redisPrefix, this.driverFactory, this.logger, {
        externalDriverFactory,
        cacheAndQueueDriver,
        ...options.queryCacheOptions,
      }
    );
    this.preAggregations = new PreAggregations(
      this.redisPrefix, this.driverFactory, this.logger, this.queryCache, {
        externalDriverFactory,
        cacheAndQueueDriver,
        ...options.preAggregationsOptions
      }
    );
  }

  async fetchQuery(queryBody) {
    return this.preAggregations.loadAllPreAggregationsIfNeeded(queryBody)
      .then(preAggregationsTablesToTempTables => this.queryCache.cachedQueryResult(
        queryBody, preAggregationsTablesToTempTables
      ));
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
}

module.exports = QueryOrchestrator;
