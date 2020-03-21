/* eslint-disable no-throw-literal */
const pt = require('promise-timeout');
const QueryOrchestrator = require('@cubejs-backend/query-orchestrator/orchestrator/QueryOrchestrator');
const ContinueWaitError = require('@cubejs-backend/query-orchestrator/orchestrator/ContinueWaitError');

class OrchestratorApi {
  constructor(driverFactory, logger, options) {
    options = options || {};
    this.options = options;
    this.orchestrator = new QueryOrchestrator(options.redisPrefix || 'STANDALONE', driverFactory, logger, options);
    this.driverFactory = driverFactory;
    const { externalDriverFactory } = options;
    this.externalDriverFactory = externalDriverFactory;
    this.logger = logger;
  }

  async executeQuery(query) {
    const queryForLog = query.query && query.query.replace(/\s+/g, ' ');
    const startQueryTime = (new Date()).getTime();

    try {
      this.logger('Query started', {
        query: queryForLog,
        params: query.values,
        requestId: query.requestId
      });

      let fetchQueryPromise = this.orchestrator.fetchQuery(query);

      fetchQueryPromise = pt.timeout(fetchQueryPromise, 5 * 1000);

      const data = await fetchQueryPromise;

      this.logger('Query completed', {
        duration: ((new Date()).getTime() - startQueryTime),
        query: queryForLog,
        params: query.values,
        requestId: query.requestId
      });

      return data;
    } catch (err) {
      if ((err instanceof pt.TimeoutError || err instanceof ContinueWaitError)) {
        this.logger('Continue wait', {
          duration: ((new Date()).getTime() - startQueryTime),
          query: queryForLog,
          params: query.values,
          requestId: query.requestId
        });

        const fromCache = await this.orchestrator.resultFromCacheIfExists(query);
        if (!query.renewQuery && fromCache) {
          this.logger('Slow Query Warning', {
            query: queryForLog,
            requestId: query.requestId,
            warning: `Query is too slow to be renewed during the user request and was served from the cache. Please consider using low latency pre-aggregations.`
          });
          return fromCache;
        }

        throw { error: 'Continue wait', stage: await this.orchestrator.queryStage(query) };
      }

      this.logger('Error querying db', {
        query: queryForLog,
        params: query.values,
        error: (err.stack || err),
        requestId: query.requestId
      });

      throw { error: err.toString() };
    }
  }

  testConnection() {
    return Promise.all([
      this.testDriverConnection(this.driverFactory),
      this.testDriverConnection(this.externalDriverFactory)
    ]);
  }

  async testDriverConnection(driverFn) {
    if (driverFn) {
      const driver = await driverFn();
      await driver.testConnection();
    }
  }

  release() {
    return Promise.all([
      this.releaseDriver(this.driverFactory),
      this.releaseDriver(this.externalDriverFactory),
      this.orchestrator.cleanup()
    ]);
  }

  async releaseDriver(driverFn) {
    if (driverFn) {
      const driver = await driverFn();
      if (driver.release) {
        await driver.release();
      }
    }
  }
}

module.exports = OrchestratorApi;
