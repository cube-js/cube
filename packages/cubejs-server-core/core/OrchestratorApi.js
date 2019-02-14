const pt = require('promise-timeout');
const QueryOrchestrator = require('@cubejs-backend/query-orchestrator/orchestrator/QueryOrchestrator');
const ContinueWaitError = require('@cubejs-backend/query-orchestrator/orchestrator/ContinueWaitError');

class OrchestratorApi {
  constructor(driverFactory, logger, options) {
    options = options || {};
    this.orchestrator = new QueryOrchestrator(options.redisPrefix || 'STANDALONE', driverFactory, logger, options);
    this.logger = logger;
  }

  async executeQuery(query) {
    const queryForLog = query.query.replace(/\s+/g, ' ');
    const startQueryTime = (new Date()).getTime();

    try {
      this.logger('Query started', {
        query: queryForLog,
        params: query.values
      });

      let fetchQueryPromise = this.orchestrator.fetchQuery(query);

      fetchQueryPromise = pt.timeout(fetchQueryPromise, 5 * 1000);

      const data = await fetchQueryPromise;

      this.logger('Query completed', {
        duration: ((new Date()).getTime() - startQueryTime),
        query: queryForLog,
        params: query.values
      });

      return { data: data };
    } catch(err) {
      if ((err instanceof pt.TimeoutError || err instanceof ContinueWaitError)) {
        this.logger('Continue wait', {
          duration: ((new Date()).getTime() - startQueryTime),
          query: queryForLog,
          params: query.values
        });

        throw { error: 'Continue wait', stage: await this.orchestrator.queryStage(query) };
      }

      this.logger('Error querying db', {
        query: queryForLog,
        params: query.values,
        error: (err.stack || err)
      });

      throw { error: err.toString() };
    }
  }
}

module.exports = OrchestratorApi;