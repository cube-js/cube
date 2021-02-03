/* eslint-disable no-throw-literal */
import pt from 'promise-timeout';
import { QueryOrchestrator, ContinueWaitError } from '@cubejs-backend/query-orchestrator';

export class OrchestratorApi {
  private seenDataSources: { [dataSource: string]: boolean } = {};
  
  protected readonly orchestrator: QueryOrchestrator;
  
  protected readonly externalDriverFactory: any;
  
  public constructor(protected driverFactory, protected logger, protected options: any = {}) {
    const { externalDriverFactory } = options;
    
    this.orchestrator = new QueryOrchestrator(options.redisPrefix || 'STANDALONE', driverFactory, logger, options);
    this.driverFactory = driverFactory;
    this.externalDriverFactory = externalDriverFactory;
    this.logger = logger;
  }

  public async executeQuery(query) {
    const queryForLog = query.query && query.query.replace(/\s+/g, ' ');
    const startQueryTime = (new Date()).getTime();
    
    try {
      this.logger('Query started', {
        query: queryForLog,
        params: query.values,
        requestId: query.requestId
      });

      let fetchQueryPromise = query.loadRefreshKeysOnly ?
        this.orchestrator.loadRefreshKeys(query) :
        this.orchestrator.fetchQuery(query);

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
        if (!query.renewQuery && fromCache && !query.scheduledRefresh) {
          this.logger('Slow Query Warning', {
            query: queryForLog,
            requestId: query.requestId,
            warning: 'Query is too slow to be renewed during the user request and was served from the cache. Please consider using low latency pre-aggregations.'
          });

          return {
            ...fromCache,
            slowQuery: true
          };
        }

        throw {
          error: 'Continue wait',
          stage: !query.scheduledRefresh ? await this.orchestrator.queryStage(query) : null
        };
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

  public async testConnection() {
    return Promise.all([
      ...Object.keys(this.seenDataSources).map(ds => this.testDriverConnection(this.driverFactory, ds)),
      this.testDriverConnection(this.externalDriverFactory)
    ]);
  }

  public async testOrchestratorConnections() {
    return this.orchestrator.testConnections();
  }

  public async testDriverConnection(driverFn, dataSource: string = 'default') {
    if (driverFn) {
      const driver = await driverFn(dataSource);
      await driver.testConnection();
    }
  }

  public async release() {
    return Promise.all([
      ...Object.keys(this.seenDataSources).map(ds => this.releaseDriver(this.driverFactory, ds)),
      this.releaseDriver(this.externalDriverFactory),
      this.orchestrator.cleanup()
    ]);
  }

  protected async releaseDriver(driverFn, dataSource: string = 'default') {
    if (driverFn) {
      const driver = await driverFn(dataSource);
      if (driver.release) {
        await driver.release();
      }
    }
  }

  public addDataSeenSource(dataSource) {
    this.seenDataSources[dataSource] = true;
  }
}
