/* eslint-disable no-throw-literal */
import * as stream from 'stream';
import pt from 'promise-timeout';
import {
  QueryOrchestrator,
  ContinueWaitError,
  DriverFactoryByDataSource,
  DriverType,
  QueryOrchestratorOptions,
  QueryBody,
} from '@cubejs-backend/query-orchestrator';

import { DbTypeAsyncFn, ExternalDbTypeFn, RequestContext } from './types';

export interface OrchestratorApiOptions extends QueryOrchestratorOptions {
  contextToDbType: DbTypeAsyncFn;
  contextToExternalDbType: ExternalDbTypeFn;
  redisPrefix?: string;
}

export class OrchestratorApi {
  private seenDataSources: Record<string, boolean> = {};

  protected readonly orchestrator: QueryOrchestrator;

  protected readonly continueWaitTimeout: number;

  public constructor(
    protected readonly driverFactory: DriverFactoryByDataSource,
    protected readonly logger,
    protected readonly options: OrchestratorApiOptions
  ) {
    this.continueWaitTimeout = this.options.continueWaitTimeout || 5;

    this.orchestrator = new QueryOrchestrator(
      options.redisPrefix || 'STANDALONE',
      driverFactory,
      logger,
      options
    );
  }

  /**
   * Returns QueryOrchestrator instance.
   */
  public getQueryOrchestrator(): QueryOrchestrator {
    return this.orchestrator;
  }

  /**
   * Force reconcile queue logic to be executed.
   */
  public async forceReconcile(datasource = 'default') {
    await this.orchestrator.forceReconcile(datasource);
  }

  /**
   * Returns stream object which will be used to stream results from
   * the data source if applicable. Throw otherwise.
   *
   * @throw Error
   */
  public async streamQuery(query: QueryBody): Promise<stream.Writable> {
    return this.orchestrator.streamQuery(query);
  }

  /**
   * Push query to the queue, fetch and return result if query takes
   * less than `continueWaitTimeout` seconds, throw `ContinueWaitError`
   * error otherwise.
   */
  public async executeQuery(query: QueryBody) {
    const queryForLog = query.query && query.query.replace(/\s+/g, ' ');
    const startQueryTime = (new Date()).getTime();

    try {
      this.logger('Query started', {
        query: queryForLog,
        params: query.values,
        requestId: query.requestId
      });

      let fetchQueryPromise = query.loadRefreshKeysOnly
        ? this.orchestrator.loadRefreshKeys(query)
        : this.orchestrator.fetchQuery(query);

      if (query.isJob) {
        // We want to immediately resolve and return a jobed build query result
        // (initialized by the /cubejs-system/v1/pre-aggregations/jobs endpoint)
        // because the following stack was optimized for such behavior.
        const job = await fetchQueryPromise;
        return job;
      }
      
      fetchQueryPromise = pt.timeout(fetchQueryPromise, this.continueWaitTimeout * 1000);

      const data = await fetchQueryPromise;

      this.logger('Query completed', {
        duration: ((new Date()).getTime() - startQueryTime),
        query: queryForLog,
        params: query.values,
        requestId: query.requestId
      });

      const extractDbType = async (response) => {
        const dbType = await this.options.contextToDbType({
          ...query.context,
          dataSource: response.dataSource,
        });
        return dbType;
      };

      const extractExternalDbType = (response) => (
        this.options.contextToExternalDbType({
          ...query.context,
          dataSource: response.dataSource,
        })
      );

      if (Array.isArray(data)) {
        const res = await Promise.all(
          data.map(async (item) => ({
            ...item,
            dbType: await extractDbType(item),
            extDbType: extractExternalDbType(item),
          }))
        );
        return res;
      }

      data.dbType = await extractDbType(data);
      data.extDbType = extractExternalDbType(data);

      return data;
    } catch (err) {
      if ((err instanceof pt.TimeoutError || err instanceof ContinueWaitError)) {
        this.logger('Continue wait', {
          duration: ((new Date()).getTime() - startQueryTime),
          query: queryForLog,
          params: query.values,
          requestId: query.requestId
        });

        const fromCache = await this
          .orchestrator
          .resultFromCacheIfExists(query);
        if (
          !query.renewQuery &&
          fromCache &&
          !query.scheduledRefresh
        ) {
          this.logger('Slow Query Warning', {
            query: queryForLog,
            requestId: query.requestId,
            warning: 'Query is too slow to be renewed during the ' +
              'user request and was served from the cache. Please ' +
              'consider using low latency pre-aggregations.'
          });

          return {
            ...fromCache,
            slowQuery: true
          };
        }

        throw {
          error: 'Continue wait',
          stage: !query.scheduledRefresh
            ? await this.orchestrator.queryStage(query)
            : null
        };
      }

      this.logger('Error querying db', {
        query: queryForLog,
        params: query.values,
        error: ((err as Error).stack || err),
        requestId: query.requestId
      });

      throw { error: err.toString() };
    }
  }

  public async testOrchestratorConnections() {
    return this.orchestrator.testConnections();
  }

  /**
   * Tests worker's connections to the Cubstore and, if not in the rollup only
   * mode, to the datasources.
   */
  public async testConnection() {
    if (this.options.rollupOnlyMode) {
      return Promise.all([
        this.testDriverConnection(this.options.externalDriverFactory, DriverType.External),
      ]);
    } else {
      return Promise.all([
        ...Object.keys(this.seenDataSources).map(
          ds => this.testDriverConnection(this.driverFactory, DriverType.Internal, ds),
        ),
        this.testDriverConnection(this.options.externalDriverFactory, DriverType.External),
      ]);
    }
  }

  /**
   * Tests connection to the data source specified by the driver factory
   * function and data source name.
   */
  public async testDriverConnection(
    driverFn?: DriverFactoryByDataSource,
    driverType?: DriverType,
    dataSource: string = 'default',
  ) {
    if (driverFn) {
      try {
        const driver = await driverFn(dataSource);
        await driver.testConnection();
        this.logger('Connection test completed successfully', {
          driverType,
          dataSource,
        });
      } catch (e: any) {
        e.driverType = driverType;
        throw e;
      }
    }
  }

  /**
   * Determines whether the partition table already exists or not.
   */
  public async isPartitionExist(
    request: string,
    external: boolean,
    dataSource = 'default',
    schema: string,
    table: string,
    key: any[],
    token: string,
  ): Promise<[boolean, string]> {
    return this.orchestrator.isPartitionExist(
      request,
      external,
      dataSource,
      schema,
      table,
      key,
      token,
    );
  }

  public async release() {
    return Promise.all([
      ...Object.keys(this.seenDataSources).map(ds => this.releaseDriver(this.driverFactory, ds)),
      this.releaseDriver(this.options.externalDriverFactory),
      this.orchestrator.cleanup()
    ]);
  }

  protected async releaseDriver(driverFn?: DriverFactoryByDataSource, dataSource: string = 'default') {
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

  public getPreAggregationVersionEntries(context: RequestContext, preAggregations, preAggregationsSchema) {
    return this.orchestrator.getPreAggregationVersionEntries(
      preAggregations,
      preAggregationsSchema,
      context.requestId
    );
  }

  public getPreAggregationPreview(context: RequestContext, preAggregation) {
    return this.orchestrator.getPreAggregationPreview(context.requestId, preAggregation);
  }

  public async expandPartitionsInPreAggregations(queryBody) {
    try {
      return await this.orchestrator.expandPartitionsInPreAggregations(queryBody);
    } catch (err) {
      if (err instanceof ContinueWaitError) {
        throw {
          error: 'Continue wait'
        };
      }
      throw err;
    }
  }

  public async checkPartitionsBuildRangeCache(queryBody) {
    return this.orchestrator.checkPartitionsBuildRangeCache(queryBody);
  }

  public async getPreAggregationQueueStates() {
    return this.orchestrator.getPreAggregationQueueStates();
  }

  public async cancelPreAggregationQueriesFromQueue(queryKeys: string[], dataSource: string) {
    return this.orchestrator.cancelPreAggregationQueriesFromQueue(queryKeys, dataSource);
  }

  public async subscribeQueueEvents(id, callback) {
    return this.orchestrator.subscribeQueueEvents(id, callback);
  }

  public async unSubscribeQueueEvents(id) {
    return this.orchestrator.unSubscribeQueueEvents(id);
  }
}
