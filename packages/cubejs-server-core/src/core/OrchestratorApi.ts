/* eslint-disable no-throw-literal */
import * as stream from 'stream';
import pt from 'promise-timeout';
import {
  ContinueWaitError,
  DriverFactory,
  DriverFactoryByDataSource,
  DriverType,
  QueryBody,
  QueryOrchestrator,
  QueryOrchestratorOptions,
} from '@cubejs-backend/query-orchestrator';

import { DatabaseType, RequestContext } from './types';

export interface OrchestratorApiOptions extends QueryOrchestratorOptions {
  contextToDbType: (dataSource: string) => Promise<DatabaseType>;
  contextToExternalDbType: () => DatabaseType;
  redisPrefix?: string;
}

/**
 * How long release() waits for the work that is already in progress. Past it the
 * connections are closed anyway: work that never finishes must not keep them
 * open for the rest of the process.
 */
const HOLDERS_TIMEOUT = 10 * 60 * 1000;

/**
 * How long the count has to stay at zero to mean the work is over. One logical
 * operation is several calls on the same api, and the count drops back to zero
 * between them.
 */
const HOLDERS_LINGER = 2 * 1000;

export class OrchestratorApi {
  private seenDataSources: Record<string, boolean> = {};

  private holders: number = 0;

  private drained: (() => void)[] = [];

  protected readonly holdersLinger: number = HOLDERS_LINGER;

  private externalDriverSeen: boolean = false;

  protected readonly externalDriverFactory?: DriverFactory;

  protected orchestrator: QueryOrchestrator;

  protected readonly continueWaitTimeout: number;

  public constructor(
    protected readonly driverFactory: DriverFactoryByDataSource,
    protected readonly logger,
    protected readonly options: OrchestratorApiOptions
  ) {
    this.continueWaitTimeout = this.options.continueWaitTimeout || 10;

    const { externalDriverFactory } = options;

    // Asking the factory for the driver is what creates it, so this is where the
    // tenant stops being one without an external connection. Everything that can
    // reach the external driver goes through this, release() included, so that it
    // doesn't open a connection just to close it.
    this.externalDriverFactory = externalDriverFactory && (async () => {
      this.externalDriverSeen = true;

      return externalDriverFactory();
    });

    this.orchestrator = new QueryOrchestrator(
      options.redisPrefix || 'STANDALONE',
      driverFactory,
      logger,
      { ...options, externalDriverFactory: this.externalDriverFactory }
    );
  }

  /**
   * Returns QueryOrchestrator instance.
   */
  public getQueryOrchestrator(): QueryOrchestrator {
    return this.orchestrator;
  }

  /**
   * Marks the api as being used until the returned function is called, which
   * holds off release(). The methods below do it themselves; a caller that
   * reaches the drivers through getQueryOrchestrator() has to do it by hand.
   */
  public acquire(): () => void {
    this.holders += 1;

    let released = false;

    return () => {
      if (released) {
        return;
      }

      released = true;
      this.holders -= 1;

      if (!this.holders) {
        const { drained } = this;
        this.drained = [];
        drained.forEach(resolve => resolve());
      }
    };
  }

  protected async hold<T>(work: () => Promise<T>): Promise<T> {
    const release = this.acquire();

    try {
      return await work();
    } finally {
      release();
    }
  }

  protected async waitForHolders(): Promise<void> {
    const deadline = Date.now() + HOLDERS_TIMEOUT;

    for (;;) {
      if (this.holders && !await this.waitForDrain(deadline - Date.now())) {
        this.logger('Orchestrator api released with work in progress', {
          warning: `${this.holders} operation(s) did not finish within ${HOLDERS_TIMEOUT / 1000}s, ` +
            'their connections are being closed anyway',
        });

        return;
      }

      await this.sleep(Math.min(this.holdersLinger, Math.max(deadline - Date.now(), 0)));

      if (!this.holders || Date.now() >= deadline) {
        return;
      }
    }
  }

  /**
   * Resolves true once nothing is holding the api, false if that didn't happen
   * within the timeout.
   */
  protected async waitForDrain(timeout: number): Promise<boolean> {
    let onDrained: () => void = () => undefined;
    let timer: ReturnType<typeof setTimeout> | undefined;

    try {
      return await new Promise<boolean>((resolve) => {
        onDrained = () => resolve(true);
        this.drained.push(onDrained);

        timer = setTimeout(() => resolve(false), Math.max(timeout, 0));
        timer.unref?.();
      });
    } finally {
      // Neither outcome needs the other one anymore, and both of them keep this
      // api reachable -- the timer from the timer queue, the resolver from the
      // array a later drain walks.
      clearTimeout(timer);
      this.drained = this.drained.filter(resolve => resolve !== onDrained);
    }
  }

  protected async sleep(ms: number): Promise<void> {
    if (!ms) {
      return;
    }

    await new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, ms);
      timer.unref?.();
    });
  }

  /**
   * Force reconcile queue logic to be executed.
   */
  public async forceReconcile(datasource = 'default') {
    await this.hold(() => this.orchestrator.forceReconcile(datasource));
  }

  /**
   * Returns stream object which will be used to stream results from
   * the data source if applicable. Throw otherwise.
   *
   * @throw Error
   */
  public async streamQuery(query: QueryBody): Promise<stream.Writable> {
    const release = this.acquire();

    try {
      // TODO merge with fetchQuery
      const result = await this.orchestrator.streamQuery(query);

      // The stream outlives this call, so the api stays held until the consumer
      // is done with it.
      stream.finished(result, () => release());

      return result;
    } catch (e) {
      release();

      throw e;
    }
  }

  /**
   * Push query to the queue, fetch and return result if query takes
   * less than `continueWaitTimeout` seconds, throw `ContinueWaitError`
   * error otherwise.
   */
  public async executeQuery(query: QueryBody) {
    return this.hold(() => this.runQuery(query));
  }

  protected async runQuery(query: QueryBody) {
    const queryForLog = query.query?.replace(/\s+/g, ' ');
    const startQueryTime = (new Date()).getTime();

    try {
      this.logger('Query started', {
        query: queryForLog,
        params: query.values,
        requestId: query.requestId
      });

      let fetchQueryPromise: Promise<any> = query.loadRefreshKeysOnly
        ? this.orchestrator.loadRefreshKeys(query)
        : this.orchestrator.fetchQuery(query);

      if (query.isJob) {
        // We want to immediately resolve and return a jobbed build query result
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

      if (Array.isArray(data)) {
        const res = await Promise.all(
          data.map(async (item) => ({
            ...item,
            dbType: await this.options.contextToDbType(item.dataSource),
            extDbType: this.options.contextToExternalDbType(),
          }))
        );
        return res;
      }

      data.dbType = await this.options.contextToDbType(data.dataSource);
      data.extDbType = this.options.contextToExternalDbType();

      return data;
    } catch (err) {
      if (err instanceof pt.TimeoutError || err instanceof ContinueWaitError) {
        this.logger('Continue wait', {
          duration: ((new Date()).getTime() - startQueryTime),
          query: queryForLog,
          params: query.values,
          requestId: query.requestId
        });

        if (query.scheduledRefresh) {
          throw {
            error: 'Continue wait',
            stage: null
          };
        }

        const fromCache = await this
          .orchestrator
          .resultFromCacheIfExists(query);

        if ((query.cacheMode === 'stale-if-slow' || query.cacheMode === 'stale-while-revalidate') && fromCache) {
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
          stage: await this.orchestrator.queryStage(query)
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
    return this.hold(() => this.orchestrator.testConnections());
  }

  /**
   * Tests worker's connections to the Cubestore and, if not in the rollup only
   * mode, to the datasources.
   */
  public async testConnection() {
    if (this.options.rollupOnlyMode) {
      return this.testDriverConnection(this.externalDriverFactory, DriverType.External);
    } else {
      return Promise.all([
        ...Object.keys(this.seenDataSources).map(
          ds => this.testDriverConnection(this.driverFactory, DriverType.Internal, ds),
        ),
        this.testDriverConnection(this.externalDriverFactory, DriverType.External),
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
      const release = this.acquire();

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
      } finally {
        release();
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
    key: any,
    token: string,
  ): Promise<[boolean, string]> {
    return this.hold(() => this.orchestrator.isPartitionExist(
      request,
      external,
      dataSource,
      schema,
      table,
      key,
      token,
    ));
  }

  public async release({ waitForWork = true }: { waitForWork?: boolean } = {}) {
    // Leaving the cache is not the same as being unused: whoever was handed this
    // api keeps it for the whole of their work, so the drivers can only be closed
    // once that work is done. A process that is shutting down is the exception:
    // there is nothing left to protect, and its own killer gives it seconds.
    if (waitForWork) {
      await this.waitForHolders();
    }

    return Promise.all([
      ...Object.keys(this.seenDataSources).map(ds => this.releaseDriver(this.driverFactory, ds)),
      // Only if there is something to release: the factory would otherwise open
      // a connection for a tenant that never had one.
      this.externalDriverSeen ? this.releaseDriver(this.externalDriverFactory) : undefined,
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

  public getPreAggregationVersionEntries(context: RequestContext, preAggregations, preAggregationsSchema): Promise<any> {
    return this.hold(() => this.orchestrator.getPreAggregationVersionEntries(
      preAggregations,
      preAggregationsSchema,
      context.requestId
    ));
  }

  public getPreAggregationPreview(context: RequestContext, preAggregation) {
    return this.hold(() => this.orchestrator.getPreAggregationPreview(context.requestId, preAggregation));
  }

  public async expandPartitionsInPreAggregations(queryBody) {
    try {
      return await this.hold(() => this.orchestrator.expandPartitionsInPreAggregations(queryBody));
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
    return this.hold(() => this.orchestrator.checkPartitionsBuildRangeCache(queryBody));
  }

  public async getPreAggregationQueueStates(dataSource?: string) {
    return this.hold(() => this.orchestrator.getPreAggregationQueueStates(dataSource));
  }

  public async cancelPreAggregationQueriesFromQueue(queryKeys: string[], dataSource: string) {
    return this.hold(() => this.orchestrator.cancelPreAggregationQueriesFromQueue(queryKeys, dataSource));
  }

  public async cancelQueryByRequestId(requestId: string) {
    return this.hold(() => this.orchestrator.cancelQueryByRequestId(requestId));
  }

  public async updateRefreshEndReached() {
    return this.hold(() => this.orchestrator.updateRefreshEndReached());
  }
}
