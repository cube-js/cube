/* eslint-disable no-throw-literal */
import * as stream from 'stream';
import pt from 'promise-timeout';
import {
  BaseDriver,
  ContinueWaitError,
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

/** A driver the factory has been asked for, and the promise it returned. */
type RequestedDriver = {
  dataSource: string;
  preAggregations: boolean;
  driver: Promise<BaseDriver>;
};

export class OrchestratorApi {
  private seenDataSources: Record<string, boolean> = {};

  /**
   * Every driver the factory has been asked to build. `seenDataSources` cannot
   * serve this purpose: it holds plain data source names only, so a dedicated
   * pre-aggregation driver — which the factory caches separately — was never
   * released or connection-tested. Only the pre-aggregation subsystem requests
   * those, and it does so lazily, so nothing but the factory itself knows they
   * exist.
   *
   * Keyed by what was *asked for*, which the schema bounds, rather than by the
   * promise handed back: an `async` factory returns a new promise per call, so
   * promise keys would grow one entry per query. Whether two requests share one
   * connection is then settled by the resolved instance, not by this key.
   */
  private requestedDrivers: Map<string, RequestedDriver> = new Map();

  protected orchestrator: QueryOrchestrator;

  protected readonly continueWaitTimeout: number;

  /**
   * Wraps the factory the orchestrator was built with, so every driver resolved
   * anywhere in the process is recorded. Kept in the `driverFactory` field, and
   * passed down to `QueryOrchestrator`, so the query path, the pre-aggregation
   * subsystem and `CompilerApi`'s data source probe all record through it.
   */
  protected readonly driverFactory: DriverFactoryByDataSource;

  public constructor(
    driverFactory: DriverFactoryByDataSource,
    protected readonly logger,
    protected readonly options: OrchestratorApiOptions
  ) {
    this.continueWaitTimeout = this.options.continueWaitTimeout || 10;

    this.driverFactory = this.recordingDriverFactory(driverFactory);

    this.orchestrator = new QueryOrchestrator(
      options.redisPrefix || 'STANDALONE',
      this.driverFactory,
      logger,
      options
    );
  }

  private recordingDriverFactory(driverFactory: DriverFactoryByDataSource): DriverFactoryByDataSource {
    return (dataSource = 'default', preAggregations = false) => {
      const driver = driverFactory(dataSource, preAggregations);

      // One entry per distinct request, overwriting the previous promise for the
      // same one: a re-request returns the same connection, and the factory owns
      // its cache, so the latest promise is the one worth holding.
      this.requestedDrivers.set(`${dataSource}${preAggregations ? '@pre_agg' : ''}`, {
        dataSource,
        preAggregations,
        driver: Promise.resolve(driver),
      });

      return driver;
    };
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
    // TODO merge with fetchQuery
    return this.orchestrator.streamQuery(query);
  }

  /**
   * Push query to the queue, fetch and return result if query takes
   * less than `continueWaitTimeout` seconds, throw `ContinueWaitError`
   * error otherwise.
   */
  public async executeQuery(query: QueryBody) {
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
    return this.orchestrator.testConnections();
  }

  /**
   * Tests worker's connections to the Cubestore and, if not in the rollup only
   * mode, to the datasources.
   */
  public async testConnection() {
    if (this.options.rollupOnlyMode) {
      return this.testDriverConnection(this.options.externalDriverFactory, DriverType.External);
    } else {
      // Requests that share a connection must not each cost a round-trip, and
      // only the resolved driver reveals which those are — the factory answers
      // a shared connection with one instance, whatever it was asked for.
      const tested = new Set<BaseDriver>();

      return Promise.all([
        ...this.connectionsToTest().map(
          ({ dataSource, preAggregations }) => this.testDriverConnection(
            this.driverFactory,
            DriverType.Internal,
            dataSource,
            preAggregations,
            tested,
          ),
        ),
        this.testDriverConnection(this.options.externalDriverFactory, DriverType.External),
      ]);
    }
  }

  /**
   * The data sources a connection test should cover: everything a driver has
   * been built for, plus anything announced through `addDataSeenSource` that
   * has not been built yet. The latter matters for the readiness probe, which
   * announces a data source and then tests it precisely to force the first
   * connection — on a fresh server nothing has been requested yet, so testing
   * only what was requested would report healthy without touching the database.
   */
  private connectionsToTest(): { dataSource: string, preAggregations: boolean }[] {
    const connections = [...this.requestedDrivers.values()].map(
      ({ dataSource, preAggregations }) => ({ dataSource, preAggregations })
    );

    Object.keys(this.seenDataSources).forEach((dataSource) => {
      // Matched against the query request specifically. A data source whose
      // pre-aggregation driver was built first has a record, but on dedicated
      // credentials that record is a different database — treating it as cover
      // would let the probe pass while the primary connection is unreachable.
      // When the two do share a connection, they resolve to one driver and the
      // duplicate costs nothing.
      const queryConnectionCovered = connections.some(
        connection => connection.dataSource === dataSource && !connection.preAggregations
      );

      if (!queryConnectionCovered) {
        connections.push({ dataSource, preAggregations: false });
      }
    });

    return connections;
  }

  /**
   * Tests connection to the data source specified by the driver factory
   * function and data source name.
   */
  public async testDriverConnection(
    driverFn?: DriverFactoryByDataSource,
    driverType?: DriverType,
    dataSource: string = 'default',
    preAggregations: boolean = false,
    tested?: Set<BaseDriver>,
  ) {
    if (driverFn) {
      try {
        const driver = await driverFn(dataSource, preAggregations);

        if (tested) {
          if (tested.has(driver)) {
            return;
          }
          tested.add(driver);
        }

        await driver.testConnection();
        this.logger('Connection test completed successfully', {
          driverType,
          dataSource,
          preAggregations,
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
    key: any,
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
    const requested = [...this.requestedDrivers.values()];

    // The api is spent once released, so stop tracking immediately; holding
    // closed drivers would keep their captured scope alive for as long as
    // anything references the api.
    this.requestedDrivers.clear();

    // Each driver is released as soon as it resolves, so one whose connection
    // never settles cannot hold up the others, the external driver, or the
    // orchestrator's own cleanup.
    //
    // Deliberately released through the recorded promise rather than the
    // factory: asking a factory for something you intend to destroy builds a
    // driver — and opens a connection — whenever the cached one is gone, which
    // is exactly the state a failed resolution leaves behind. A resolution that
    // failed has no driver to close, so it is skipped.
    const released = new Set<BaseDriver>();

    const results = await Promise.allSettled([
      ...requested.map(async ({ driver }) => {
        // A driver that never resolved was never built, so there is nothing to
        // close and nothing leaked — whatever failed the resolution was already
        // reported to whoever requested it.
        const resolved = await driver.catch(() => null);

        // A data source and its pre-aggregation request resolve to one driver
        // unless a dedicated connection is configured; close it once.
        if (!resolved || released.has(resolved)) {
          return;
        }
        released.add(resolved);

        await this.releaseDriverInstance(resolved);
      }),
      this.releaseDriver(this.options.externalDriverFactory),
      this.orchestrator.cleanup()
    ]);

    // Teardown runs to completion before anything is reported: one driver
    // refusing to close must not strand the rest. But the failure still has to
    // reach the caller — shutdown exits non-zero on it, and a reset must not
    // reload over state it could not release.
    const errors = results
      .filter((result): result is PromiseRejectedResult => result.status === 'rejected')
      .map(result => result.reason);

    if (errors.length) {
      errors.forEach((error) => {
        this.logger('Error during release', {
          error: (error as Error)?.stack || error,
        });
      });

      throw errors[0];
    }
  }

  protected async releaseDriver(driverFn?: DriverFactoryByDataSource, dataSource: string = 'default') {
    if (driverFn) {
      const driver = await driverFn(dataSource);
      await this.releaseDriverInstance(driver);
    }
  }

  private async releaseDriverInstance(driver: BaseDriver) {
    if (driver.release) {
      await driver.release();
    }
  }

  public addDataSeenSource(dataSource) {
    this.seenDataSources[dataSource] = true;
  }

  public getPreAggregationVersionEntries(context: RequestContext, preAggregations, preAggregationsSchema): Promise<any> {
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

  public async getPreAggregationQueueStates(dataSource?: string) {
    return this.orchestrator.getPreAggregationQueueStates(dataSource);
  }

  public async cancelPreAggregationQueriesFromQueue(queryKeys: string[], dataSource: string) {
    return this.orchestrator.cancelPreAggregationQueriesFromQueue(queryKeys, dataSource);
  }

  public async cancelQueryByRequestId(requestId: string) {
    return this.orchestrator.cancelQueryByRequestId(requestId);
  }

  public async updateRefreshEndReached() {
    return this.orchestrator.updateRefreshEndReached();
  }
}
