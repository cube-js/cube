import type { BaseDriver } from '@cubejs-backend/query-orchestrator';
import { getEnv, hasPreAggregationsEnvVars } from '@cubejs-backend/shared';

import type { SwappedDriverReleases } from './SwappedDriverReleases';
import type { DriverContext, LoggerFn, OrchestratorOptions, QueueOptions, RequestContext } from './types';

/** A failed swap to one driver version is not retried for this long. */
const SWAP_FAILURE_BACKOFF_MS = 10 * 1000;

/** Failing versions remembered, oldest dropped first. */
const MAX_FAILURES_KEPT = 16;

/** Builds and tests one driver. On failure it releases the driver unless `keep` says it is in use. */
export type BuildDriverFn = (context: DriverContext, keep?: (driver: BaseDriver) => boolean) => Promise<BaseDriver>;

export type OrchestratorDriversOptions = {
  orchestratorId: string;
  /** The context the orchestrator was created for; drivers are built from it until a swap. */
  context: RequestContext;
  /** The `driverVersion` of that context, if the option is set. */
  version: string | undefined;
  /** As configured, before defaults: read for the queues' execution timeouts. */
  orchestratorOptions: OrchestratorOptions;
  isCustomDriverFactory: boolean;
  buildDriver: BuildDriverFn;
  releases: SwappedDriverReleases;
  logger: LoggerFn;
};

/** A cached driver and what it was built for, so a swap can rebuild it. */
type CachedDriver = {
  driver: Promise<BaseDriver>;
  dataSource: string;
  preAggregations: boolean;
};

/**
 * One orchestrator's drivers by data source (`ds`, `ds@pre_agg`) and their `driverVersion`.
 * A request with another version starts a background rebuild of all of them from its
 * context, installed at once; requests keep the current drivers meanwhile.
 */
export class OrchestratorDrivers {
  protected readonly cache: Record<string, CachedDriver | null> = {};

  protected context: RequestContext;

  protected version: string | undefined;

  /** Never rejects: a failure is logged and backs off. */
  protected swapping: Promise<void> | undefined;

  /**
   * Each version's latest failed swap: it backs the version off, and while no version is
   * installed it is the error a request for that version gets.
   */
  protected readonly failures: Map<string, { at: number, error: unknown, reported: boolean }> = new Map();

  public constructor(protected readonly options: OrchestratorDriversOptions) {
    this.context = options.context;
    this.version = options.version;
  }

  /** The `DriverFactoryByDataSource` of the orchestrator. */
  public async getDriver(dataSource: string = 'default', preAggregations: boolean = false): Promise<BaseDriver> {
    const key = preAggregations ? `${dataSource}@pre_agg` : dataSource;

    if (this.cache[key]) {
      return this.cache[key].driver;
    }

    const { isCustomDriverFactory } = this.options;
    const hasSeparatePreAggEnv = hasPreAggregationsEnvVars(dataSource);

    if (preAggregations && hasSeparatePreAggEnv && isCustomDriverFactory) {
      this.options.logger('Pre-aggregation driver conflict', {
        error: 'Both driverFactory and PRE_AGGREGATIONS env vars are defined. driverFactory will take precedence.',
        dataSource,
      });
    }

    const usePreAgg = preAggregations && hasSeparatePreAggEnv && !isCustomDriverFactory;
    // No separate pre-agg driver needed: both keys share one entry, whichever is asked first
    const keys = hasSeparatePreAggEnv ? [key] : [dataSource, `${dataSource}@pre_agg`];

    const cached: CachedDriver = {
      dataSource,
      preAggregations: usePreAgg,
      driver: this.options.buildDriver({ ...this.context, dataSource, preAggregations: usePreAgg })
        .catch((e) => {
          keys.forEach((k) => {
            if (this.cache[k] === cached) {
              this.cache[k] = null;
            }
          });

          throw e;
        }),
    };

    keys.forEach((k) => {
      this.cache[k] = cached;
    });

    return cached.driver;
  }

  /**
   * Starts a swap when `requested` differs from the version the drivers were built for; the
   * request is served by the current drivers. Resolves false when the orchestrator cannot
   * serve it: `isLive` says it was evicted or released.
   */
  public async ensureVersion(requested: string, context: RequestContext, isLive: () => boolean): Promise<boolean> {
    if (requested === this.version) {
      return true;
    }

    if (!isLive()) {
      return false;
    }

    if (!Object.values(this.cache).some((cached) => cached)) {
      // Nothing to rebuild: build the first drivers from this context
      this.context = context;
      this.version = requested;

      return true;
    }

    this.requestSwap(requested, context, isLive);

    // Drivers built for no version, as from the readiness probe's empty context, may hold a fallback credential:
    // the request waits for a swap to install a version, and fails once the swap to its own version failed
    while (this.version === undefined && this.swapping && !this.failures.has(requested)) {
      await this.swapping;

      // The swap was another version's and installed nothing; this one was not queued, so ask now
      if (this.version === undefined && !this.failures.has(requested) && isLive()) {
        this.requestSwap(requested, context, isLive);
      }
    }

    if (!isLive()) {
      return false;
    }

    if (this.version === undefined) {
      const failure = this.failures.get(requested);

      throw failure
        ? failure.error
        : new Error('Drivers built for no version cannot serve a request with a driverVersion, and no swap installed one.');
    }

    return true;
  }

  /** Resolves once no swap is running. */
  public async settled(): Promise<void> {
    while (this.swapping) {
      await this.swapping;
    }
  }

  /**
   * Starts a swap unless one is running or `version` failed less than the backoff ago. A version
   * asked for during a swap is not queued: a later request asks again.
   */
  protected requestSwap(version: string, context: RequestContext, isLive: () => boolean) {
    const { orchestratorId } = this.options;
    const failure = this.failures.get(version);

    if (failure && Date.now() - failure.at < SWAP_FAILURE_BACKOFF_MS) {
      if (!failure.reported) {
        failure.reported = true;

        this.options.logger('Driver Swap Backoff', {
          orchestratorId,
          warning: `Swapping drivers to a new driverVersion failed less than ${SWAP_FAILURE_BACKOFF_MS / 1000}s ago. ` +
            'Serving with the previous drivers until it is retried.',
        });
      }

      return;
    }

    // An error kept from before the backoff passed is stale: a request waiting on another swap must not get it
    this.failures.delete(version);

    if (this.swapping) {
      return;
    }

    this.swapping = this.swap(version, context, isLive)
      .catch((error) => {
        this.failures.set(version, { at: Date.now(), error, reported: false });

        if (this.failures.size > MAX_FAILURES_KEPT) {
          this.failures.delete(this.failures.keys().next().value);
        }

        this.options.logger('Driver Swap Error', {
          orchestratorId,
          error: (error as Error).stack || String(error),
        });
      })
      .finally(() => {
        this.swapping = undefined;
      })
      // Started without being awaited, so a throwing logger must not become an unhandled rejection
      .catch((): undefined => undefined);
  }

  /**
   * Rebuilds every cached driver from `context` and installs all of them at once, or none: on
   * failure, or when `isLive` turns false before the install, the old drivers and version stay.
   */
  protected async swap(version: string, context: RequestContext, isLive: () => boolean): Promise<void> {
    const { releases } = this.options;
    const rebuilt = new Map<CachedDriver, BaseDriver>();
    // Drivers the cache held during the swap, with their data source
    const inUse = new Map<BaseDriver, string>();
    // Those, and drivers awaiting a delayed release, may still be serving queries
    const keep = (driver: BaseDriver) => inUse.has(driver) || releases.isPending(driver);
    const stale = () => [...new Set(Object.values(this.cache))].filter((cached) => cached && !rebuilt.has(cached));
    let failed: { reason: unknown } | undefined;

    // Repeated for drivers built lazily from the old context meanwhile; last checked in the same
    // synchronous step as the install, so none slips in
    do {
      const round = stale();
      const previous = await Promise.all(round.map((cached) => cached.driver.catch((): null => null)));
      previous.forEach((driver, i) => driver && inUse.set(driver, round[i].dataSource));

      const built = await Promise.allSettled(round.map(async ({ dataSource, preAggregations }, i) => (
        // A null previous failed its own build, which dropped it from the cache: nothing to rebuild
        previous[i] && this.options.buildDriver({ ...context, dataSource, preAggregations }, keep)
      )));

      built.forEach((result, i) => {
        if (previous[i] && result.status === 'fulfilled') {
          rebuilt.set(round[i], result.value);
        }
      });
      failed = built.find((result): result is PromiseRejectedResult => result.status === 'rejected');
    } while (!failed && isLive() && stale().length);

    const installed = new Set(rebuilt.values());
    const releaseAfter = new Map<BaseDriver, number>();

    if (!failed && isLive()) {
      try {
        // Worked out before the install: queueOptions is the user's function, and its throw must fail the swap whole
        inUse.forEach((dataSource, driver) => {
          if (!installed.has(driver)) {
            releaseAfter.set(driver, this.queryTimeoutSeconds(dataSource));
          }
        });
      } catch (reason) {
        failed = { reason };
      }
    }

    // Not live: released or evicted meanwhile, so nothing would ever release drivers installed into it
    if (failed || !isLive()) {
      await Promise.all(
        [...new Set(rebuilt.values())]
          .filter((driver) => !keep(driver))
          .map((driver) => releases.releaseSafely(driver)),
      );

      if (failed) {
        throw failed.reason;
      }

      return;
    }

    // Synchronous since the last `stale()` check, so no request sees some drivers swapped and others not
    rebuilt.forEach((driver, cached) => {
      cached.driver = Promise.resolve(driver);
    });

    // A caching driverFactory can hand back a driver awaiting its release, or the one it replaces: keep those open
    installed.forEach((driver) => releases.cancel(driver));
    releaseAfter.forEach((seconds, driver) => releases.schedule(driver, seconds));

    this.context = context;
    this.version = version;

    this.options.logger('Driver Swap', {
      orchestratorId: this.options.orchestratorId,
      dataSources: Object.keys(this.cache).filter((key) => this.cache[key]),
    });
  }

  /**
   * How long a query or pre-aggregation build on `dataSource` may run, in seconds: the queues'
   * executionTimeout, defaulting as QueryQueue does. Read raw, since resolving the queue options
   * computes concurrency, which can call driverFactory.
   */
  protected queryTimeoutSeconds(dataSource: string): number {
    const { orchestratorOptions } = this.options;
    const timeout = (queueOptions?: QueueOptions | ((ds: string) => QueueOptions)) => (
      (typeof queueOptions === 'function' ? queueOptions(dataSource) : queueOptions)?.executionTimeout ||
      getEnv('dbQueryTimeout')
    );

    return Math.max(
      timeout(orchestratorOptions.queryCacheOptions?.queueOptions),
      timeout(orchestratorOptions.preAggregationsOptions?.queueOptions),
    );
  }
}
