import type { BaseDriver } from '@cubejs-backend/query-orchestrator';
import { getEnv, hasPreAggregationsEnvVars } from '@cubejs-backend/shared';

import type { SwappedDriverReleases } from './SwappedDriverReleases';
import type { LoggerFn, OrchestratorOptions, QueueOptions, RequestContext } from './types';

/** A failed swap to one driver version is not retried for this long. */
const SWAP_FAILURE_BACKOFF_MS = 10 * 1000;

/** A swap back to the version replaced less than this long before the swap started is reported as ping-pong. */
const SWAP_PING_PONG_MS = 5 * 1000;

/** Builds and tests one driver. On failure it releases the driver unless `keep` says it is in use. */
export type BuildDriverFn = (
  context: RequestContext,
  dataSource: string,
  preAggregations: boolean,
  keep?: (driver: BaseDriver) => boolean,
) => Promise<BaseDriver>;

export type OrchestratorDriversOptions = {
  orchestratorId: string;
  /** The context the orchestrator was created for; drivers are built from it until a swap. */
  context: RequestContext;
  /** The `driverVersion` of that context, if the option is set. */
  version: string | undefined;
  /** As configured, before defaults: read for the queues' execution timeouts. */
  orchestratorOptions: OrchestratorOptions;
  isCustomDriverFactory: () => boolean;
  buildDriver: BuildDriverFn;
  releases: SwappedDriverReleases;
  logger: LoggerFn;
};

/** Whether the orchestrator can serve the request, or was evicted or released meanwhile. */
export type EnsureVersionResult = 'ready' | 'evicted';

type DriverSpec = { dataSource: string, preAggregations: boolean };

/** One rebuild of every cached driver, from the context that asked for a new version. */
export type SwapRun = {
  context: RequestContext;
  replacements: Map<Promise<BaseDriver>, { previous: BaseDriver | null, next: BaseDriver | null }>;
  replacedDataSources: Map<BaseDriver, string>;
  /** Drivers the cache held when the swap started; they may still be serving queries. */
  inUse: Set<BaseDriver>;
  keep: (driver: BaseDriver) => boolean;
};

type Installation = {
  installed: Set<BaseDriver>;
  replaced: Set<BaseDriver>;
  /** Built, but no cache key still pointed at the driver they replace. */
  discarded: Set<BaseDriver>;
};

/**
 * The drivers of one orchestrator, keyed by data source (`ds`, `ds@pre_agg`), and the
 * `driverVersion` they were built for. When a request brings another version, every
 * cached driver is rebuilt from that request's context and swapped in at once.
 */
export class OrchestratorDrivers {
  protected readonly drivers: Record<string, Promise<BaseDriver> | null> = {};

  /** What each `drivers` key was built for, so a swap can rebuild it. */
  protected readonly specs: Record<string, DriverSpec> = {};

  protected context: RequestContext;

  protected version: string | undefined;

  protected previousVersion: string | undefined;

  protected lastSwapAt: number | undefined;

  protected swapping: Promise<void> | undefined;

  protected lastFailure: { version: string, at: number, reported: boolean } | undefined;

  public constructor(protected readonly options: OrchestratorDriversOptions) {
    this.context = options.context;
    this.version = options.version;
  }

  public get orchestratorId(): string {
    return this.options.orchestratorId;
  }

  /** The `DriverFactoryByDataSource` of the orchestrator. */
  public async getDriver(dataSource: string = 'default', preAggregations: boolean = false): Promise<BaseDriver> {
    const factoryKey = preAggregations ? `${dataSource}@pre_agg` : dataSource;
    const cached = this.drivers[factoryKey];

    if (cached) {
      return cached;
    }

    const hasSeparatePreAggEnv = hasPreAggregationsEnvVars(dataSource);
    const spec = this.driverSpec(dataSource, preAggregations, hasSeparatePreAggEnv);
    // No separate pre-agg driver needed: both keys share one promise, whichever is asked first
    const keys = hasSeparatePreAggEnv ? [factoryKey] : [dataSource, `${dataSource}@pre_agg`];

    const pending = this.options.buildDriver(this.context, dataSource, spec.preAggregations)
      .catch((e) => {
        this.forget(keys, pending);

        throw e;
      });

    keys.forEach((key) => {
      this.drivers[key] = pending;
      this.specs[key] = spec;
    });

    return pending;
  }

  /**
   * Swaps the drivers when `requested` differs from the version they were built for.
   * Only the request that starts a failing swap sees its error; others keep the old
   * drivers, and a failed version is not retried until the backoff passes.
   */
  public async ensureVersion(
    requested: string,
    context: RequestContext,
    isLive: () => boolean,
  ): Promise<EnsureVersionResult> {
    while (requested !== this.version) {
      if (!isLive()) {
        return 'evicted';
      }

      if (this.swapping) {
        await this.swapping.catch(() => undefined);
      } else if (this.isBackingOff(requested)) {
        return 'ready';
      } else {
        await this.startSwap(context, requested, isLive);
      }
    }

    return 'ready';
  }

  protected driverSpec(dataSource: string, preAggregations: boolean, hasSeparatePreAggEnv: boolean): DriverSpec {
    const isCustomDriverFactory = this.options.isCustomDriverFactory();

    if (preAggregations && hasSeparatePreAggEnv && isCustomDriverFactory) {
      this.options.logger('Pre-aggregation driver conflict', {
        error: 'Both driverFactory and PRE_AGGREGATIONS env vars are defined. driverFactory will take precedence.',
        dataSource,
      });
    }

    return {
      dataSource,
      preAggregations: preAggregations && hasSeparatePreAggEnv && !isCustomDriverFactory,
    };
  }

  /** Drops a failed build from the cache, unless a swap already replaced it. */
  protected forget(keys: string[], failed: Promise<BaseDriver>) {
    keys.forEach((key) => {
      if (this.drivers[key] === failed) {
        this.drivers[key] = null;
      }
    });
  }

  protected isBackingOff(requested: string): boolean {
    const { lastFailure } = this;

    if (!lastFailure || lastFailure.version !== requested || Date.now() - lastFailure.at >= SWAP_FAILURE_BACKOFF_MS) {
      return false;
    }

    if (!lastFailure.reported) {
      lastFailure.reported = true;

      this.options.logger('Driver Swap Backoff', {
        orchestratorId: this.options.orchestratorId,
        warning: `Swapping drivers to a new driverVersion failed less than ${SWAP_FAILURE_BACKOFF_MS / 1000}s ago. ` +
          'Serving with the previous drivers until it is retried.',
      });
    }

    return true;
  }

  protected startSwap(context: RequestContext, version: string, isLive: () => boolean): Promise<void> {
    const swap = this.swap(context, version, isLive)
      .then(
        () => {
          this.lastFailure = undefined;
        },
        (error) => {
          this.recordFailure(version, error);

          throw error;
        },
      )
      .finally(() => {
        this.swapping = undefined;
      });

    this.swapping = swap;

    return swap;
  }

  protected recordFailure(version: string, error: unknown) {
    this.lastFailure = { version, at: Date.now(), reported: false };

    this.options.logger('Driver Swap Error', {
      orchestratorId: this.options.orchestratorId,
      error: (error as Error).stack || String(error),
    });
  }

  /**
   * Rebuilds every cached driver from `context` and installs all of them at once, or
   * none: on failure the old drivers and version stay. Resolves false, installing
   * nothing, when `isLive` turns false before the install.
   */
  protected async swap(context: RequestContext, version: string, isLive: () => boolean): Promise<boolean> {
    // Before the build: a slow testConnection() must not stretch the gap past the ping-pong window
    const startedAt = Date.now();
    const run = this.newSwapRun(context);

    try {
      // Last checked in the same synchronous step as the install, so no driver built from the old context slips in
      do {
        await this.rebuildAll(run);
      } while (isLive() && this.unswapped(run).size);
    } catch (e) {
      await this.discardBuilt(run);

      throw e;
    }

    // Released or evicted meanwhile, so nothing would ever release drivers installed into it
    if (!isLive()) {
      await this.discardBuilt(run);

      return false;
    }

    const installation = this.install(run);
    this.settleOldDrivers(run, installation);
    this.warnIfPingPong(version, startedAt);
    this.recordSwap(context, version);

    return true;
  }

  protected newSwapRun(context: RequestContext): SwapRun {
    const inUse = new Set<BaseDriver>();

    return {
      context,
      replacements: new Map(),
      replacedDataSources: new Map(),
      inUse,
      // Replaced drivers, and those awaiting a delayed release, may still be serving queries
      keep: (driver) => inUse.has(driver) || this.options.releases.isPending(driver),
    };
  }

  /** Repeated because a driver built lazily during the swap used the old context. */
  protected async rebuildAll(run: SwapRun) {
    for (let pending = this.unswapped(run); pending.size; pending = this.unswapped(run)) {
      await this.rebuildRound(run, pending);
    }
  }

  /** Cached driver promises the swap has not rebuilt yet, each with one key that holds it. */
  protected unswapped(run: SwapRun): Map<Promise<BaseDriver>, string> {
    const firstKeys = new Map<Promise<BaseDriver>, string>();

    Object.keys(this.drivers).forEach((key) => {
      const promise = this.drivers[key];

      if (promise && !run.replacements.has(promise) && !firstKeys.has(promise)) {
        firstKeys.set(promise, key);
      }
    });

    return firstKeys;
  }

  protected async rebuildRound(run: SwapRun, pending: Map<Promise<BaseDriver>, string>) {
    const promises = [...pending.keys()];
    const keys = [...pending.values()];
    const previous = await Promise.all(promises.map((promise) => promise.catch(() => null)));

    previous.forEach((driver) => driver && run.inUse.add(driver));

    const built = await Promise.allSettled(
      promises.map((_, i) => this.rebuildOne(run, previous[i], keys[i])),
    );

    built.forEach((result, i) => {
      if (result.status === 'fulfilled') {
        run.replacements.set(promises[i], { previous: previous[i], next: result.value });
      }
    });

    const failure = built.find((result): result is PromiseRejectedResult => result.status === 'rejected');

    if (failure) {
      throw failure.reason;
    }
  }

  protected async rebuildOne(run: SwapRun, previous: BaseDriver | null, key: string): Promise<BaseDriver | null> {
    // Its own build failed and dropped it from the cache
    if (!previous) {
      return null;
    }

    const spec = this.specs[key];
    run.replacedDataSources.set(previous, spec.dataSource);

    return this.options.buildDriver(run.context, spec.dataSource, spec.preAggregations, run.keep);
  }

  protected async discardBuilt(run: SwapRun) {
    const built = new Set([...run.replacements.values()].map(({ next }) => next));

    await Promise.all(
      [...built]
        .filter((next): next is BaseDriver => !!next && !run.keep(next))
        .map((next) => this.options.releases.releaseSafely(next)),
    );
  }

  /** Synchronous, so no request sees some keys swapped and others not. */
  protected install(run: SwapRun): Installation {
    const installation: Installation = { installed: new Set(), replaced: new Set(), discarded: new Set() };

    run.replacements.forEach(({ previous, next }, previousPromise) => {
      const keys = Object.keys(this.drivers).filter((key) => this.drivers[key] === previousPromise);

      // A null previous means its build failed, so there is no next either
      if (!next || !previous) {
        return;
      }

      if (!keys.length) {
        installation.discarded.add(next);
        return;
      }

      const nextPromise = Promise.resolve(next);

      keys.forEach((key) => {
        this.drivers[key] = nextPromise;
      });

      installation.installed.add(next);
      installation.replaced.add(previous);
    });

    return installation;
  }

  /** Releases replaced drivers after the query timeout, and unused new ones at once. */
  protected settleOldDrivers(run: SwapRun, { installed, replaced, discarded }: Installation) {
    const { releases } = this.options;

    installed.forEach((driver) => releases.cancel(driver));

    replaced.forEach((driver) => {
      if (!installed.has(driver)) {
        releases.schedule(driver, this.queryTimeoutSeconds(run.replacedDataSources.get(driver) ?? 'default'));
      }
    });

    // Never handed out, so nothing can be using them
    discarded.forEach((driver) => {
      if (!installed.has(driver) && !run.keep(driver)) {
        releases.releaseSafely(driver);
      }
    });
  }

  /** Two forward swaps in a row are normal (two credentials rotating a moment apart); flipping back is not. */
  protected warnIfPingPong(version: string, startedAt: number) {
    if (
      version === this.previousVersion &&
      this.lastSwapAt !== undefined &&
      startedAt - this.lastSwapAt < SWAP_PING_PONG_MS
    ) {
      this.options.logger('Driver Swap Ping-Pong', {
        orchestratorId: this.options.orchestratorId,
        warning: `Drivers were swapped back to the version they replaced within ${SWAP_PING_PONG_MS / 1000}s. contextToOrchestratorId must give ` +
          'contexts with different driverVersion values different ids, or they keep replacing each other\'s drivers.',
      });
    }
  }

  protected recordSwap(context: RequestContext, version: string) {
    this.context = context;
    this.previousVersion = this.version;
    this.version = version;
    this.lastSwapAt = Date.now();

    this.options.logger('Driver Swap', {
      orchestratorId: this.options.orchestratorId,
      dataSources: Object.keys(this.drivers).filter((key) => this.drivers[key]),
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
