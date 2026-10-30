import type { BaseDriver } from '@cubejs-backend/query-orchestrator';
import { getEnv, hasPreAggregationsEnvVars } from '@cubejs-backend/shared';

import type { SwappedDriverReleases } from './SwappedDriverReleases';
import type { LoggerFn, OrchestratorOptions, QueueOptions, RequestContext } from './types';

/** A failed swap to one driver version is not retried for this long. */
const SWAP_FAILURE_BACKOFF_MS = 10 * 1000;

/** Failing versions remembered for backoff and waiting requests, oldest dropped first. */
const MAX_FAILURES_KEPT = 16;

function keepLatest<V>(map: Map<string, V>, key: string, value: V) {
  map.delete(key);
  map.set(key, value);
  map.forEach((_, k) => map.size > MAX_FAILURES_KEPT && map.delete(k));
}

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
};

/**
 * One orchestrator's drivers by data source (`ds`, `ds@pre_agg`) and their `driverVersion`.
 * A request with another version starts a background rebuild of all of them from its
 * context, installed at once; requests keep the current drivers meanwhile.
 */
export class OrchestratorDrivers {
  protected readonly drivers: Record<string, Promise<BaseDriver> | null> = {};

  /** What each `drivers` key was built for, so a swap can rebuild it. */
  protected readonly specs: Record<string, DriverSpec> = {};

  protected context: RequestContext;

  protected version: string | undefined;

  /** Never rejects: a failure is logged and backs off. */
  protected swapping: Promise<void> | undefined;

  /** When each failing version last failed, so several failing versions all back off. */
  protected readonly backoffs: Map<string, { at: number, reported: boolean }> = new Map();

  /**
   * While no version is installed: why each version's latest swap failed, until it is
   * asked for again after its backoff. Read only by `awaitFirstVersion`.
   */
  protected readonly failures: Map<string, unknown> = new Map();

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
   * Starts a swap when `requested` differs from the version the drivers were built for.
   * The request is served by the current drivers, unless they were built for no version
   * (see `awaitFirstVersion`). A failed version is not retried until the backoff passes.
   * A version asked for while another swap runs is not queued: a later request asks again.
   */
  public async ensureVersion(
    requested: string,
    context: RequestContext,
    isLive: () => boolean,
  ): Promise<EnsureVersionResult> {
    if (requested === this.version) {
      return 'ready';
    }

    if (!isLive()) {
      return 'evicted';
    }

    if (!this.hasDrivers()) {
      // Nothing to rebuild: build the first drivers from this context
      this.context = context;
      this.version = requested;

      return 'ready';
    }

    this.requestSwap(requested, context, isLive);

    return this.version === undefined ? this.awaitFirstVersion(requested, context, isLive) : 'ready';
  }

  /** Resolves once no swap is running. */
  public async settled(): Promise<void> {
    while (this.swapping) {
      await this.swapping;
    }
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
    const backoff = this.backoffs.get(requested);

    if (!backoff || Date.now() - backoff.at >= SWAP_FAILURE_BACKOFF_MS) {
      return false;
    }

    if (!backoff.reported) {
      backoff.reported = true;

      this.options.logger('Driver Swap Backoff', {
        orchestratorId: this.options.orchestratorId,
        warning: `Swapping drivers to a new driverVersion failed less than ${SWAP_FAILURE_BACKOFF_MS / 1000}s ago. ` +
          'Serving with the previous drivers until it is retried.',
      });
    }

    return true;
  }

  /**
   * Drivers built with no version, as from the readiness probe's empty context, may hold a
   * fallback credential. A versioned request waits until a swap installs a version, and
   * fails with its own swap's error once that fails.
   */
  protected async awaitFirstVersion(
    requested: string,
    context: RequestContext,
    isLive: () => boolean,
  ): Promise<EnsureVersionResult> {
    while (this.swapping && this.version === undefined && !this.failures.has(requested)) {
      await this.swapping;

      // The swap was another version's and installed nothing; this one was not queued, so ask now
      if (!this.swapping && this.version === undefined && !this.failures.has(requested) && isLive()) {
        this.requestSwap(requested, context, isLive);
      }
    }

    if (!isLive()) {
      return 'evicted';
    }

    if (this.version === undefined) {
      throw this.failures.has(requested)
        ? this.failures.get(requested)
        : new Error('Drivers built for no version cannot serve a request with a driverVersion, and no swap installed one.');
    }

    return 'ready';
  }

  protected hasDrivers(): boolean {
    return Object.values(this.drivers).some((driver) => driver);
  }

  protected requestSwap(version: string, context: RequestContext, isLive: () => boolean) {
    if (!this.isBackingOff(version)) {
      // An error kept from before the backoff passed is stale: a request waiting on another swap must not get it
      this.failures.delete(version);

      if (!this.swapping) {
        this.startSwap(context, version, isLive);
      }
    }
  }

  protected startSwap(context: RequestContext, version: string, isLive: () => boolean) {
    this.swapping = this.swap(context, version, isLive)
      .then(
        () => {
          this.backoffs.delete(version);
        },
        (error) => this.recordFailure(version, error),
      )
      .finally(() => {
        this.swapping = undefined;
      })
      // Started without being awaited, so a throwing logger must not become an unhandled rejection
      .catch((): undefined => undefined);
  }

  protected recordFailure(version: string, error: unknown) {
    keepLatest(this.backoffs, version, { at: Date.now(), reported: false });
    if (this.version === undefined) {
      keepLatest(this.failures, version, error);
    }

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
    const run = this.newSwapRun(context);

    try {
      // Repeated for drivers built lazily from the old context meanwhile; last checked in the same synchronous
      // step as the install, so none slips in
      do {
        await this.rebuildRound(run, this.unswapped(run));
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
    const previous = await Promise.all(promises.map((promise) => promise.catch((): null => null)));

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
    const installation: Installation = { installed: new Set(), replaced: new Set() };

    run.replacements.forEach(({ previous, next }, previousPromise) => {
      const keys = Object.keys(this.drivers).filter((key) => this.drivers[key] === previousPromise);

      // A null previous means its build failed, so there is no next either
      if (!next || !previous) {
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

  /** Releases replaced drivers after the query timeout. */
  protected settleOldDrivers(run: SwapRun, { installed, replaced }: Installation) {
    const { releases } = this.options;

    installed.forEach((driver) => releases.cancel(driver));

    replaced.forEach((driver) => {
      if (!installed.has(driver)) {
        releases.schedule(driver, this.queryTimeoutSeconds(run.replacedDataSources.get(driver) ?? 'default'));
      }
    });
  }

  protected recordSwap(context: RequestContext, version: string) {
    this.failures.clear();
    this.context = context;
    this.version = version;

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
