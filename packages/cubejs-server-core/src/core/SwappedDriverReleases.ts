import type { BaseDriver } from '@cubejs-backend/query-orchestrator';

import type { LoggerFn } from './types';

/** Past 2^31-1 ms setTimeout fires at once, which would release a driver under its queries. */
const MAX_TIMEOUT_MS = 2 ** 31 - 1;

/**
 * Drivers a `driverVersion` swap replaced, each released once the queries and
 * pre-aggregation builds that may still hold it have timed out. Shutdown and reset
 * release the pending ones at once.
 */
export class SwappedDriverReleases {
  protected readonly timers: Map<BaseDriver, NodeJS.Timeout> = new Map();

  public constructor(protected readonly logger: LoggerFn) {
  }

  public isPending(driver: BaseDriver): boolean {
    return this.timers.has(driver);
  }

  /**
   * Release destroys or drains a driver at once, so it waits for `afterSeconds`.
   * Streams have no timeout, so one still running then is cut off.
   */
  public schedule(driver: BaseDriver, afterSeconds: number): void {
    if (!this.timers.has(driver)) {
      const timer = setTimeout(() => this.releaseNow(driver), Math.min(afterSeconds * 1000, MAX_TIMEOUT_MS));
      timer.unref();

      this.timers.set(driver, timer);
    }
  }

  /** A caching driverFactory can hand back a driver still awaiting its release; keep it open. */
  public cancel(driver: BaseDriver): void {
    clearTimeout(this.timers.get(driver));
    this.timers.delete(driver);
  }

  public async releaseAll(): Promise<void> {
    await Promise.all([...this.timers.keys()].map((driver) => this.releaseNow(driver)));
  }

  public async releaseSafely(driver: BaseDriver): Promise<void> {
    try {
      if (driver.release) {
        await driver.release();
      }
    } catch (error) {
      this.logger('Driver Release Error', {
        error: (error as Error).stack || String(error),
      });
    }
  }

  protected releaseNow(driver: BaseDriver): Promise<void> {
    this.cancel(driver);

    return this.releaseSafely(driver);
  }
}
