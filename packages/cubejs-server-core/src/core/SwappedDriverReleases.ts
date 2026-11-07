import type { BaseDriver } from '@cubejs-backend/query-orchestrator';

import type { LoggerFn } from './types';

/** Past 2^31-1 ms setTimeout fires at once, which would release a driver under its queries. */
const MAX_TIMEOUT_MS = 2 ** 31 - 1;

type PendingRelease = {
  timer?: NodeJS.Timeout;
  release: () => Promise<void>;
};

/**
 * Drivers a `driverVersion` swap replaced, each released once the queries and
 * pre-aggregation builds that may still hold it have timed out. Shutdown and reset
 * release the pending ones at once.
 */
export class SwappedDriverReleases {
  protected readonly pending: Map<BaseDriver, PendingRelease> = new Map();

  public constructor(protected readonly logger: LoggerFn) {
  }

  public isPending(driver: BaseDriver): boolean {
    return this.pending.has(driver);
  }

  /**
   * Release destroys or drains a driver at once, so it waits for `afterSeconds`.
   * Streams have no timeout, so one still running then is cut off.
   */
  public schedule(driver: BaseDriver, afterSeconds: number): void {
    if (this.pending.has(driver)) {
      return;
    }

    const entry: PendingRelease = {
      release: async () => {
        clearTimeout(entry.timer);
        this.pending.delete(driver);

        await this.releaseSafely(driver);
      },
    };

    entry.timer = setTimeout(entry.release, Math.min(afterSeconds * 1000, MAX_TIMEOUT_MS));
    entry.timer.unref();

    this.pending.set(driver, entry);
  }

  /** A caching driverFactory can hand back a driver still awaiting its release; keep it open. */
  public cancel(driver: BaseDriver): void {
    const entry = this.pending.get(driver);

    if (entry) {
      clearTimeout(entry.timer);
      this.pending.delete(driver);
    }
  }

  public async releaseAll(): Promise<void> {
    await Promise.all([...this.pending.values()].map((entry) => entry.release()));
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
}
