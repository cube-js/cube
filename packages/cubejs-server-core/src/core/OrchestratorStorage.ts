import { LRUCache } from 'lru-cache';
import type { OrchestratorApi } from './OrchestratorApi';

/**
 * How long an api that left the cache is kept before its connections are
 * closed. Leaving the cache doesn't mean nobody is using it: a request holds
 * the reference it was given across the whole of its work, so releasing right
 * away would close the drivers under a running query.
 */
const RELEASE_DELAY = 30 * 1000;

interface ScheduledRelease {
  api: OrchestratorApi;
  timer: ReturnType<typeof setTimeout>;
}

export class OrchestratorStorage {
  protected readonly storage: LRUCache<string, OrchestratorApi>;

  protected readonly releaseDelay: number;

  protected readonly scheduledReleases: Set<ScheduledRelease> = new Set();

  protected readonly pendingReleases: Set<Promise<void>> = new Set();

  public constructor(options: { compilerCacheSize?: number, maxCompilerCacheKeepAlive?: number, updateCompilerCacheKeepAlive?: boolean, releaseDelay?: number } = { compilerCacheSize: 100 }) {
    this.releaseDelay = options.releaseDelay ?? RELEASE_DELAY;

    this.storage = new LRUCache<string, OrchestratorApi>({
      max: options.compilerCacheSize,
      ttl: options.maxCompilerCacheKeepAlive,
      updateAgeOnGet: options.updateCompilerCacheKeepAlive,
      // Any removal reason ('evict' | 'set' | 'delete' | 'expire') means the api is not
      // reachable through the cache anymore, so its connections have to be closed.
      // Without it an evicted api keeps its Cube Store web socket open forever: the heartbeat
      // interval holds a reference to the socket, so it is never garbage collected either.
      // disposeAfter, not dispose: release() is async and calls into the drivers, while
      // dispose runs synchronously inside set()/delete().
      disposeAfter: (api: OrchestratorApi) => {
        this.scheduleRelease(api);
      },
    });
  }

  protected scheduleRelease(api: OrchestratorApi) {
    const scheduled: ScheduledRelease = {
      api,
      timer: setTimeout(() => {
        this.scheduledReleases.delete(scheduled);
        this.release(api);
      }, this.releaseDelay),
    };

    // A release that is only waiting out its delay is not a reason to keep the
    // process running.
    scheduled.timer.unref?.();

    this.scheduledReleases.add(scheduled);
  }

  protected release(api: OrchestratorApi) {
    const pending: Promise<void> = api.release()
      .then(() => undefined, (e) => {
        // Swallowed so that releasing a dead tenant can't take the process down
        // with an unhandled rejection, but not silently: connections this failed
        // to close stay open, which is the very thing the release is for.
        console.error('Failed to release an orchestrator api', e);
      })
      .then(() => {
        this.pendingReleases.delete(pending);
      });

    this.pendingReleases.add(pending);
  }

  protected releaseScheduled() {
    // eslint-disable-next-line no-restricted-syntax
    for (const scheduled of [...this.scheduledReleases]) {
      clearTimeout(scheduled.timer);
      this.scheduledReleases.delete(scheduled);
      this.release(scheduled.api);
    }
  }

  public has(orchestratorId: string) {
    return this.storage.has(orchestratorId);
  }

  public get(orchestratorId: string) {
    return this.storage.get(orchestratorId);
  }

  public set(orchestratorId: string, orchestratorApi: OrchestratorApi) {
    return this.storage.set(orchestratorId, orchestratorApi);
  }

  public clear() {
    this.storage.clear();
  }

  public async testConnections() {
    return Promise.all([...this.storage.values()].map(api => api.testConnection()));
  }

  public async testOrchestratorConnections() {
    return Promise.all([...this.storage.values()].map(api => api.testOrchestratorConnections()));
  }

  public async releaseConnections() {
    // clear() disposes every entry, which schedules a release for each of them.
    this.storage.clear();

    // Nothing is going to run after this, so the delay that protects a running
    // query has nothing left to protect.
    this.releaseScheduled();

    await Promise.all([...this.pendingReleases]);
  }
}
