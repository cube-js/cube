import { LRUCache } from 'lru-cache';
import type { OrchestratorApi } from './OrchestratorApi';

export class OrchestratorStorage {
  protected readonly storage: LRUCache<string, OrchestratorApi>;

  protected readonly pendingReleases: Set<Promise<void>> = new Set();

  /**
   * What the release triggered by the next cache removal does about the work an
   * api is still serving. Read by disposeAfter, which takes no arguments of its
   * own.
   */
  protected releaseWaitsForWork: boolean = true;

  public constructor(options: { compilerCacheSize?: number, maxCompilerCacheKeepAlive?: number, updateCompilerCacheKeepAlive?: boolean } = { compilerCacheSize: 100 }) {
    this.storage = new LRUCache<string, OrchestratorApi>({
      max: options.compilerCacheSize,
      ttl: options.maxCompilerCacheKeepAlive,
      updateAgeOnGet: options.updateCompilerCacheKeepAlive,
      // Any removal reason ('evict' | 'set' | 'delete' | 'expire') means the api is not
      // reachable through the cache anymore, so its connections have to be closed.
      // Without it an evicted api keeps its Cube Store web socket open forever: the heartbeat
      // interval holds a reference to the socket, so it is never garbage collected either.
      // The api itself holds the release off until the work it was handed to is done.
      // disposeAfter, not dispose: release() is async and calls into the drivers, while
      // dispose runs synchronously inside set()/delete().
      disposeAfter: (api: OrchestratorApi) => {
        this.release(api);
      },
    });
  }

  protected release(api: OrchestratorApi) {
    const pending: Promise<void> = api.release({ waitForWork: this.releaseWaitsForWork })
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

  /**
   * @param waitForWork Whether the apis get to finish what they are serving.
   * A shutdown can't afford it: its own killer gives it seconds, and being
   * force killed halfway is worse than cutting the queries off.
   */
  public async releaseConnections({ waitForWork = true }: { waitForWork?: boolean } = {}) {
    // A release from an earlier removal is waiting out the work its api was
    // serving, and that wait is exactly what this path can't afford: whatever it
    // doesn't get to close, the process going away does.
    const earlier = waitForWork ? new Set<Promise<void>>() : new Set(this.pendingReleases);

    this.releaseWaitsForWork = waitForWork;

    try {
      // clear() disposes every entry, which schedules release() for each of them.
      this.storage.clear();
    } finally {
      this.releaseWaitsForWork = true;
    }

    await Promise.all([...this.pendingReleases].filter(release => !earlier.has(release)));
  }
}
