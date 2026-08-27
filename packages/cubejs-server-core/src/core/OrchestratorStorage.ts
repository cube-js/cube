import { LRUCache } from 'lru-cache';
import type { OrchestratorApi } from './OrchestratorApi';

export class OrchestratorStorage {
  protected readonly storage: LRUCache<string, OrchestratorApi>;

  public constructor(options: { compilerCacheSize?: number, maxCompilerCacheKeepAlive?: number, updateCompilerCacheKeepAlive?: boolean } = { compilerCacheSize: 100 }) {
    this.storage = new LRUCache<string, OrchestratorApi>({
      max: options.compilerCacheSize,
      ttl: options.maxCompilerCacheKeepAlive,
      updateAgeOnGet: options.updateCompilerCacheKeepAlive
    });
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
   * Every api releases, then the map is cleared, and only then is a failure
   * reported. An api is spent the moment `release()` is entered — it has stopped
   * tracking its drivers and closed some of them — so a rejection must not leave
   * it in the map: `get()` would hand a spent api back out, and a second
   * `release()` on it is a no-op, so any pool that did survive becomes
   * unreachable. `allSettled` for the same reason one level down: one pool
   * refusing to close must not strand the other apis' teardown.
   */
  public async releaseConnections() {
    try {
      const results = await Promise.allSettled([...this.storage.values()].map(api => api.release()));

      const failure = results.find((result): result is PromiseRejectedResult => result.status === 'rejected');

      if (failure) {
        throw failure.reason;
      }
    } finally {
      this.storage.clear();
    }
  }
}
