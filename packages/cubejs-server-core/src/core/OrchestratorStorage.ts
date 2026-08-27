import { LRUCache } from 'lru-cache';
import type { OrchestratorApi } from './OrchestratorApi';

export class OrchestratorStorage {
  protected readonly storage: LRUCache<string, OrchestratorApi>;

  protected readonly pendingReleases: Set<Promise<void>> = new Set();

  public constructor(options: { compilerCacheSize?: number, maxCompilerCacheKeepAlive?: number, updateCompilerCacheKeepAlive?: boolean } = { compilerCacheSize: 100 }) {
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
        this.release(api);
      },
    });
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
    // clear() disposes every entry, which schedules release() for each of them.
    this.storage.clear();

    await Promise.all([...this.pendingReleases]);
  }
}
