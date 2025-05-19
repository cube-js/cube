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

  public async releaseConnections() {
    await Promise.all([...this.storage.values()].map(api => api.release()));
    this.storage.clear();
  }
}
