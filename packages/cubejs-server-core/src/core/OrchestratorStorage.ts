import type { OrchestratorApi } from './OrchestratorApi';

export class OrchestratorStorage {
  protected readonly storage: Map<string, OrchestratorApi> = new Map();

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
    const result = [];

    // eslint-disable-next-line no-restricted-syntax
    for (const orchestratorApi of this.storage.values()) {
      result.push(orchestratorApi.testConnection());
    }

    return Promise.all(result);
  }

  public async testOrchestratorConnections() {
    const result = [];

    // eslint-disable-next-line no-restricted-syntax
    for (const orchestratorApi of this.storage.values()) {
      result.push(orchestratorApi.testOrchestratorConnections());
    }

    return Promise.all(result);
  }

  public async releaseConnections() {
    const result = [];

    // eslint-disable-next-line no-restricted-syntax
    for (const orchestratorApi of this.storage.values()) {
      result.push(orchestratorApi.release());
    }

    await Promise.all(result);

    this.storage.clear();
  }
}
