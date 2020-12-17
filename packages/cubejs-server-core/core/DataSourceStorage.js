class DataSourceStorage {
  constructor() {
    this.storage = new Map();
  }

  has(dataSourceId) {
    return this.storage.has(dataSourceId);
  }

  get(dataSourceId) {
    return this.storage.get(dataSourceId);
  }

  set(dataSourceId, orchestratorApi) {
    return this.storage.set(dataSourceId, orchestratorApi);
  }

  testConnections() {
    const result = [];

    // eslint-disable-next-line no-restricted-syntax
    for (const orchestratorApi of this.storage.values()) {
      result.push(orchestratorApi.testConnection());
    }

    return Promise.all(result);
  }

  testOrchestratorConnections() {
    const result = [];

    // eslint-disable-next-line no-restricted-syntax
    for (const orchestratorApi of this.storage.values()) {
      result.push(orchestratorApi.testOrchestratorConnections());
    }

    return Promise.all(result);
  }

  async releaseConnections() {
    const result = [];

    // eslint-disable-next-line no-restricted-syntax
    for (const orchestratorApi of this.storage.values()) {
      result.push(orchestratorApi.release());
    }

    await Promise.all(result);

    this.storage = new Map();
  }
}

module.exports = DataSourceStorage;
