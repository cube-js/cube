class BaseDbRunner {
  testQuery(query, fixture) {
    return this.testQueries([query], fixture);
  }

  async testQueries(queries, fixture) {
    if (!this.container && !process.env.TEST_LOCAL) {
      console.log(`Starting container`);
      this.container = await this.containerLazyInit();
    }
    if (!this.connection) {
      console.log(`Initializing connection`);
      const port = this.container ? this.container.getMappedPort(this.port()) : this.port();
      this.connection = await this.connectionLazyInit(port);
    }
    return this.connection.testQueries(queries, fixture);
  }

  async tearDown() {
    if (this.container) {
      await this.container.stop();
      this.connection = null;
      this.container = null;
    }
  }

  // eslint-disable-next-line no-unused-vars
  async connectionLazyInit(port) {
    throw new Error('Not implemented');
  }

  async containerLazyInit() {
    throw new Error('Not implemented');
  }

  port() {
    throw new Error('Not implemented');
  }
}

module.exports = BaseDbRunner;
