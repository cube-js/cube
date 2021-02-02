export class BaseDbRunner {
  containerLazyInitPromise = null;

  connectionLazyInitPromise = null;

  testQuery(query, fixture) {
    return this.testQueries([query], fixture);
  }

  async testQueries(queries, fixture) {
    if (this.containerLazyInitPromise) {
      await this.containerLazyInitPromise;
    }

    if (!this.container && !process.env.TEST_LOCAL) {
      console.log('[Container] Starting');

      this.containerLazyInitPromise = this.containerLazyInit();

      try {
        this.container = await this.containerLazyInitPromise;

        console.log(`[Container] Started ${this.container.getId()}`);
      } finally {
        this.containerLazyInitPromise = null;
      }
    }

    if (this.connectionLazyInitPromise) {
      await this.connectionLazyInitPromise;
    }

    if (!this.connection) {
      const port = this.container ? this.container.getMappedPort(this.port()) : this.port();
      console.log('[Connection] Initializing');

      this.connectionLazyInitPromise = this.connectionLazyInit(port);

      try {
        this.connection = await this.connectionLazyInitPromise;
      } finally {
        this.connectionLazyInitPromise = null;
      }

      console.log('[Connection] Initialized');
    }
    return this.connection.testQueries(queries, fixture);
  }

  async tearDown() {
    console.log('[TearDown] Starting');

    if (this.containerLazyInitPromise) {
      throw new Error('container was not resolved before tearDown');
    }

    if (this.connectionLazyInitPromise) {
      throw new Error('connection was not resolved before tearDown');
    }

    if (this.connection) {
      console.log('[Connection] Closing');

      if (this.connection.close) {
        try {
          await this.connection.close();
        } catch (e) {
          console.log(e);
        }
      }

      this.connection = null;

      console.log('[Connection] Closed');
    }

    if (this.container) {
      console.log(`[Container] Shutdown ${this.container.getId()}`);

      await this.container.stop();

      console.log(`[Container] Stopped ${this.container.getId()}`);

      this.container = null;
    }

    console.log('[TearDown] Finished');
  }

  // eslint-disable-next-line no-unused-vars,@typescript-eslint/no-unused-vars
  async connectionLazyInit(port) {
    throw new Error('Not implemented connectionLazyInit');
  }

  async containerLazyInit() {
    throw new Error('Not implemented containerLazyInit');
  }

  port() {
    throw new Error('Not implemented port');
  }
}
