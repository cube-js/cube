import { PreAggregationPartitionRangeLoader } from '@cubejs-backend/query-orchestrator';
// eslint-disable-next-line import/no-extraneous-dependencies
import { StartedDockerComposeEnvironment } from 'testcontainers';

export class BaseDbRunner {
  protected containerLazyInitPromise: any = null;

  protected connectionLazyInitPromise: any = null;

  protected container: StartedDockerComposeEnvironment | null = null;

  protected connection: any;

  protected nextSeed: number = 1;

  public testQuery(query: any, fixture: any = null) {
    return this.testQueries([query], fixture);
  }

  public async testQueries(queries: string[], fixture: any = null) {
    queries.forEach((q: any) => {
      console.log(q[0]);
      console.log(q[1]);
    });
    if (this.containerLazyInitPromise) {
      await this.containerLazyInitPromise;
    }

    if (!this.container && !process.env.TEST_LOCAL) {
      console.log('[Container] Starting');

      this.containerLazyInitPromise = this.containerLazyInit();

      try {
        this.container = await this.containerLazyInitPromise;
      } finally {
        this.containerLazyInitPromise = null;
      }
    }

    if (this.connectionLazyInitPromise) {
      await this.connectionLazyInitPromise;
    }

    if (!this.connection) {
      // @ts-ignore
      const port = this.port();
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

  public async tearDown() {
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
      console.log('[Container] Shutdown');

      await this.container.down();

      console.log('[Container] Stopped');

      this.container = null;
    }

    console.log('[TearDown] Finished');
  }

  public async connectionLazyInit(_port: number) {
    throw new Error('Not implemented connectionLazyInit');
  }

  public async containerLazyInit(): Promise<StartedDockerComposeEnvironment> {
    throw new Error('Not implemented containerLazyInit');
  }

  public port(): number {
    throw new Error('Not implemented port');
  }
}
