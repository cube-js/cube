import { CubeStoreHandler } from '@cubejs-backend/cubestore';

import { CubeStoreDriver } from './CubeStoreDriver';
import { ConnectionConfig } from './types';
import { AsyncConnection } from './connection';

export class CubeStoreDevDriver extends CubeStoreDriver {
  public constructor(
    protected readonly cubeStoreHandler: CubeStoreHandler,
    config?: Partial<ConnectionConfig>
  ) {
    super({
      ...config,
      // @todo Make random port selection when 13306 is already used?
      port: 13306,
    });
  }

  protected async acquireCubeStore() {
    return this.cubeStoreHandler.acquire();
  }

  public async withConnection(fn: (connection: AsyncConnection) => Promise<unknown>) {
    await this.acquireCubeStore();

    return super.withConnection(fn);
  }

  public async testConnection() {
    await this.acquireCubeStore();

    return super.testConnection();
  }

  public async release(): Promise<void> {
    await super.release();

    if (this.cubeStoreHandler) {
      await (await this.cubeStoreHandler).release();
    }
  }
}
