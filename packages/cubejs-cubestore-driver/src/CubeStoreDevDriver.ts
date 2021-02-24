import { CubeStoreHandler, startCubeStoreHandler } from '@cubejs-backend/cubestore';

import { CubeStoreDriver } from './CubeStoreDriver';
import { ConnectionConfig } from './types';
import { AsyncConnection } from './connection';

export class CubeStoreDevDriver extends CubeStoreDriver {
  // Let's use Promise as Mutex to protect multiple starting of Cube Store
  protected cubeStoreHandler: Promise<CubeStoreHandler>|null = null;

  public constructor(config?: Partial<ConnectionConfig>) {
    super({
      ...config,
      // @todo Make random port selection when 13306 is already used?
      port: 13306,
    });
  }

  protected async acquireCubeStore() {
    if (!this.cubeStoreHandler) {
      this.cubeStoreHandler = startCubeStoreHandler({
        stdout: (data) => {
          console.log(data.toString().trim());
        },
        stderr: (data) => {
          console.log(data.toString().trim());
        },
        onRestart: (code) => this.logger('Cube Store Restarting', {
          warning: `Instance exit with ${code}, restarting`,
        }),
      });
    }

    await (await this.cubeStoreHandler).acquire();
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
