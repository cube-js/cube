import { CubeStoreHandler } from '@cubejs-backend/cubestore';

import { CubeStoreDriver } from './CubeStoreDriver';
import { ConnectionConfig } from './types';

export class CubeStoreDevDriver extends CubeStoreDriver {
  public constructor(
    protected readonly cubeStoreHandler: CubeStoreHandler,
    config?: Partial<ConnectionConfig>,
  ) {
    super({
      ...config,
      host: '127.0.0.1',
      // CubeStoreDriver is using env variables, let's override it by undefined
      user: undefined,
      password: undefined,
      // @todo Make random port selection when 13306 is already used?
      port: 3030,
    });
  }

  protected async acquireCubeStore() {
    return this.cubeStoreHandler.acquire();
  }

  public async query(query, values, options): Promise<any[]> {
    await this.acquireCubeStore();
    return super.query(query, values, options);
  }
}
