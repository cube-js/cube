import { CubeStoreDriver } from './CubeStoreDriver';

export class X extends CubeStoreDriver {
  public constructor() {
    super({
      host: '127.0.0.1',
      user: undefined,
      password: undefined,
      port: 3030,
    });
  }
}