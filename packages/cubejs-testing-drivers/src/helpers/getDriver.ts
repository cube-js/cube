import { BaseDriver } from '@cubejs-backend/base-driver';
import { CubeStoreDriver } from '@cubejs-backend/cubestore-driver';

export async function getDriver(type: string): Promise<{
  source: BaseDriver,
  storage: BaseDriver,
}> {
  // `module: nodenext` emits import() verbatim, and a native import of these CommonJS packages
  // would put the whole module on `.default`.
  // eslint-disable-next-line global-require, import/no-dynamic-require
  const driverModule = require(`@cubejs-backend/${type}-driver`);
  // eslint-disable-next-line new-cap
  const source: BaseDriver = new driverModule.default();
  source.setLogger((msg: unknown, event: unknown) => console.log(`${msg}: ${JSON.stringify(event)}`));
  const storage = new CubeStoreDriver();
  return { source, storage };
}
