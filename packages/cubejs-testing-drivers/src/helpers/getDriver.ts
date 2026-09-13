import { BaseDriver } from '@cubejs-backend/base-driver';
import { CubeStoreDriver } from '@cubejs-backend/cubestore-driver';

export async function getDriver(type: string): Promise<{
  source: BaseDriver,
  storage: BaseDriver,
}> {
  // `module: nodenext` emits import() verbatim, so this stays a require() and does the
  // esModuleInterop dance by hand: most driver packages set `module.exports` to the class,
  // while databricks-jdbc compiles from TypeScript and exposes it as `default`.
  // eslint-disable-next-line global-require, import/no-dynamic-require
  const driverModule = require(`@cubejs-backend/${type}-driver`);
  const DriverClass = driverModule.__esModule ? driverModule.default : driverModule;
  const source: BaseDriver = new DriverClass();
  source.setLogger((msg: unknown, event: unknown) => console.log(`${msg}: ${JSON.stringify(event)}`));
  const storage = new CubeStoreDriver();
  return { source, storage };
}
