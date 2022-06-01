import type { BaseDriver } from '../driver';

export type DriverFactory = (maxPool?: number) => (Promise<BaseDriver> | BaseDriver);
export type ExternalDriverFactory = () => (Promise<BaseDriver> | BaseDriver);
export type DriverFactoryByDataSource =
  (dataSource: string, maxpoolOrConcurrency?: number | boolean) => (
    | Promise<BaseDriver>
    | BaseDriver
    | number
  );
