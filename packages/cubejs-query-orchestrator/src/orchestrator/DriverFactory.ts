import type { BaseDriver } from '../driver';

export type DriverFactory = (maxPool?: number) => (Promise<BaseDriver> | BaseDriver);
export type ExternalDriverFactory = () => (Promise<BaseDriver> | BaseDriver);
export type DriverFactoryByDataSource =
  (dataSource: string, maxPool?: number, concurrency?: boolean) => (
    | Promise<BaseDriver>
    | BaseDriver
    | number
  );
