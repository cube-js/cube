import type { BaseDriver } from '@cubejs-backend/base-driver';

export type DriverFactory = () => (Promise<BaseDriver> | BaseDriver);
export type DriverFactoryByDataSource =
  (dataSource: string) => (Promise<BaseDriver> | BaseDriver);
