import type { BaseDriver } from '../driver';

export type DriverFactory = () => (Promise<BaseDriver> | BaseDriver);
export type DriverFactoryByDataSource = (
  dataSource: string,
  config: {
    poolSize: number,
  },
) => (Promise<BaseDriver> | BaseDriver);
