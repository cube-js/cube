import type { BaseDriver } from '../driver';

export type DriverFactory = () => (Promise<BaseDriver> | BaseDriver);
export type DriverFactoryByDataSource = (dataSource: string) => (Promise<BaseDriver> | BaseDriver);
export type getConcurrencyFn = (dataSource?: string) => {
  poolSize: number,
  workersNumber: number,
  queriesNumber: number,
};
export type concurrencyFactoryFn = () => {
  poolSize: number,
  workersNumber: number,
  queriesNumber: number,
};
