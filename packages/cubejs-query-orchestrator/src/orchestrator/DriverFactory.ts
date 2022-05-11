import type { BaseDriver } from '../driver';

export type DriverFactory = () => (Promise<BaseDriver> | BaseDriver);
export type DriverFactoryByDataSource = (dataSource: string) => (Promise<BaseDriver> | BaseDriver);
export type getConcurrencyFn = (dataSource?: string) => {
  maxpool: number;
  queries: number;
  preaggs: number;
};
export type concurrencyFactoryFn = () => {
  maxpool: number;
  queries: number;
  preaggs: number;
};
