import type { BaseDriver } from '../driver';

export type DriverFactory = () => (Promise<BaseDriver> | BaseDriver);
export type DriverFactoryByDataSource = (dataSource: string) => (Promise<BaseDriver> | BaseDriver);
