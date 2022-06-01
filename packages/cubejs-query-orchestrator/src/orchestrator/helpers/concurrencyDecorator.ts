import { DriverFactoryByDataSource } from '../DriverFactory';

export function concurrencyDecorator(
  driverFactory: DriverFactoryByDataSource,
  queueOptions: any | ((dataSource: string) => any),
  dataSource: string,
) {
  const concurrency = driverFactory(dataSource, true) as number;
  const options = (
    (typeof queueOptions === 'function')
      ? queueOptions(dataSource)
      : queueOptions
  ) || {};
    
  if (!options.concurrency && concurrency > 0) {
    options.concurrency = concurrency;
  }
  return options;
}
