import { Options } from 'generic-pool';

export type JDBCDriverConfiguration = {
  database: string,
  dbType: string,
  url: string,
  drivername: string,
  dataSource?: string,
  customClassPath?: string,
  properties: Record<string, any>,
  maxPoolSize?: number;
  poolOptions?: Options;
  prepareConnectionQueries?: string[];
};
