import { PoolOptions } from '@cubejs-backend/shared';

export type JDBCDriverConfiguration = {
  database: string,
  dbType: string,
  url: string,
  drivername: string,
  customClassPath?: string,
  properties: Record<string, any>,
  poolOptions?: PoolOptions;
  prepareConnectionQueries?: string[];
  /**
   * Number of rows read from a result set per round trip to the JVM.
   */
  batchSize?: number;
};
