import { PoolOptions } from '@cubejs-backend/shared';

export type JDBCDriverConfiguration = {
  database: string,
  dbType: string,
  url: string,
  drivername: string,
  customClassPath?: string,
  properties: Record<string, any>,
  poolOptions?: PoolOptions;
  /** @deprecated Use `sqlPreamble`, which takes one string. */
  prepareConnectionQueries?: string[];
  sqlPreamble?: string;
  dataSource?: string;
  preAggregations?: boolean;
  preAggregationsSqlPreamble?: boolean;
};
