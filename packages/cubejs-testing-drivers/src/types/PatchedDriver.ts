import { BaseDriver } from '@cubejs-backend/base-driver';

export type Method =
  | 'streamQuery'
  | 'downloadQueryResults'
  | 'readOnly'
  | 'tablesSchema'
  | 'createSchemaIfNotExists'
  | 'getTablesQuery'
  | 'loadPreAggregationIntoTable'
  | 'dropTable'
  | 'param'
  | 'testConnectionTimeout'
  | 'downloadTable'
  | 'uploadTable'
  | 'uploadTableWithIndexes'
  | 'tableColumnTypes'
  | 'queryColumnTypes'
  | 'createTable'
  | 'setLogger'
  | 'release'
  | 'capabilities'
  | 'nowTimestamp'
  | 'wrapQueryWithLimit'
  | 'query'
  | 'testConnection'
  | 'stream';

export type PatchedDriver = BaseDriver & {
  stream?: (...args: any[]) => any,
  calls?: Method[],
};
