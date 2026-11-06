import type { BaseQuery, BaseQueryOptions } from './BaseQuery';
import type { Compilers } from '../compiler/PrepareCompiler';
import { PostgresQuery } from './PostgresQuery';
import { MysqlQuery } from './MysqlQuery';
import { MongoBiQuery } from './MongoBiQuery';
import { MssqlQuery } from './MssqlQuery';
import { BigqueryQuery } from './BigqueryQuery';
import { RedshiftQuery } from './RedshiftQuery';
import { PrestodbQuery } from './PrestodbQuery';
import { VerticaQuery } from './VerticaQuery';
import { SnowflakeQuery } from './SnowflakeQuery';
import { ClickHouseQuery } from './ClickHouseQuery';
import { CrateQuery } from './CrateQuery';
import { HiveQuery } from './HiveQuery';
import { OracleQuery } from './OracleQuery';
import { SqliteQuery } from './SqliteQuery';
import { CubeStoreQuery } from './CubeStoreQuery';
import { AthenaQuery } from './AthenaQuery';
import { TrinoQuery } from './TrinoQuery';

const ADAPTERS: Record<string, typeof BaseQuery> = {
  postgres: PostgresQuery,
  redshift: RedshiftQuery,
  mysql: MysqlQuery,
  mysqlauroraserverless: MysqlQuery,
  mongobi: MongoBiQuery,
  mssql: MssqlQuery,
  bigquery: BigqueryQuery,
  prestodb: PrestodbQuery,
  qubole_prestodb: PrestodbQuery,
  athena: AthenaQuery,
  trino: TrinoQuery,
  vertica: VerticaQuery,
  snowflake: SnowflakeQuery,
  clickhouse: ClickHouseQuery,
  crate: CrateQuery,
  hive: HiveQuery,
  oracle: OracleQuery,
  sqlite: SqliteQuery,
  materialize: PostgresQuery,
  cubestore: CubeStoreQuery,
};

export type CreateQueryOptions = BaseQueryOptions & {
  dialectClass?: typeof BaseQuery | null;
  externalDialectClass?: typeof BaseQuery | null;
  externalDbType?: string;
};

export const queryClass = (dbType: string, dialectClass?: typeof BaseQuery | null): typeof BaseQuery => dialectClass || ADAPTERS[dbType];

/**
 * @param dbType picks the dialect unless `dialectClass` is given, which needs no `dbType`
 */
export const createQuery = (compilers: Compilers, dbType: string | undefined, queryOptions: CreateQueryOptions): BaseQuery | null => {
  const { dialectClass, externalDialectClass, externalDbType, ...options } = queryOptions;

  const QueryClass = dialectClass || (dbType !== undefined ? ADAPTERS[dbType] : undefined);
  if (!QueryClass) {
    return null;
  }

  let externalQueryClass = externalDialectClass;

  if (!externalQueryClass && externalDbType) {
    if (!ADAPTERS[externalDbType]) {
      throw new Error(`Dialect for '${externalDbType}' is not found`);
    }

    externalQueryClass = ADAPTERS[externalDbType];
  }

  return compilers.createQuery(QueryClass, {
    ...options,
    externalQueryClass,
  });
};
