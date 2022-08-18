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
import { AWSElasticSearchQuery } from './AWSElasticSearchQuery';
import { ElasticSearchQuery } from './ElasticSearchQuery';

const ADAPTERS = {
  postgres: PostgresQuery,
  redshift: RedshiftQuery,
  mysql: MysqlQuery,
  mysqlauroraserverless: MysqlQuery,
  mongobi: MongoBiQuery,
  mssql: MssqlQuery,
  bigquery: BigqueryQuery,
  prestodb: PrestodbQuery,
  qubole_prestodb: PrestodbQuery,
  athena: PrestodbQuery,
  vertica: VerticaQuery,
  snowflake: SnowflakeQuery,
  clickhouse: ClickHouseQuery,
  crate: CrateQuery,
  hive: HiveQuery,
  oracle: OracleQuery,
  sqlite: SqliteQuery,
  awselasticsearch: AWSElasticSearchQuery,
  elasticsearch: ElasticSearchQuery,
  materialize: PostgresQuery,
};

export const queryClass = (dbType, dialectClass) => dialectClass || ADAPTERS[dbType];

export const createQuery = (compilers, dbType, queryOptions) => {
  if (!queryOptions.dialectClass && !ADAPTERS[dbType]) {
    return null;
  }

  let externalQueryClass = queryOptions.externalDialectClass;

  if (!externalQueryClass && queryOptions.externalDbType) {
    if (!ADAPTERS[queryOptions.externalDbType]) {
      throw new Error(`Dialect for '${queryOptions.externalDbType}' is not found`);
    }

    externalQueryClass = ADAPTERS[queryOptions.externalDbType];
  }

  return new (queryClass(dbType, queryOptions.dialectClass))(compilers, {
    ...queryOptions,
    externalQueryClass,
  });
};
