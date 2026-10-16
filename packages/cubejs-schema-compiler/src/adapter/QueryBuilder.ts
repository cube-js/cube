import type { BaseQuery } from './BaseQuery';
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

export const dialectByDbType = (dbType: string | undefined): typeof BaseQuery | undefined => (
  dbType ? ADAPTERS[dbType] : undefined
);
