/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview Drivers dependencies and lookup service declaration.
 */

import fs from 'fs-extra';
import path from 'path';
import type { BaseDriver } from '@cubejs-backend/query-orchestrator';
import type { Constructor } from '@cubejs-backend/shared';
import type { DatabaseType } from '../types';

/**
 * Drivers dependencies hash table.
 */
export const DriverDependencies = {
  postgres: '@cubejs-backend/postgres-driver',
  mysql: '@cubejs-backend/mysql-driver',
  mysqlauroraserverless: '@cubejs-backend/mysql-aurora-serverless-driver',
  mssql: '@cubejs-backend/mssql-driver',
  athena: '@cubejs-backend/athena-driver',
  jdbc: '@cubejs-backend/jdbc-driver',
  mongobi: '@cubejs-backend/mongobi-driver',
  bigquery: '@cubejs-backend/bigquery-driver',
  redshift: '@cubejs-backend/redshift-driver',
  clickhouse: '@cubejs-backend/clickhouse-driver',
  hive: '@cubejs-backend/hive-driver',
  snowflake: '@cubejs-backend/snowflake-driver',
  prestodb: '@cubejs-backend/prestodb-driver',
  oracle: '@cubejs-backend/oracle-driver',
  sqlite: '@cubejs-backend/sqlite-driver',
  awselasticsearch: '@cubejs-backend/elasticsearch-driver',
  elasticsearch: '@cubejs-backend/elasticsearch-driver',
  dremio: '@cubejs-backend/dremio-driver',
  druid: '@cubejs-backend/druid-driver',
  cubestore: '@cubejs-backend/cubestore-driver',
  ksql: '@cubejs-backend/ksql-driver',
  questdb: '@cubejs-backend/questdb-driver',
  materialize: '@cubejs-backend/materialize-driver',
  // List for JDBC drivers
  'databricks-jdbc': '@cubejs-backend/databricks-jdbc-driver',
};

/**
 * Returns driver's package name for the specified database type.
 */
export function driverDependencies(dbType: DatabaseType): string {
  if (DriverDependencies[dbType]) {
    return DriverDependencies[dbType];
  } else if (fs.existsSync(path.join('node_modules', `${dbType}-cubejs-driver`))) {
    return `${dbType}-cubejs-driver`;
  }

  throw new Error(`Unsupported db type: ${dbType}`);
}

/**
 * Returns driver's constructor for the specified database type.
 */
export function lookupDriverClass(dbType): Constructor<BaseDriver> & {
  dialectClass?: () => any;
  monoConcurrencyDefault?: () => number;
  calcConcurrency?: (mono: number, preaggsWarmUp: boolean) => {
    maxpool: number;
    queries: number;
    preaggs: number;
  }
} {
  // eslint-disable-next-line global-require,import/no-dynamic-require
  const module = require(driverDependencies(dbType || process.env.CUBEJS_DB_TYPE));
  if (module.default) {
    return module.default;
  }
  return module;
}
