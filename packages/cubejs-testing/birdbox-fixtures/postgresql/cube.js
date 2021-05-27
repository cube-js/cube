/* eslint-disable @typescript-eslint/no-unused-vars */
/**
 * Dont remove it, We use it to detect regression in package publishing & validation for docker
 */
const MysqlDriver = require('@cubejs-backend/mysql-driver');
const MSSQLDriver = require('@cubejs-backend/mssql-driver');
const PostgresDriver = require('@cubejs-backend/postgres-driver');
const RedshiftDriver = require('@cubejs-backend/redshift-driver');
const BigQueryDriver = require('@cubejs-backend/bigquery-driver');
const DruidDriver = require('@cubejs-backend/druid-driver');
const SnowflakeDriver = require('@cubejs-backend/snowflake-driver');
const SqliteDriver = require('@cubejs-backend/sqlite-driver');
const PrestodbDriver = require('@cubejs-backend/prestodb-driver');
const MongoBIDriver = require('@cubejs-backend/mongobi-driver');
const DremioDriver = require('@cubejs-backend/dremio-driver');
const ElasticSearchDriver = require('@cubejs-backend/elasticsearch-driver');
const OracleDriver = require('@cubejs-backend/oracle-driver');
const AthenaDriver = require('@cubejs-backend/athena-driver');

// Cube.js configuration options: https://cube.dev/docs/config
module.exports = {
};
