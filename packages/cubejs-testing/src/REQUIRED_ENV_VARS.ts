/**
 * Environment variables that cannot be hardcoded, and instead must be specified via the cli.
 * Usually cloud db config & auth variables.
 */
export const REQUIRED_ENV_VARS: {[key: string]: string[]} = {
  athena: [
    'CUBEJS_AWS_KEY',
    'CUBEJS_AWS_SECRET',
    'CUBEJS_AWS_REGION',
    'CUBEJS_AWS_S3_OUTPUT_LOCATION',
    'CUBEJS_DB_EXPORT_BUCKET'
  ],
  bigquery: [
    'CUBEJS_DB_BQ_PROJECT_ID',
    'CUBEJS_DB_EXPORT_BUCKET',
    'CUBEJS_DB_BQ_CREDENTIALS',
  ],
  snowflake: [
    'CUBEJS_DB_USER',
    'CUBEJS_DB_PASS',
    'CUBEJS_DB_NAME',
    'CUBEJS_DB_SNOWFLAKE_ACCOUNT',
    'CUBEJS_DB_SNOWFLAKE_REGION',
    'CUBEJS_DB_SNOWFLAKE_WAREHOUSE',
  ],
  crate: [],
  firebolt: [
    'CUBEJS_DB_USER',
    'CUBEJS_DB_PASS',
    'CUBEJS_DB_NAME',
    'CUBEJS_FIREBOLT_ENGINE_NAME'
  ],
  materialize: [],
  multidb: [],
  oracle: [],
  questdb: [],
  postgres: [],
  redshift: [
    'CUBEJS_DB_HOST',
    'CUBEJS_DB_PORT',
    'CUBEJS_DB_NAME',
    'CUBEJS_DB_USER',
    'CUBEJS_DB_PASS',
  ],
  'databricks-jdbc': [
    'CUBEJS_DB_TYPE',
    'CUBEJS_DB_DATABRICKS_URL',
    'CUBEJS_DB_DATABRICKS_ACCEPT_POLICY',
    'CUBEJS_DB_NAME',
    'CUBEJS_DB_EXPORT_BUCKET_TYPE',
    'CUBEJS_DB_EXPORT_BUCKET',
    'CUBEJS_DB_EXPORT_BUCKET_AWS_KEY',
    'CUBEJS_DB_EXPORT_BUCKET_AWS_SECRET',
    'CUBEJS_DB_EXPORT_BUCKET_AWS_REGION',
  ],
  vertica: [],
  prestodb: [],
  trino: [],
  mssql: [],
  duckdb: [],
};
