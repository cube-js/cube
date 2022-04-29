/**
 * Required environment variables per datasource.
 */
export const REQ_ENV_VARS: {[key: string]: string[]} = {
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
  multidb: [],
  questdb: [],
  postgres: [
    'CUBEJS_DB_USER',
    'CUBEJS_DB_PASS',
    'CUBEJS_DB_TYPE',
  ],
};
