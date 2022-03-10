import { createDriverTestCase } from './birdbox-driver.test';

createDriverTestCase(
  'bigquery',
  ['CUBEJS_DB_BQ_PROJECT_ID', 'CUBEJS_DB_EXPORT_BUCKET', 'CUBEJS_DB_BQ_CREDENTIALS']
);
