import { createDriverTestCase } from './birdbox-driver.test';

createDriverTestCase(
  'athena',
  ['CUBEJS_AWS_KEY', 'CUBEJS_AWS_SECRET', 'CUBEJS_AWS_REGION', 'CUBEJS_AWS_S3_OUTPUT_LOCATION', 'CUBEJS_DB_EXPORT_BUCKET']
);
