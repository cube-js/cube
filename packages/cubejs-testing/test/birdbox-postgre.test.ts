import { createDriverTestCase } from './birdbox-driver.test';

createDriverTestCase(
  'postgresql',
  [
    'CUBEJS_DB_HOST',
    'CUBEJS_DB_PORT',
    'CUBEJS_DB_NAME',
    'CUBEJS_DB_USER',
    'CUBEJS_DB_PASS',
    'CUBEJS_DB_TYPE',
    'CUBEJS_API_SECRET',
    'CUBEJS_EXTERNAL_DEFAULT',
    'CUBEJS_SCHEDULED_REFRESH_DEFAULT',
    'CUBEJS_DEV_MODE',
    'CUBEJS_WEB_SOCKETS',
  ]
);
