const REQUIRED_ENV_VARS = [
  'CUBEJS_DB_URL',
  'CUBEJS_DB_NAME',
  'CUBEJS_DB_DREMIO_AUTH_TOKEN',
];

REQUIRED_ENV_VARS.forEach((key) => {
  // Trying to populate from DRIVERS_TESTS_DREMIO_* vars
  if (process.env[`DRIVERS_TESTS_DREMIO_${key}`] !== undefined) {
    process.env[key] = process.env[`DRIVERS_TESTS_DREMIO_${key}`];
  }
});
