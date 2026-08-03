const REQUIRED_ENV_VARS = [
  'CUBEJS_DB_USER',
  'CUBEJS_DB_PASS',
  'CUBEJS_DB_NAME',
  'CUBEJS_FIREBOLT_ENGINE_NAME',
  'CUBEJS_FIREBOLT_ACCOUNT',
];

REQUIRED_ENV_VARS.forEach((key) => {
  // Trying to populate from DRIVERS_TESTS_FIREBOLT_* vars
  if (process.env[`DRIVERS_TESTS_FIREBOLT_${key}`] !== undefined) {
    process.env[key] = process.env[`DRIVERS_TESTS_FIREBOLT_${key}`];
  }
});
