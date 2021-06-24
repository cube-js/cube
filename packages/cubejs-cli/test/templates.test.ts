import templates from '../src/templates';

const dotEnv = templates.express.files['.env'];

const secret = 123;

const generateTestEnv = (apiSecret, dbType) => ({
  apiSecret,
  dbType,
  dockerVersion: 'latest',
  projectName: 'test',
});

test('dotEnv should return default env vars for mysql DB type', () => {
  const dbType = 'mysql';
  const expectedDotEnvVars = `# Cube.js environment variables: https://cube.dev/docs/reference/environment-variables
CUBEJS_DEV_MODE=true
CUBEJS_DB_TYPE=${dbType}
CUBEJS_API_SECRET=${secret}
CUBEJS_EXTERNAL_DEFAULT=true
CUBEJS_SCHEDULED_REFRESH_DEFAULT=true
CUBEJS_WEB_SOCKETS=true`;

  expect(dotEnv(generateTestEnv(secret, dbType))).toBe(expectedDotEnvVars);
});

test('dotEnv should return default env vars for unsupported DB type', () => {
  const dbType = 'unsupported';
  const expectedDotEnvVars = `# Cube.js environment variables: https://cube.dev/docs/reference/environment-variables
CUBEJS_DEV_MODE=true
CUBEJS_DB_TYPE=${dbType}
CUBEJS_API_SECRET=${secret}
CUBEJS_EXTERNAL_DEFAULT=true
CUBEJS_SCHEDULED_REFRESH_DEFAULT=true
CUBEJS_WEB_SOCKETS=true`;

  expect(dotEnv(generateTestEnv(secret, dbType))).toBe(expectedDotEnvVars);
});

test('dotEnv should return Athena-specific env vars for Athena DB type', () => {
  const dbType = 'athena';
  const expectedDotEnvVars = `# Cube.js environment variables: https://cube.dev/docs/reference/environment-variables
CUBEJS_AWS_KEY=<YOUR ATHENA AWS KEY HERE>
CUBEJS_AWS_SECRET=<YOUR ATHENA SECRET KEY HERE>
CUBEJS_AWS_REGION=<AWS REGION STRING, e.g. us-east-1>
# You can find the Athena S3 Output location here: https://docs.aws.amazon.com/athena/latest/ug/querying.html
CUBEJS_AWS_S3_OUTPUT_LOCATION=<S3 OUTPUT LOCATION>
CUBEJS_JDBC_DRIVER=athena
CUBEJS_DEV_MODE=true
CUBEJS_DB_TYPE=${dbType}
CUBEJS_API_SECRET=${secret}
CUBEJS_EXTERNAL_DEFAULT=true
CUBEJS_SCHEDULED_REFRESH_DEFAULT=true`;

  expect(dotEnv(generateTestEnv(secret, dbType))).toBe(expectedDotEnvVars);
});
