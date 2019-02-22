/*
global test expect
*/
const { express } = require("./templates");

const dotEnv = express.files['.env'];

const secret = 123;
const generateTestEnv = (apiSecret, dbType) => ({ apiSecret, dbType });

test('dotEnv should return default env vars for mysql DB type', () => {
  const dbType = 'mysql';
  const expectedDotEnvVars = `CUBEJS_DB_HOST=<YOUR_DB_HOST_HERE>
CUBEJS_DB_NAME=<YOUR_DB_NAME_HERE>
CUBEJS_DB_USER=<YOUR_DB_USER_HERE>
CUBEJS_DB_PASS=<YOUR_DB_PASS_HERE>
CUBEJS_DB_TYPE=${dbType}
CUBEJS_API_SECRET=${secret}`;

  expect(dotEnv(generateTestEnv(secret, dbType))).toBe(expectedDotEnvVars);
});

test('dotEnv should return default env vars for unsupported DB type', () => {
  const dbType = 'unsupported';
  const expectedDotEnvVars = `CUBEJS_DB_HOST=<YOUR_DB_HOST_HERE>
CUBEJS_DB_NAME=<YOUR_DB_NAME_HERE>
CUBEJS_DB_USER=<YOUR_DB_USER_HERE>
CUBEJS_DB_PASS=<YOUR_DB_PASS_HERE>
CUBEJS_DB_TYPE=${dbType}
CUBEJS_API_SECRET=${secret}`;

  expect(dotEnv(generateTestEnv(secret, dbType))).toBe(expectedDotEnvVars);
});

test('dotEnv should return Athena-specific env vars for Athena DB type', () => {
  const dbType = 'athena';
  const expectedDotEnvVars = `CUBEJS_AWS_KEY=<YOUR ATHENA AWS KEY HERE>
CUBEJS_AWS_SECRET=<YOUR ATHENA SECRET KEY HERE>
CUBEJS_AWS_REGION=<AWS REGION STRING, e.g. us-east-1>
# You can find the Athena S3 Output location here: https://docs.aws.amazon.com/athena/latest/ug/querying.html
CUBEJS_AWS_S3_OUTPUT_LOCATION=<S3 OUTPUT LOCATION>
CUBEJS_JDBC_DRIVER=athena
CUBEJS_DB_TYPE=${dbType}
CUBEJS_API_SECRET=${secret}`;

  expect(dotEnv(generateTestEnv(secret, dbType))).toBe(expectedDotEnvVars);
});
