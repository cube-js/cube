/* globals describe, afterAll, beforeAll, test, expect, jest */
const { GenericContainer } = require('testcontainers');
const AWS = require('aws-sdk');
const AuroraServerlessMySqlDriver = require('../driver/AuroraServerlessMySqlDriver');

const DUMMY_SECRET_ARN = 'arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy';
const DUMMY_RESOURCE_ARN = 'arn:aws:rds:us-east-1:123456789012:cluster:dummy';

describe('AuroraServerlessMySqlDriver', () => {
  let mysqlContainer;
  let container;
  let driver;

  jest.setTimeout(120000);

  // Aurora Serverless doesn't support mysql 8.0 && We want to bypass the ssl defauls
  const mysqlVersion = process.env.TEST_MYSQL_VERSION || '5.6.50';
  const localDataApiVersion = process.env.TEST_LOCAL_DATA_API_VERSION || 'latest';

  beforeAll(async () => {
    const mysqlRootPassword = process.env.TEST_DB_PASSWORD || 'Test1test';

    mysqlContainer = await new GenericContainer('mysql', mysqlVersion)
      .withEnv('MYSQL_ROOT_PASSWORD', mysqlRootPassword)
      .withExposedPorts(3306)
      .start();

    const mappedSqlPort = mysqlContainer && mysqlContainer.getMappedPort(3306) || 3306;

    container = await new GenericContainer('koxudaxi/local-data-api', localDataApiVersion)
      .withEnv('MYSQL_HOST', 'host.docker.internal')
      .withEnv('MYSQL_PORT', mappedSqlPort)
      .withEnv('MYSQL_USER', 'root')
      .withEnv('MYSQL_PASSWORD', mysqlRootPassword)
      .withEnv('SECRET_ARN', DUMMY_SECRET_ARN)
      .withEnv('RESOURCE_ARN', DUMMY_RESOURCE_ARN)
      .withExposedPorts(80)
      .start();

    const mappedPort = container.getMappedPort(80);
    const host = container.getHost();

    const endpoint = `http://${host}:${mappedPort}`;

    // Configure the AWS SDK so that it doesn't get mad
    // AWS.config.credentials = new AWS.Credentials({ accessKeyId: 'awstest', secretAccessKey: 'awstest' });
    AWS.config.region = 'us-east-1';
    AWS.config.endpoint = new AWS.Endpoint(endpoint);

    driver = new AuroraServerlessMySqlDriver({
      secretArn: DUMMY_SECRET_ARN,
      resourceArn: DUMMY_RESOURCE_ARN,
      database: 'mysql'
    });

    await driver.createSchemaIfNotExists('test');
    await driver.query('DROP SCHEMA test');
    await driver.createSchemaIfNotExists('test');
  });

  afterAll(async () => {
    if (container) await container.stop();
    if (mysqlContainer) await mysqlContainer.stop();
  });

  test('basic query', async () => {
    await driver.query('SELECT 1');
  });

  test('truncated wrong value', async () => {
    await driver.uploadTable('test.wrong_value', [{ name: 'value', type: 'string' }], {
      rows: [{ value: 'Tekirdağ' }]
    });

    expect(JSON.parse(JSON.stringify(await driver.query('select * from test.wrong_value'))))
      .toStrictEqual([{ value: 'Tekirdağ' }]);

    expect(JSON.parse(JSON.stringify((await driver.downloadQueryResults('select * from test.wrong_value')).rows)))
      .toStrictEqual([{ value: 'Tekirdağ' }]);
  });

  test('boolean field', async () => {
    await driver.uploadTable('test.boolean', [{ name: 'b_value', type: 'boolean' }], {
      rows: [
        { b_value: true },
        { b_value: true },
        { b_value: 'true' },
        { b_value: false },
        { b_value: 'false' },
        { b_value: null }
      ]
    });

    expect(JSON.parse(JSON.stringify(await driver.query('select * from test.boolean where b_value = ?', [true]))))
      .toStrictEqual([{ b_value: true }, { b_value: true }, { b_value: true }]);

    expect(JSON.parse(JSON.stringify(await driver.query('select * from test.boolean where b_value = ?', [false]))))
      .toStrictEqual([{ b_value: false }, { b_value: false }]);
  });
});
