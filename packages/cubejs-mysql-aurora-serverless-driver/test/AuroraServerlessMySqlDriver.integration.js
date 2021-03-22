/* globals describe, afterAll, beforeAll, test, expect, jest */
const path = require('path');

const { DockerComposeEnvironment, Wait } = require('testcontainers');
const AWS = require('aws-sdk');
const AuroraServerlessMySqlDriver = require('../driver/AuroraServerlessMySqlDriver');

const DUMMY_SECRET_ARN = 'arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy';
const DUMMY_RESOURCE_ARN = 'arn:aws:rds:us-east-1:123456789012:cluster:dummy';

describe('AuroraServerlessMySqlDriver', () => {
  let env;
  let driver;

  jest.setTimeout(60 * 2 * 1000);

  beforeAll(async () => {
    const dc = new DockerComposeEnvironment(
      path.resolve(path.dirname(__filename), '../'),
      'docker-compose.yml'
    );

    env = await dc
      .withEnv('TEST_MYSQL_VERSION', process.env.TEST_MYSQL_VERSION || '5.6.50')
      .withEnv('TEST_LOCAL_DATA_API_VERSION', process.env.TEST_LOCAL_DATA_API_VERSION || '0.6.4')
      .withWaitStrategy('mysql', Wait.forHealthCheck())
      .up();

    // Configure the AWS SDK so that it doesn't get mad
    // AWS.config.credentials = new AWS.Credentials({ accessKeyId: 'awstest', secretAccessKey: 'awstest' });
    AWS.config.accessKeyId = 'awstest';
    AWS.config.secretAccessKey = 'awstest';
    AWS.config.region = 'us-east-1';
    AWS.config.sslEnabled = false;

    driver = new AuroraServerlessMySqlDriver({
      secretArn: DUMMY_SECRET_ARN,
      resourceArn: DUMMY_RESOURCE_ARN,
      database: 'mysql',
      options: {
        sslEnabled: false,
        endpoint: `http://${env.getContainer('router').getHost()}:${env.getContainer('router').getMappedPort(80)}`,
      }
    });

    await driver.testConnection();

    await driver.createSchemaIfNotExists('test');
    await driver.query('DROP SCHEMA test');
    await driver.createSchemaIfNotExists('test');
  });

  afterAll(async () => {
    if (env) {
      await env.stop();
    }
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
