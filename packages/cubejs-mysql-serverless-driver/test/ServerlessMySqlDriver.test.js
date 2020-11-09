/* globals describe, test, expect */
const ServerlessMySqlDriver = require('../driver/ServerlessMySqlDriver');

const DUMMY_SECRET_ARN = 'arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy';
const DUMMY_RESOURCE_ARN = 'arn:aws:rds:us-east-1:123456789012:cluster:dummy';

const driver = new ServerlessMySqlDriver({
  secretArn: DUMMY_SECRET_ARN,
  resourceArn: DUMMY_RESOURCE_ARN,
  database: 'mysql'
});

describe('ServerlessMySqlDriver Unit', () => {
  test('quote identifier', () => {
    const identifier = driver.quoteIdentifier('test');
    expect(identifier).toEqual('`test`');
  });

  test('position bindings', () => {
    const sql = 'select * from something where val = ?';
    const replaceBindings = driver.positionBindings(sql);
    expect(replaceBindings).toEqual('select * from something where val = :b0');
  });
});
