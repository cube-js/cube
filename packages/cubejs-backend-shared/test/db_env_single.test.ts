import { getEnv, assertDataSource, keyByDataSource } from '../src/env';

delete process.env.CUBEJS_DATASOURCES;

describe('Single datasources', () => {
  test('getEnv("dataSources")', () => {
    expect(getEnv('dataSources')).toEqual([]);
  });

  test('assertDataSource with multiple data sources', () => {
    expect(assertDataSource()).toEqual('default');
    expect(assertDataSource('default')).toEqual('default');
    expect(assertDataSource('postgres')).toEqual('postgres');
    expect(assertDataSource('wrong')).toEqual('wrong');
  });

  test('keyByDataSource(origin, dataSource?)', () => {
    expect(keyByDataSource('CUBEJS_VAR')).toEqual('CUBEJS_VAR');
    expect(keyByDataSource('CUBEJS_VAR', 'default')).toEqual('CUBEJS_VAR');
    expect(keyByDataSource('CUBEJS_VAR', 'postgres')).toEqual('CUBEJS_VAR');
    expect(keyByDataSource('CUBE_VAR', 'default')).toEqual('CUBE_VAR');
    expect(keyByDataSource('CUBE_VAR', 'postgres')).toEqual('CUBE_VAR');
    expect(keyByDataSource('CUBE_VAR', 'wrong')).toEqual('CUBE_VAR');
  });

  test('getEnv("dbType")', () => {
    process.env.CUBEJS_DB_TYPE = 'default1';
    expect(getEnv('dbType', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbType', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('dbType', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_TYPE = 'default2';
    expect(getEnv('dbType', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbType', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('dbType', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_TYPE;
    expect(getEnv('dbType', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbType', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('dbType', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("dbSsl")', () => {
    process.env.CUBEJS_DB_SSL = 'true';
    expect(getEnv('dbSsl', { dataSource: 'default' })).toEqual(true);
    expect(getEnv('dbSsl', { dataSource: 'postgres' })).toEqual(true);
    expect(getEnv('dbSsl', { dataSource: 'wrong' })).toEqual(true);

    process.env.CUBEJS_DB_SSL = 'false';
    expect(getEnv('dbSsl', { dataSource: 'default' })).toEqual(false);
    expect(getEnv('dbSsl', { dataSource: 'postgres' })).toEqual(false);
    expect(getEnv('dbSsl', { dataSource: 'wrong' })).toEqual(false);

    process.env.CUBEJS_DB_SSL = 'wrong';
    expect(() => getEnv('dbSsl', { dataSource: 'default' })).toThrow(
      'The CUBEJS_DB_SSL must be either \'true\' or \'false\'.'
    );
    expect(() => getEnv('dbSsl', { dataSource: 'postgres' })).toThrow(
      'The CUBEJS_DB_SSL must be either \'true\' or \'false\'.'
    );
    expect(() => getEnv('dbSsl', { dataSource: 'wrong' })).toThrow(
      'The CUBEJS_DB_SSL must be either \'true\' or \'false\'.'
    );

    delete process.env.CUBEJS_DB_SSL;
    expect(getEnv('dbSsl', { dataSource: 'default' })).toEqual(false);
    expect(getEnv('dbSsl', { dataSource: 'postgres' })).toEqual(false);
    expect(getEnv('dbSsl', { dataSource: 'wrong' })).toEqual(false);
  });

  test('getEnv("dbSslRejectUnauthorized")', () => {
    process.env.CUBEJS_DB_SSL_REJECT_UNAUTHORIZED = 'true';
    expect(getEnv('dbSslRejectUnauthorized', { dataSource: 'default' })).toEqual(true);
    expect(getEnv('dbSslRejectUnauthorized', { dataSource: 'postgres' })).toEqual(true);
    expect(getEnv('dbSslRejectUnauthorized', { dataSource: 'wrong' })).toEqual(true);

    process.env.CUBEJS_DB_SSL_REJECT_UNAUTHORIZED = 'false';
    expect(getEnv('dbSslRejectUnauthorized', { dataSource: 'default' })).toEqual(false);
    expect(getEnv('dbSslRejectUnauthorized', { dataSource: 'postgres' })).toEqual(false);
    expect(getEnv('dbSslRejectUnauthorized', { dataSource: 'wrong' })).toEqual(false);

    process.env.CUBEJS_DB_SSL_REJECT_UNAUTHORIZED = 'wrong';
    expect(() => getEnv('dbSslRejectUnauthorized', { dataSource: 'default' })).toThrow(
      'The CUBEJS_DB_SSL_REJECT_UNAUTHORIZED must be either \'true\' or \'false\'.'
    );
    expect(() => getEnv('dbSslRejectUnauthorized', { dataSource: 'postgres' })).toThrow(
      'The CUBEJS_DB_SSL_REJECT_UNAUTHORIZED must be either \'true\' or \'false\'.'
    );
    expect(() => getEnv('dbSslRejectUnauthorized', { dataSource: 'wrong' })).toThrow(
      'The CUBEJS_DB_SSL_REJECT_UNAUTHORIZED must be either \'true\' or \'false\'.'
    );

    delete process.env.CUBEJS_DB_SSL_REJECT_UNAUTHORIZED;
    expect(getEnv('dbSslRejectUnauthorized', { dataSource: 'default' })).toEqual(false);
    expect(getEnv('dbSslRejectUnauthorized', { dataSource: 'postgres' })).toEqual(false);
    expect(getEnv('dbSslRejectUnauthorized', { dataSource: 'wrong' })).toEqual(false);
  });

  test('getEnv("dbUrl")', () => {
    process.env.CUBEJS_DB_URL = 'default1';
    expect(getEnv('dbUrl', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbUrl', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('dbUrl', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_URL = 'default2';
    expect(getEnv('dbUrl', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbUrl', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('dbUrl', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_URL;
    expect(getEnv('dbUrl', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbUrl', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('dbUrl', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("dbHost")', () => {
    process.env.CUBEJS_DB_HOST = 'default1';
    expect(getEnv('dbHost', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbHost', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('dbHost', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_HOST = 'default2';
    expect(getEnv('dbHost', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbHost', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('dbHost', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_HOST;
    expect(getEnv('dbHost', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbHost', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('dbHost', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("dbDomain")', () => {
    process.env.CUBEJS_DB_DOMAIN = 'default1';
    expect(getEnv('dbDomain', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbDomain', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('dbDomain', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_DOMAIN = 'default2';
    expect(getEnv('dbDomain', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbDomain', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('dbDomain', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_DOMAIN;
    expect(getEnv('dbDomain', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbDomain', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('dbDomain', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("dbPort")', () => {
    process.env.CUBEJS_DB_PORT = '5432';
    expect(getEnv('dbPort', { dataSource: 'default' })).toEqual(5432);
    expect(getEnv('dbPort', { dataSource: 'postgres' })).toEqual(5432);
    expect(getEnv('dbPort', { dataSource: 'wrong' })).toEqual(5432);

    process.env.CUBEJS_DB_PORT = '2345';
    expect(getEnv('dbPort', { dataSource: 'default' })).toEqual(2345);
    expect(getEnv('dbPort', { dataSource: 'postgres' })).toEqual(2345);
    expect(getEnv('dbPort', { dataSource: 'wrong' })).toEqual(2345);

    delete process.env.CUBEJS_DB_PORT;
    expect(getEnv('dbPort', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbPort', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('dbPort', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("dbSocketPath")', () => {
    process.env.CUBEJS_DB_SOCKET_PATH = 'default1';
    expect(getEnv('dbSocketPath', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbSocketPath', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('dbSocketPath', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_SOCKET_PATH = 'default2';
    expect(getEnv('dbSocketPath', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbSocketPath', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('dbSocketPath', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_SOCKET_PATH;
    expect(getEnv('dbSocketPath', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbSocketPath', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('dbSocketPath', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("dbUser")', () => {
    process.env.CUBEJS_DB_USER = 'default1';
    expect(getEnv('dbUser', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbUser', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('dbUser', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_USER = 'default2';
    expect(getEnv('dbUser', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbUser', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('dbUser', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_USER;
    expect(getEnv('dbUser', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbUser', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('dbUser', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("dbPass")', () => {
    process.env.CUBEJS_DB_PASS = 'default1';
    expect(getEnv('dbPass', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbPass', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('dbPass', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_PASS = 'default2';
    expect(getEnv('dbPass', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbPass', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('dbPass', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_PASS;
    expect(getEnv('dbPass', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbPass', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('dbPass', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("dbName")', () => {
    process.env.CUBEJS_DB_NAME = 'default1';
    expect(getEnv('dbName', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbName', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('dbName', { dataSource: 'wrong' })).toEqual('default1');
    expect(getEnv('dbName', { dataSource: 'default', required: true })).toEqual('default1');
    expect(getEnv('dbName', { dataSource: 'postgres', required: true })).toEqual('default1');
    expect(getEnv('dbName', { dataSource: 'wrong', required: true })).toEqual('default1');

    process.env.CUBEJS_DB_NAME = 'default2';
    expect(getEnv('dbName', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbName', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('dbName', { dataSource: 'wrong' })).toEqual('default2');
    expect(getEnv('dbName', { dataSource: 'default', required: true })).toEqual('default2');
    expect(getEnv('dbName', { dataSource: 'postgres', required: true })).toEqual('default2');
    expect(getEnv('dbName', { dataSource: 'wrong', required: true })).toEqual('default2');

    delete process.env.CUBEJS_DB_NAME;
    expect(getEnv('dbName', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbName', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('dbName', { dataSource: 'wrong' })).toBeUndefined();
    expect(() => getEnv('dbName', {
      dataSource: 'default',
      required: true,
    })).toThrow('The CUBEJS_DB_NAME is required and missing.');
    expect(() => getEnv('dbName', {
      dataSource: 'postgres',
      required: true,
    })).toThrow('The CUBEJS_DB_NAME is required and missing.');
    expect(() => getEnv('dbName', {
      dataSource: 'wrong',
      required: true,
    })).toThrow('The CUBEJS_DB_NAME is required and missing.');
  });

  test('getEnv("dbSchema")', () => {
    process.env.CUBEJS_DB_SCHEMA = 'default1';
    expect(getEnv('dbSchema', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbSchema', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('dbSchema', { dataSource: 'wrong' })).toEqual('default1');
    expect(getEnv('dbSchema', { dataSource: 'default', required: true })).toEqual('default1');
    expect(getEnv('dbSchema', { dataSource: 'postgres', required: true })).toEqual('default1');
    expect(getEnv('dbSchema', { dataSource: 'wrong', required: true })).toEqual('default1');

    process.env.CUBEJS_DB_SCHEMA = 'default2';
    expect(getEnv('dbSchema', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbSchema', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('dbSchema', { dataSource: 'wrong' })).toEqual('default2');
    expect(getEnv('dbSchema', { dataSource: 'default', required: true })).toEqual('default2');
    expect(getEnv('dbSchema', { dataSource: 'postgres', required: true })).toEqual('default2');
    expect(getEnv('dbSchema', { dataSource: 'wrong', required: true })).toEqual('default2');

    delete process.env.CUBEJS_DB_SCHEMA;
    expect(getEnv('dbSchema', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbSchema', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('dbSchema', { dataSource: 'wrong' })).toBeUndefined();
    expect(() => getEnv('dbSchema', {
      dataSource: 'default',
      required: true,
    })).toThrow('The CUBEJS_DB_SCHEMA is required and missing.');
    expect(() => getEnv('dbSchema', {
      dataSource: 'postgres',
      required: true,
    })).toThrow('The CUBEJS_DB_SCHEMA is required and missing.');
    expect(() => getEnv('dbSchema', {
      dataSource: 'wrong',
      required: true,
    })).toThrow('The CUBEJS_DB_SCHEMA is required and missing.');
  });

  test('getEnv("dbDatabase")', () => {
    process.env.CUBEJS_DATABASE = 'default1';
    expect(getEnv('dbDatabase', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbDatabase', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('dbDatabase', { dataSource: 'wrong' })).toEqual('default1');
    expect(getEnv('dbDatabase', { dataSource: 'default', required: true })).toEqual('default1');
    expect(getEnv('dbDatabase', { dataSource: 'postgres', required: true })).toEqual('default1');
    expect(getEnv('dbDatabase', { dataSource: 'wrong', required: true })).toEqual('default1');

    process.env.CUBEJS_DATABASE = 'default2';
    expect(getEnv('dbDatabase', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbDatabase', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('dbDatabase', { dataSource: 'wrong' })).toEqual('default2');
    expect(getEnv('dbDatabase', { dataSource: 'default', required: true })).toEqual('default2');
    expect(getEnv('dbDatabase', { dataSource: 'postgres', required: true })).toEqual('default2');
    expect(getEnv('dbDatabase', { dataSource: 'wrong', required: true })).toEqual('default2');

    delete process.env.CUBEJS_DATABASE;
    expect(getEnv('dbDatabase', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbDatabase', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('dbDatabase', { dataSource: 'wrong' })).toBeUndefined();
    expect(() => getEnv('dbDatabase', {
      dataSource: 'default',
      required: true,
    })).toThrow('The CUBEJS_DATABASE is required and missing.');
    expect(() => getEnv('dbDatabase', {
      dataSource: 'postgres',
      required: true,
    })).toThrow('The CUBEJS_DATABASE is required and missing.');
    expect(() => getEnv('dbDatabase', {
      dataSource: 'wrong',
      required: true,
    })).toThrow('The CUBEJS_DATABASE is required and missing.');
  });

  test('getEnv("dbMaxPoolSize")', () => {
    process.env.CUBEJS_DB_MAX_POOL = '5432';
    expect(getEnv('dbMaxPoolSize', { dataSource: 'default' })).toEqual(5432);
    expect(getEnv('dbMaxPoolSize', { dataSource: 'postgres' })).toEqual(5432);
    expect(getEnv('dbMaxPoolSize', { dataSource: 'wrong' })).toEqual(5432);

    process.env.CUBEJS_DB_MAX_POOL = '2345';
    expect(getEnv('dbMaxPoolSize', { dataSource: 'default' })).toEqual(2345);
    expect(getEnv('dbMaxPoolSize', { dataSource: 'postgres' })).toEqual(2345);
    expect(getEnv('dbMaxPoolSize', { dataSource: 'wrong' })).toEqual(2345);

    delete process.env.CUBEJS_DB_MAX_POOL;
    expect(getEnv('dbMaxPoolSize', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbMaxPoolSize', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('dbMaxPoolSize', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("dbPollMaxInterval")', () => {
    process.env.CUBEJS_DB_POLL_MAX_INTERVAL = '4';
    expect(getEnv('dbPollMaxInterval', { dataSource: 'default' })).toEqual(4);
    expect(getEnv('dbPollMaxInterval', { dataSource: 'postgres' })).toEqual(4);
    expect(getEnv('dbPollMaxInterval', { dataSource: 'wrong' })).toEqual(4);

    process.env.CUBEJS_DB_POLL_MAX_INTERVAL = '3';
    expect(getEnv('dbPollMaxInterval', { dataSource: 'default' })).toEqual(3);
    expect(getEnv('dbPollMaxInterval', { dataSource: 'postgres' })).toEqual(3);
    expect(getEnv('dbPollMaxInterval', { dataSource: 'wrong' })).toEqual(3);

    delete process.env.CUBEJS_DB_POLL_MAX_INTERVAL;
    expect(getEnv('dbPollMaxInterval', { dataSource: 'default' })).toEqual(5);
    expect(getEnv('dbPollMaxInterval', { dataSource: 'postgres' })).toEqual(5);
    expect(getEnv('dbPollMaxInterval', { dataSource: 'wrong' })).toEqual(5);
  });

  test('getEnv("dbPollTimeout")', () => {
    process.env.CUBEJS_DB_POLL_TIMEOUT = '4';
    expect(getEnv('dbPollTimeout', { dataSource: 'default' })).toEqual(4);
    expect(getEnv('dbPollTimeout', { dataSource: 'postgres' })).toEqual(4);
    expect(getEnv('dbPollTimeout', { dataSource: 'wrong' })).toEqual(4);

    process.env.CUBEJS_DB_POLL_TIMEOUT = '3';
    expect(getEnv('dbPollTimeout', { dataSource: 'default' })).toEqual(3);
    expect(getEnv('dbPollTimeout', { dataSource: 'postgres' })).toEqual(3);
    expect(getEnv('dbPollTimeout', { dataSource: 'wrong' })).toEqual(3);

    delete process.env.CUBEJS_DB_POLL_TIMEOUT;
    expect(getEnv('dbPollTimeout', { dataSource: 'default' })).toEqual(null);
    expect(getEnv('dbPollTimeout', { dataSource: 'postgres' })).toEqual(null);
    expect(getEnv('dbPollTimeout', { dataSource: 'wrong' })).toEqual(null);
  });

  test('getEnv("dbQueryTimeout")', () => {
    process.env.CUBEJS_DB_QUERY_TIMEOUT = '4';
    expect(getEnv('dbQueryTimeout', { dataSource: 'default' })).toEqual(4);
    expect(getEnv('dbQueryTimeout', { dataSource: 'postgres' })).toEqual(4);
    expect(getEnv('dbQueryTimeout', { dataSource: 'wrong' })).toEqual(4);

    process.env.CUBEJS_DB_QUERY_TIMEOUT = '3';
    expect(getEnv('dbQueryTimeout', { dataSource: 'default' })).toEqual(3);
    expect(getEnv('dbQueryTimeout', { dataSource: 'postgres' })).toEqual(3);
    expect(getEnv('dbQueryTimeout', { dataSource: 'wrong' })).toEqual(3);

    delete process.env.CUBEJS_DB_QUERY_TIMEOUT;
    expect(getEnv('dbQueryTimeout', { dataSource: 'default' })).toEqual(600);
    expect(getEnv('dbQueryTimeout', { dataSource: 'postgres' })).toEqual(600);
    expect(getEnv('dbQueryTimeout', { dataSource: 'wrong' })).toEqual(600);
  });

  test('getEnv("jdbcUrl")', () => {
    process.env.CUBEJS_JDBC_URL = 'default1';
    expect(getEnv('jdbcUrl', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('jdbcUrl', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('jdbcUrl', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_JDBC_URL = 'default2';
    expect(getEnv('jdbcUrl', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('jdbcUrl', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('jdbcUrl', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_JDBC_URL;
    expect(getEnv('jdbcUrl', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('jdbcUrl', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('jdbcUrl', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("jdbcDriver")', () => {
    process.env.CUBEJS_JDBC_DRIVER = 'default1';
    expect(getEnv('jdbcDriver', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('jdbcDriver', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('jdbcDriver', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_JDBC_DRIVER = 'default2';
    expect(getEnv('jdbcDriver', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('jdbcDriver', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('jdbcDriver', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_JDBC_DRIVER;
    expect(getEnv('jdbcDriver', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('jdbcDriver', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('jdbcDriver', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("dbExportBucketCsvEscapeSymbol")', () => {
    process.env.CUBEJS_DB_EXPORT_BUCKET_CSV_ESCAPE_SYMBOL = '"';
    expect(getEnv('dbExportBucketCsvEscapeSymbol', { dataSource: 'default' })).toEqual('"');
    expect(getEnv('dbExportBucketCsvEscapeSymbol', { dataSource: 'postgres' })).toEqual('"');
    expect(getEnv('dbExportBucketCsvEscapeSymbol', { dataSource: 'wrong' })).toEqual('"');

    process.env.CUBEJS_DB_EXPORT_BUCKET_CSV_ESCAPE_SYMBOL = '\'';
    expect(getEnv('dbExportBucketCsvEscapeSymbol', { dataSource: 'default' })).toEqual('\'');
    expect(getEnv('dbExportBucketCsvEscapeSymbol', { dataSource: 'postgres' })).toEqual('\'');
    expect(getEnv('dbExportBucketCsvEscapeSymbol', { dataSource: 'wrong' })).toEqual('\'');

    delete process.env.CUBEJS_DB_EXPORT_BUCKET_CSV_ESCAPE_SYMBOL;
    expect(getEnv('dbExportBucketCsvEscapeSymbol', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbExportBucketCsvEscapeSymbol', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('dbExportBucketCsvEscapeSymbol', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("dbExportBucketType")', () => {
    process.env.CUBEJS_DB_EXPORT_BUCKET_TYPE = 'default1';
    expect(getEnv('dbExportBucketType', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbExportBucketType', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('dbExportBucketType', { dataSource: 'wrong' })).toEqual('default1');
    expect(getEnv('dbExportBucketType', {
      dataSource: 'default',
      supported: ['default1'],
    })).toEqual('default1');
    expect(getEnv('dbExportBucketType', {
      dataSource: 'postgres',
      supported: ['default1'],
    })).toEqual('default1');
    expect(getEnv('dbExportBucketType', {
      dataSource: 'wrong',
      supported: ['default1'],
    })).toEqual('default1');
    expect(() => getEnv('dbExportBucketType', {
      dataSource: 'default',
      supported: [],
    })).toThrow('The CUBEJS_DB_EXPORT_BUCKET_TYPE must be one of the [].');
    expect(() => getEnv('dbExportBucketType', {
      dataSource: 'postgres',
      supported: [],
    })).toThrow('The CUBEJS_DB_EXPORT_BUCKET_TYPE must be one of the [].');
    expect(() => getEnv('dbExportBucketType', {
      dataSource: 'wrong',
      supported: [],
    })).toThrow('The CUBEJS_DB_EXPORT_BUCKET_TYPE must be one of the [].');

    process.env.CUBEJS_DB_EXPORT_BUCKET_TYPE = 'default2';
    expect(getEnv('dbExportBucketType', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbExportBucketType', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('dbExportBucketType', { dataSource: 'wrong' })).toEqual('default2');
    expect(getEnv('dbExportBucketType', {
      dataSource: 'default',
      supported: ['default2'],
    })).toEqual('default2');
    expect(getEnv('dbExportBucketType', {
      dataSource: 'postgres',
      supported: ['default2'],
    })).toEqual('default2');
    expect(getEnv('dbExportBucketType', {
      dataSource: 'wrong',
      supported: ['default2'],
    })).toEqual('default2');
    expect(() => getEnv('dbExportBucketType', {
      dataSource: 'default',
      supported: [],
    })).toThrow('The CUBEJS_DB_EXPORT_BUCKET_TYPE must be one of the [].');
    expect(() => getEnv('dbExportBucketType', {
      dataSource: 'postgres',
      supported: [],
    })).toThrow('The CUBEJS_DB_EXPORT_BUCKET_TYPE must be one of the [].');
    expect(() => getEnv('dbExportBucketType', {
      dataSource: 'wrong',
      supported: [],
    })).toThrow('The CUBEJS_DB_EXPORT_BUCKET_TYPE must be one of the [].');

    delete process.env.CUBEJS_DB_EXPORT_BUCKET_TYPE;
    expect(getEnv('dbExportBucketType', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbExportBucketType', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('dbExportBucketType', { dataSource: 'wrong' })).toBeUndefined();
    expect(getEnv('dbExportBucketType', {
      dataSource: 'default',
      supported: [],
    })).toBeUndefined();
    expect(getEnv('dbExportBucketType', {
      dataSource: 'postgres',
      supported: [],
    })).toBeUndefined();
    expect(getEnv('dbExportBucketType', {
      dataSource: 'wrong',
      supported: [],
    })).toBeUndefined();
  });

  test('getEnv("dbExportBucket")', () => {
    process.env.CUBEJS_DB_EXPORT_BUCKET = 'default1';
    expect(getEnv('dbExportBucket', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbExportBucket', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('dbExportBucket', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_EXPORT_BUCKET = 'default2';
    expect(getEnv('dbExportBucket', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbExportBucket', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('dbExportBucket', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_EXPORT_BUCKET;
    expect(getEnv('dbExportBucket', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbExportBucket', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('dbExportBucket', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("dbExportBucketMountDir")', () => {
    process.env.CUBEJS_DB_EXPORT_BUCKET_MOUNT_DIR = 'default1';
    expect(getEnv('dbExportBucketMountDir', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbExportBucketMountDir', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('dbExportBucketMountDir', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_EXPORT_BUCKET_MOUNT_DIR = 'default2';
    expect(getEnv('dbExportBucketMountDir', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbExportBucketMountDir', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('dbExportBucketMountDir', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_EXPORT_BUCKET_MOUNT_DIR;
    expect(getEnv('dbExportBucketMountDir', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbExportBucketMountDir', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('dbExportBucketMountDir', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("dbExportBucketAwsKey")', () => {
    process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_KEY = 'default1';
    expect(getEnv('dbExportBucketAwsKey', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbExportBucketAwsKey', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('dbExportBucketAwsKey', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_KEY = 'default2';
    expect(getEnv('dbExportBucketAwsKey', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbExportBucketAwsKey', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('dbExportBucketAwsKey', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_KEY;
    expect(getEnv('dbExportBucketAwsKey', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbExportBucketAwsKey', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('dbExportBucketAwsKey', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("dbExportBucketAwsSecret")', () => {
    process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_SECRET = 'default1';
    expect(getEnv('dbExportBucketAwsSecret', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbExportBucketAwsSecret', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('dbExportBucketAwsSecret', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_SECRET = 'default2';
    expect(getEnv('dbExportBucketAwsSecret', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbExportBucketAwsSecret', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('dbExportBucketAwsSecret', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_SECRET;
    expect(getEnv('dbExportBucketAwsSecret', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbExportBucketAwsSecret', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('dbExportBucketAwsSecret', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("dbExportBucketAwsRegion")', () => {
    process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_REGION = 'default1';
    expect(getEnv('dbExportBucketAwsRegion', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbExportBucketAwsRegion', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('dbExportBucketAwsRegion', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_REGION = 'default2';
    expect(getEnv('dbExportBucketAwsRegion', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbExportBucketAwsRegion', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('dbExportBucketAwsRegion', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_REGION;
    expect(getEnv('dbExportBucketAwsRegion', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbExportBucketAwsRegion', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('dbExportBucketAwsRegion', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("dbExportBucketAzureKey")', () => {
    process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_KEY = 'default1';
    expect(getEnv('dbExportBucketAzureKey', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbExportBucketAzureKey', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('dbExportBucketAzureKey', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_KEY = 'default2';
    expect(getEnv('dbExportBucketAzureKey', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbExportBucketAzureKey', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('dbExportBucketAzureKey', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_KEY;
    expect(getEnv('dbExportBucketAzureKey', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbExportBucketAzureKey', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('dbExportBucketAzureKey', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("dbExportBucketAzureTenantId")', () => {
    process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_TENANT_ID = 'default1';
    expect(getEnv('dbExportBucketAzureTenantId', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbExportBucketAzureTenantId', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('dbExportBucketAzureTenantId', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_TENANT_ID = 'default2';
    expect(getEnv('dbExportBucketAzureTenantId', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbExportBucketAzureTenantId', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('dbExportBucketAzureTenantId', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_TENANT_ID;
    expect(getEnv('dbExportBucketAzureTenantId', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbExportBucketAzureTenantId', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('dbExportBucketAzureTenantId', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("dbExportBucketAzureClientId")', () => {
    process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_CLIENT_ID = 'default1';
    expect(getEnv('dbExportBucketAzureClientId', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbExportBucketAzureClientId', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('dbExportBucketAzureClientId', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_CLIENT_ID = 'default2';
    expect(getEnv('dbExportBucketAzureClientId', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbExportBucketAzureClientId', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('dbExportBucketAzureClientId', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_CLIENT_ID;
    expect(getEnv('dbExportBucketAzureClientId', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbExportBucketAzureClientId', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('dbExportBucketAzureClientId', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("dbExportBucketAzureClientSecret")', () => {
    process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_CLIENT_SECRET = 'default1';
    expect(getEnv('dbExportBucketAzureClientSecret', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbExportBucketAzureClientSecret', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('dbExportBucketAzureClientSecret', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_CLIENT_SECRET = 'default2';
    expect(getEnv('dbExportBucketAzureClientSecret', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbExportBucketAzureClientSecret', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('dbExportBucketAzureClientSecret', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_CLIENT_SECRET;
    expect(getEnv('dbExportBucketAzureClientSecret', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbExportBucketAzureClientSecret', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('dbExportBucketAzureClientSecret', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("dbExportIntegration")', () => {
    process.env.CUBEJS_DB_EXPORT_INTEGRATION = 'default1';
    expect(getEnv('dbExportIntegration', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbExportIntegration', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('dbExportIntegration', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_EXPORT_INTEGRATION = 'default2';
    expect(getEnv('dbExportIntegration', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbExportIntegration', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('dbExportIntegration', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_EXPORT_INTEGRATION;
    expect(getEnv('dbExportIntegration', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbExportIntegration', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('dbExportIntegration', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("dbExportGCSCredentials")', () => {
    process.env.CUBEJS_DB_EXPORT_GCS_CREDENTIALS = 'eyJhIjogMX0=';
    expect(getEnv('dbExportGCSCredentials', { dataSource: 'default' })).toEqual({ a: 1 });
    expect(getEnv('dbExportGCSCredentials', { dataSource: 'postgres' })).toEqual({ a: 1 });
    expect(getEnv('dbExportGCSCredentials', { dataSource: 'wrong' })).toEqual({ a: 1 });

    process.env.CUBEJS_DB_EXPORT_GCS_CREDENTIALS = 'eyJhIjogMn0=';
    expect(getEnv('dbExportGCSCredentials', { dataSource: 'default' })).toEqual({ a: 2 });
    expect(getEnv('dbExportGCSCredentials', { dataSource: 'postgres' })).toEqual({ a: 2 });
    expect(getEnv('dbExportGCSCredentials', { dataSource: 'wrong' })).toEqual({ a: 2 });

    delete process.env.CUBEJS_DB_EXPORT_GCS_CREDENTIALS;
    expect(getEnv('dbExportGCSCredentials', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbExportGCSCredentials', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('dbExportGCSCredentials', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("databrickUrl")', () => {
    process.env.CUBEJS_DB_DATABRICKS_URL = 'default1';
    expect(getEnv('databrickUrl', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('databrickUrl', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('databrickUrl', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_DATABRICKS_URL = 'default2';
    expect(getEnv('databrickUrl', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('databrickUrl', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('databrickUrl', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_DATABRICKS_URL;
    expect(() => getEnv('databrickUrl', { dataSource: 'default' })).toThrow(
      'The CUBEJS_DB_DATABRICKS_URL is required and missing.'
    );
    expect(() => getEnv('databrickUrl', { dataSource: 'postgres' })).toThrow(
      'The CUBEJS_DB_DATABRICKS_URL is required and missing.'
    );
    expect(() => getEnv('databrickUrl', { dataSource: 'wrong' })).toThrow(
      'The CUBEJS_DB_DATABRICKS_URL is required and missing.'
    );
  });

  test('getEnv("databrickToken")', () => {
    process.env.CUBEJS_DB_DATABRICKS_TOKEN = 'default1';
    expect(getEnv('databrickToken', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('databrickToken', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('databrickToken', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_DATABRICKS_TOKEN = 'default2';
    expect(getEnv('databrickToken', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('databrickToken', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('databrickToken', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_DATABRICKS_TOKEN;
    expect(getEnv('databrickToken', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('databrickToken', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('databrickToken', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("databricksCatalog")', () => {
    process.env.CUBEJS_DB_DATABRICKS_CATALOG = 'default1';
    expect(getEnv('databricksCatalog', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('databricksCatalog', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('databricksCatalog', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_DATABRICKS_CATALOG = 'default2';
    expect(getEnv('databricksCatalog', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('databricksCatalog', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('databricksCatalog', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_DATABRICKS_CATALOG;
    expect(getEnv('databricksCatalog', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('databricksCatalog', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('databricksCatalog', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("databrickAcceptPolicy")', () => {
    process.env.CUBEJS_DB_DATABRICKS_ACCEPT_POLICY = 'true';
    expect(getEnv('databrickAcceptPolicy', { dataSource: 'default' })).toEqual(true);
    expect(getEnv('databrickAcceptPolicy', { dataSource: 'postgres' })).toEqual(true);
    expect(getEnv('databrickAcceptPolicy', { dataSource: 'wrong' })).toEqual(true);

    process.env.CUBEJS_DB_DATABRICKS_ACCEPT_POLICY = 'false';
    expect(getEnv('databrickAcceptPolicy', { dataSource: 'default' })).toEqual(false);
    expect(getEnv('databrickAcceptPolicy', { dataSource: 'postgres' })).toEqual(false);
    expect(getEnv('databrickAcceptPolicy', { dataSource: 'wrong' })).toEqual(false);

    process.env.CUBEJS_DB_DATABRICKS_ACCEPT_POLICY = 'wrong';
    expect(() => getEnv('databrickAcceptPolicy', { dataSource: 'default' })).toThrow(
      'env-var: "CUBEJS_DB_DATABRICKS_ACCEPT_POLICY" should be either "true", "false", "TRUE", or "FALSE"'
    );
    expect(() => getEnv('databrickAcceptPolicy', { dataSource: 'postgres' })).toThrow(
      'env-var: "CUBEJS_DB_DATABRICKS_ACCEPT_POLICY" should be either "true", "false", "TRUE", or "FALSE"'
    );
    expect(() => getEnv('databrickAcceptPolicy', { dataSource: 'wrong' })).toThrow(
      'env-var: "CUBEJS_DB_DATABRICKS_ACCEPT_POLICY" should be either "true", "false", "TRUE", or "FALSE"'
    );

    delete process.env.CUBEJS_DB_DATABRICKS_ACCEPT_POLICY;
    expect(getEnv('databrickAcceptPolicy', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('databrickAcceptPolicy', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('databrickAcceptPolicy', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("athenaAwsKey")', () => {
    process.env.CUBEJS_AWS_KEY = 'default1';
    expect(getEnv('athenaAwsKey', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('athenaAwsKey', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('athenaAwsKey', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_AWS_KEY = 'default2';
    expect(getEnv('athenaAwsKey', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('athenaAwsKey', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('athenaAwsKey', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_AWS_KEY;
    expect(getEnv('athenaAwsKey', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('athenaAwsKey', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('athenaAwsKey', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("athenaAwsSecret")', () => {
    process.env.CUBEJS_AWS_SECRET = 'default1';
    expect(getEnv('athenaAwsSecret', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('athenaAwsSecret', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('athenaAwsSecret', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_AWS_SECRET = 'default2';
    expect(getEnv('athenaAwsSecret', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('athenaAwsSecret', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('athenaAwsSecret', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_AWS_SECRET;
    expect(getEnv('athenaAwsSecret', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('athenaAwsSecret', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('athenaAwsSecret', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("athenaAwsRegion")', () => {
    process.env.CUBEJS_AWS_REGION = 'default1';
    expect(getEnv('athenaAwsRegion', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('athenaAwsRegion', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('athenaAwsRegion', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_AWS_REGION = 'default2';
    expect(getEnv('athenaAwsRegion', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('athenaAwsRegion', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('athenaAwsRegion', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_AWS_REGION;
    expect(getEnv('athenaAwsRegion', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('athenaAwsRegion', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('athenaAwsRegion', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("athenaAwsS3OutputLocation")', () => {
    process.env.CUBEJS_AWS_S3_OUTPUT_LOCATION = 'default1';
    expect(getEnv('athenaAwsS3OutputLocation', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('athenaAwsS3OutputLocation', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('athenaAwsS3OutputLocation', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_AWS_S3_OUTPUT_LOCATION = 'default2';
    expect(getEnv('athenaAwsS3OutputLocation', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('athenaAwsS3OutputLocation', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('athenaAwsS3OutputLocation', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_AWS_S3_OUTPUT_LOCATION;
    expect(getEnv('athenaAwsS3OutputLocation', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('athenaAwsS3OutputLocation', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('athenaAwsS3OutputLocation', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("athenaAwsWorkgroup")', () => {
    process.env.CUBEJS_AWS_ATHENA_WORKGROUP = 'default1';
    expect(getEnv('athenaAwsWorkgroup', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('athenaAwsWorkgroup', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('athenaAwsWorkgroup', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_AWS_ATHENA_WORKGROUP = 'default2';
    expect(getEnv('athenaAwsWorkgroup', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('athenaAwsWorkgroup', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('athenaAwsWorkgroup', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_AWS_ATHENA_WORKGROUP;
    expect(getEnv('athenaAwsWorkgroup', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('athenaAwsWorkgroup', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('athenaAwsWorkgroup', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("bigqueryProjectId")', () => {
    process.env.CUBEJS_DB_BQ_PROJECT_ID = 'default1';
    expect(getEnv('bigqueryProjectId', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('bigqueryProjectId', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('bigqueryProjectId', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_BQ_PROJECT_ID = 'default2';
    expect(getEnv('bigqueryProjectId', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('bigqueryProjectId', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('bigqueryProjectId', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_BQ_PROJECT_ID;
    expect(getEnv('bigqueryProjectId', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('bigqueryProjectId', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('bigqueryProjectId', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("bigqueryKeyFile")', () => {
    process.env.CUBEJS_DB_BQ_KEY_FILE = 'default1';
    expect(getEnv('bigqueryKeyFile', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('bigqueryKeyFile', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('bigqueryKeyFile', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_BQ_KEY_FILE = 'default2';
    expect(getEnv('bigqueryKeyFile', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('bigqueryKeyFile', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('bigqueryKeyFile', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_BQ_KEY_FILE;
    expect(getEnv('bigqueryKeyFile', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('bigqueryKeyFile', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('bigqueryKeyFile', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("bigqueryCredentials")', () => {
    process.env.CUBEJS_DB_BQ_CREDENTIALS = 'default1';
    expect(getEnv('bigqueryCredentials', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('bigqueryCredentials', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('bigqueryCredentials', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_BQ_CREDENTIALS = 'default2';
    expect(getEnv('bigqueryCredentials', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('bigqueryCredentials', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('bigqueryCredentials', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_BQ_CREDENTIALS;
    expect(getEnv('bigqueryCredentials', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('bigqueryCredentials', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('bigqueryCredentials', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("bigqueryLocation")', () => {
    process.env.CUBEJS_DB_BQ_LOCATION = 'default1';
    expect(getEnv('bigqueryLocation', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('bigqueryLocation', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('bigqueryLocation', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_BQ_LOCATION = 'default2';
    expect(getEnv('bigqueryLocation', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('bigqueryLocation', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('bigqueryLocation', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_BQ_LOCATION;
    expect(getEnv('bigqueryLocation', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('bigqueryLocation', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('bigqueryLocation', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("bigqueryExportBucket")', () => {
    process.env.CUBEJS_DB_BQ_EXPORT_BUCKET = 'default1';
    expect(getEnv('bigqueryExportBucket', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('bigqueryExportBucket', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('bigqueryExportBucket', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_BQ_EXPORT_BUCKET = 'default2';
    expect(getEnv('bigqueryExportBucket', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('bigqueryExportBucket', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('bigqueryExportBucket', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_BQ_EXPORT_BUCKET;
    expect(getEnv('bigqueryExportBucket', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('bigqueryExportBucket', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('bigqueryExportBucket', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("clickhouseReadOnly")', () => {
    process.env.CUBEJS_DB_CLICKHOUSE_READONLY = 'default1';
    expect(getEnv('clickhouseReadOnly', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('clickhouseReadOnly', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('clickhouseReadOnly', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_CLICKHOUSE_READONLY = 'default2';
    expect(getEnv('clickhouseReadOnly', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('clickhouseReadOnly', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('clickhouseReadOnly', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_CLICKHOUSE_READONLY;
    expect(getEnv('clickhouseReadOnly', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('clickhouseReadOnly', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('clickhouseReadOnly', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("elasticApiId")', () => {
    process.env.CUBEJS_DB_ELASTIC_APIKEY_ID = 'default1';
    expect(getEnv('elasticApiId', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('elasticApiId', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('elasticApiId', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_ELASTIC_APIKEY_ID = 'default2';
    expect(getEnv('elasticApiId', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('elasticApiId', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('elasticApiId', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_ELASTIC_APIKEY_ID;
    expect(getEnv('elasticApiId', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('elasticApiId', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('elasticApiId', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("elasticApiKey")', () => {
    process.env.CUBEJS_DB_ELASTIC_APIKEY_KEY = 'default1';
    expect(getEnv('elasticApiKey', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('elasticApiKey', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('elasticApiKey', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_ELASTIC_APIKEY_KEY = 'default2';
    expect(getEnv('elasticApiKey', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('elasticApiKey', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('elasticApiKey', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_ELASTIC_APIKEY_KEY;
    expect(getEnv('elasticApiKey', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('elasticApiKey', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('elasticApiKey', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("elasticOpenDistro")', () => {
    process.env.CUBEJS_DB_ELASTIC_OPENDISTRO = 'default1';
    expect(getEnv('elasticOpenDistro', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('elasticOpenDistro', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('elasticOpenDistro', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_ELASTIC_OPENDISTRO = 'default2';
    expect(getEnv('elasticOpenDistro', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('elasticOpenDistro', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('elasticOpenDistro', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_ELASTIC_OPENDISTRO;
    expect(getEnv('elasticOpenDistro', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('elasticOpenDistro', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('elasticOpenDistro', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("elasticQueryFormat")', () => {
    process.env.CUBEJS_DB_ELASTIC_QUERY_FORMAT = 'default1';
    expect(getEnv('elasticQueryFormat', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('elasticQueryFormat', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('elasticQueryFormat', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_ELASTIC_QUERY_FORMAT = 'default2';
    expect(getEnv('elasticQueryFormat', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('elasticQueryFormat', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('elasticQueryFormat', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_ELASTIC_QUERY_FORMAT;
    expect(getEnv('elasticQueryFormat', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('elasticQueryFormat', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('elasticQueryFormat', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("fireboltApiEndpoint")', () => {
    process.env.CUBEJS_FIREBOLT_API_ENDPOINT = 'default1';
    expect(getEnv('fireboltApiEndpoint', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('fireboltApiEndpoint', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('fireboltApiEndpoint', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_FIREBOLT_API_ENDPOINT = 'default2';
    expect(getEnv('fireboltApiEndpoint', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('fireboltApiEndpoint', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('fireboltApiEndpoint', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_FIREBOLT_API_ENDPOINT;
    expect(getEnv('fireboltApiEndpoint', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('fireboltApiEndpoint', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('fireboltApiEndpoint', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("fireboltEngineName")', () => {
    process.env.CUBEJS_FIREBOLT_ENGINE_NAME = 'default1';
    expect(getEnv('fireboltEngineName', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('fireboltEngineName', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('fireboltEngineName', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_FIREBOLT_ENGINE_NAME = 'default2';
    expect(getEnv('fireboltEngineName', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('fireboltEngineName', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('fireboltEngineName', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_FIREBOLT_ENGINE_NAME;
    expect(getEnv('fireboltEngineName', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('fireboltEngineName', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('fireboltEngineName', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("fireboltEngineEndpoint")', () => {
    process.env.CUBEJS_FIREBOLT_ENGINE_ENDPOINT = 'default1';
    expect(getEnv('fireboltEngineEndpoint', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('fireboltEngineEndpoint', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('fireboltEngineEndpoint', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_FIREBOLT_ENGINE_ENDPOINT = 'default2';
    expect(getEnv('fireboltEngineEndpoint', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('fireboltEngineEndpoint', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('fireboltEngineEndpoint', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_FIREBOLT_ENGINE_ENDPOINT;
    expect(getEnv('fireboltEngineEndpoint', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('fireboltEngineEndpoint', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('fireboltEngineEndpoint', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("fireboltAccount")', () => {
    process.env.CUBEJS_FIREBOLT_ACCOUNT = "default1";
    expect(getEnv('fireboltAccount', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('fireboltAccount', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('fireboltAccount', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_FIREBOLT_ACCOUNT = "default2";
    expect(getEnv('fireboltAccount', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('fireboltAccount', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('fireboltAccount', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_FIREBOLT_ACCOUNT;
    expect(getEnv('fireboltAccount', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('fireboltAccount', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('fireboltAccount', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("hiveType")', () => {
    process.env.CUBEJS_DB_HIVE_TYPE = 'default1';
    expect(getEnv('hiveType', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('hiveType', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('hiveType', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_HIVE_TYPE = 'default2';
    expect(getEnv('hiveType', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('hiveType', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('hiveType', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_HIVE_TYPE;
    expect(getEnv('hiveType', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('hiveType', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('hiveType', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("hiveVer")', () => {
    process.env.CUBEJS_DB_HIVE_VER = 'default1';
    expect(getEnv('hiveVer', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('hiveVer', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('hiveVer', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_HIVE_VER = 'default2';
    expect(getEnv('hiveVer', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('hiveVer', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('hiveVer', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_HIVE_VER;
    expect(getEnv('hiveVer', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('hiveVer', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('hiveVer', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("hiveThriftVer")', () => {
    process.env.CUBEJS_DB_HIVE_THRIFT_VER = 'default1';
    expect(getEnv('hiveThriftVer', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('hiveThriftVer', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('hiveThriftVer', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_HIVE_THRIFT_VER = 'default2';
    expect(getEnv('hiveThriftVer', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('hiveThriftVer', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('hiveThriftVer', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_HIVE_THRIFT_VER;
    expect(getEnv('hiveThriftVer', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('hiveThriftVer', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('hiveThriftVer', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("hiveCdhVer")', () => {
    process.env.CUBEJS_DB_HIVE_CDH_VER = 'default1';
    expect(getEnv('hiveCdhVer', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('hiveCdhVer', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('hiveCdhVer', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_HIVE_CDH_VER = 'default2';
    expect(getEnv('hiveCdhVer', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('hiveCdhVer', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('hiveCdhVer', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_HIVE_CDH_VER;
    expect(getEnv('hiveCdhVer', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('hiveCdhVer', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('hiveCdhVer', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("auroraSecretArn")', () => {
    process.env.CUBEJS_DATABASE_SECRET_ARN = 'default1';
    expect(getEnv('auroraSecretArn', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('auroraSecretArn', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('auroraSecretArn', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DATABASE_SECRET_ARN = 'default2';
    expect(getEnv('auroraSecretArn', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('auroraSecretArn', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('auroraSecretArn', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DATABASE_SECRET_ARN;
    expect(getEnv('auroraSecretArn', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('auroraSecretArn', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('auroraSecretArn', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("auroraClusterArn")', () => {
    process.env.CUBEJS_DATABASE_CLUSTER_ARN = 'default1';
    expect(getEnv('auroraClusterArn', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('auroraClusterArn', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('auroraClusterArn', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DATABASE_CLUSTER_ARN = 'default2';
    expect(getEnv('auroraClusterArn', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('auroraClusterArn', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('auroraClusterArn', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DATABASE_CLUSTER_ARN;
    expect(getEnv('auroraClusterArn', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('auroraClusterArn', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('auroraClusterArn', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("redshiftUnloadArn")', () => {
    process.env.CUBEJS_DB_EXPORT_BUCKET_REDSHIFT_ARN = 'default1';
    expect(getEnv('redshiftUnloadArn', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('redshiftUnloadArn', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('redshiftUnloadArn', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_EXPORT_BUCKET_REDSHIFT_ARN = 'default2';
    expect(getEnv('redshiftUnloadArn', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('redshiftUnloadArn', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('redshiftUnloadArn', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_EXPORT_BUCKET_REDSHIFT_ARN;
    expect(getEnv('redshiftUnloadArn', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('redshiftUnloadArn', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('redshiftUnloadArn', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("snowflakeAccount")', () => {
    process.env.CUBEJS_DB_SNOWFLAKE_ACCOUNT = 'default1';
    expect(getEnv('snowflakeAccount', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('snowflakeAccount', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('snowflakeAccount', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_SNOWFLAKE_ACCOUNT = 'default2';
    expect(getEnv('snowflakeAccount', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('snowflakeAccount', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('snowflakeAccount', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_SNOWFLAKE_ACCOUNT;
    expect(getEnv('snowflakeAccount', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('snowflakeAccount', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('snowflakeAccount', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("snowflakeRegion")', () => {
    process.env.CUBEJS_DB_SNOWFLAKE_REGION = 'default1';
    expect(getEnv('snowflakeRegion', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('snowflakeRegion', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('snowflakeRegion', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_SNOWFLAKE_REGION = 'default2';
    expect(getEnv('snowflakeRegion', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('snowflakeRegion', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('snowflakeRegion', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_SNOWFLAKE_REGION;
    expect(getEnv('snowflakeRegion', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('snowflakeRegion', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('snowflakeRegion', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("snowflakeWarehouse")', () => {
    process.env.CUBEJS_DB_SNOWFLAKE_WAREHOUSE = 'default1';
    expect(getEnv('snowflakeWarehouse', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('snowflakeWarehouse', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('snowflakeWarehouse', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_SNOWFLAKE_WAREHOUSE = 'default2';
    expect(getEnv('snowflakeWarehouse', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('snowflakeWarehouse', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('snowflakeWarehouse', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_SNOWFLAKE_WAREHOUSE;
    expect(getEnv('snowflakeWarehouse', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('snowflakeWarehouse', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('snowflakeWarehouse', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("snowflakeRole")', () => {
    process.env.CUBEJS_DB_SNOWFLAKE_ROLE = 'default1';
    expect(getEnv('snowflakeRole', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('snowflakeRole', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('snowflakeRole', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_SNOWFLAKE_ROLE = 'default2';
    expect(getEnv('snowflakeRole', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('snowflakeRole', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('snowflakeRole', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_SNOWFLAKE_ROLE;
    expect(getEnv('snowflakeRole', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('snowflakeRole', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('snowflakeRole', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("snowflakeSessionKeepAlive")', () => {
    process.env.CUBEJS_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE = 'true';
    expect(getEnv('snowflakeSessionKeepAlive', { dataSource: 'default' })).toEqual(true);
    expect(getEnv('snowflakeSessionKeepAlive', { dataSource: 'postgres' })).toEqual(true);
    expect(getEnv('snowflakeSessionKeepAlive', { dataSource: 'wrong' })).toEqual(true);

    process.env.CUBEJS_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE = 'false';
    expect(getEnv('snowflakeSessionKeepAlive', { dataSource: 'default' })).toEqual(false);
    expect(getEnv('snowflakeSessionKeepAlive', { dataSource: 'postgres' })).toEqual(false);
    expect(getEnv('snowflakeSessionKeepAlive', { dataSource: 'wrong' })).toEqual(false);

    process.env.CUBEJS_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE = 'wrong';
    expect(() => getEnv('snowflakeSessionKeepAlive', { dataSource: 'default' })).toThrow(
      'The CUBEJS_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE must be either \'true\' or \'false\'.'
    );
    expect(() => getEnv('snowflakeSessionKeepAlive', { dataSource: 'postgres' })).toThrow(
      'The CUBEJS_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE must be either \'true\' or \'false\'.'
    );
    expect(() => getEnv('snowflakeSessionKeepAlive', { dataSource: 'wrong' })).toThrow(
      'The CUBEJS_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE must be either \'true\' or \'false\'.'
    );

    delete process.env.CUBEJS_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE;
    expect(getEnv('snowflakeSessionKeepAlive', { dataSource: 'default' })).toEqual(true);
    expect(getEnv('snowflakeSessionKeepAlive', { dataSource: 'postgres' })).toEqual(true);
    expect(getEnv('snowflakeSessionKeepAlive', { dataSource: 'wrong' })).toEqual(true);
  });

  test('getEnv("snowflakeAuthenticator")', () => {
    process.env.CUBEJS_DB_SNOWFLAKE_AUTHENTICATOR = 'default1';
    expect(getEnv('snowflakeAuthenticator', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('snowflakeAuthenticator', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('snowflakeAuthenticator', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_SNOWFLAKE_AUTHENTICATOR = 'default2';
    expect(getEnv('snowflakeAuthenticator', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('snowflakeAuthenticator', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('snowflakeAuthenticator', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_SNOWFLAKE_AUTHENTICATOR;
    expect(getEnv('snowflakeAuthenticator', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('snowflakeAuthenticator', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('snowflakeAuthenticator', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("snowflakePrivateKey")', () => {
    process.env.CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY = 'default1';
    expect(getEnv('snowflakePrivateKey', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('snowflakePrivateKey', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('snowflakePrivateKey', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY = 'default2';
    expect(getEnv('snowflakePrivateKey', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('snowflakePrivateKey', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('snowflakePrivateKey', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY;
    expect(getEnv('snowflakePrivateKey', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('snowflakePrivateKey', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('snowflakePrivateKey', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("snowflakePrivateKeyPath")', () => {
    process.env.CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PATH = 'default1';
    expect(getEnv('snowflakePrivateKeyPath', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('snowflakePrivateKeyPath', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('snowflakePrivateKeyPath', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PATH = 'default2';
    expect(getEnv('snowflakePrivateKeyPath', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('snowflakePrivateKeyPath', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('snowflakePrivateKeyPath', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PATH;
    expect(getEnv('snowflakePrivateKeyPath', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('snowflakePrivateKeyPath', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('snowflakePrivateKeyPath', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("snowflakePrivateKeyPass")', () => {
    process.env.CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PASS = 'default1';
    expect(getEnv('snowflakePrivateKeyPass', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('snowflakePrivateKeyPass', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('snowflakePrivateKeyPass', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PASS = 'default2';
    expect(getEnv('snowflakePrivateKeyPass', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('snowflakePrivateKeyPass', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('snowflakePrivateKeyPass', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PASS;
    expect(getEnv('snowflakePrivateKeyPass', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('snowflakePrivateKeyPass', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('snowflakePrivateKeyPass', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("dbCatalog")', () => {
    process.env.CUBEJS_DB_CATALOG = 'default1';
    expect(getEnv('dbCatalog', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbCatalog', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('dbCatalog', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_CATALOG = 'default2';
    expect(getEnv('dbCatalog', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbCatalog', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('dbCatalog', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_CATALOG;
    expect(getEnv('dbCatalog', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbCatalog', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('dbCatalog', { dataSource: 'wrong' })).toBeUndefined();
  });

  test('getEnv("prestoCatalog")', () => {
    process.env.CUBEJS_DB_PRESTO_CATALOG = 'default1';
    expect(getEnv('prestoCatalog', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('prestoCatalog', { dataSource: 'postgres' })).toEqual('default1');
    expect(getEnv('prestoCatalog', { dataSource: 'wrong' })).toEqual('default1');

    process.env.CUBEJS_DB_PRESTO_CATALOG = 'default2';
    expect(getEnv('prestoCatalog', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('prestoCatalog', { dataSource: 'postgres' })).toEqual('default2');
    expect(getEnv('prestoCatalog', { dataSource: 'wrong' })).toEqual('default2');

    delete process.env.CUBEJS_DB_PRESTO_CATALOG;
    expect(getEnv('prestoCatalog', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('prestoCatalog', { dataSource: 'postgres' })).toBeUndefined();
    expect(getEnv('prestoCatalog', { dataSource: 'wrong' })).toBeUndefined();
  });
});
