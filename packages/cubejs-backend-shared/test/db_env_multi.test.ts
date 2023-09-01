import { getEnv, assertDataSource, keyByDataSource } from '../src/env';

process.env.CUBEJS_DATASOURCES = ' default, postgres ';

describe('Multiple datasources', () => {
  test('getEnv("dataSources")', () => {
    expect(getEnv('dataSources')).toEqual(['default', 'postgres']);
  });

  test('assertDataSource(dataSource)', () => {
    expect(assertDataSource()).toEqual('default');
    expect(assertDataSource('default')).toEqual('default');
    expect(assertDataSource('postgres')).toEqual('postgres');
    expect(() => assertDataSource('wrong')).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('keyByDataSource(origin, dataSource?)', () => {
    expect(keyByDataSource('CUBEJS_VAR')).toEqual('CUBEJS_VAR');
    expect(keyByDataSource('CUBEJS_VAR', 'default')).toEqual('CUBEJS_VAR');
    expect(keyByDataSource('CUBEJS_VAR', 'postgres')).toEqual('CUBEJS_DS_POSTGRES_VAR');
    expect(keyByDataSource('CUBE_VAR', 'default')).toEqual('CUBE_VAR');
    expect(() => keyByDataSource('CUBE_VAR', 'postgres')).toThrow(
      'The CUBE_VAR environment variable can not be converted for the postgres data source.'
    );
    expect(() => keyByDataSource('CUBE_VAR', 'wrong')).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbType")', () => {
    process.env.CUBEJS_DB_TYPE = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_TYPE = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_TYPE = 'wrong1';
    expect(getEnv('dbType', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbType', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('dbType', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_TYPE = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_TYPE = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_TYPE = 'wrong2';
    expect(getEnv('dbType', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbType', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('dbType', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_TYPE;
    delete process.env.CUBEJS_DS_POSTGRES_DB_TYPE;
    delete process.env.CUBEJS_DS_WRONG_DB_TYPE;
    expect(getEnv('dbType', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbType', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('dbType', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbSsl")', () => {
    process.env.CUBEJS_DB_SSL = 'true';
    process.env.CUBEJS_DS_POSTGRES_DB_SSL = 'true';
    process.env.CUBEJS_DS_WRONG_DB_SSL = 'true';
    expect(getEnv('dbSsl', { dataSource: 'default' })).toEqual(true);
    expect(getEnv('dbSsl', { dataSource: 'postgres' })).toEqual(true);
    expect(() => getEnv('dbSsl', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_SSL = 'false';
    process.env.CUBEJS_DS_POSTGRES_DB_SSL = 'false';
    process.env.CUBEJS_DS_WRONG_DB_SSL = 'false';
    expect(getEnv('dbSsl', { dataSource: 'default' })).toEqual(false);
    expect(getEnv('dbSsl', { dataSource: 'postgres' })).toEqual(false);
    expect(() => getEnv('dbSsl', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_SSL = 'wrong';
    process.env.CUBEJS_DS_POSTGRES_DB_SSL = 'wrong';
    process.env.CUBEJS_DS_WRONG_DB_SSL = 'wrong';
    expect(() => getEnv('dbSsl', { dataSource: 'default' })).toThrow(
      'The CUBEJS_DB_SSL must be either \'true\' or \'false\'.'
    );
    expect(() => getEnv('dbSsl', { dataSource: 'postgres' })).toThrow(
      'The CUBEJS_DS_POSTGRES_DB_SSL must be either \'true\' or \'false\'.'
    );
    expect(() => getEnv('dbSsl', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_SSL;
    delete process.env.CUBEJS_DS_POSTGRES_DB_SSL;
    delete process.env.CUBEJS_DS_WRONG_DB_SSL;
    expect(getEnv('dbSsl', { dataSource: 'default' })).toEqual(false);
    expect(getEnv('dbSsl', { dataSource: 'postgres' })).toEqual(false);
    expect(() => getEnv('dbSsl', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbSslRejectUnauthorized")', () => {
    process.env.CUBEJS_DB_SSL_REJECT_UNAUTHORIZED = 'true';
    process.env.CUBEJS_DS_POSTGRES_DB_SSL_REJECT_UNAUTHORIZED = 'true';
    process.env.CUBEJS_DS_WRONG_DB_SSL_REJECT_UNAUTHORIZED = 'true';
    expect(getEnv('dbSslRejectUnauthorized', { dataSource: 'default' })).toEqual(true);
    expect(getEnv('dbSslRejectUnauthorized', { dataSource: 'postgres' })).toEqual(true);
    expect(() => getEnv('dbSslRejectUnauthorized', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_SSL_REJECT_UNAUTHORIZED = 'false';
    process.env.CUBEJS_DS_POSTGRES_DB_SSL_REJECT_UNAUTHORIZED = 'false';
    process.env.CUBEJS_DS_WRONG_DB_SSL_REJECT_UNAUTHORIZED = 'false';
    expect(getEnv('dbSslRejectUnauthorized', { dataSource: 'default' })).toEqual(false);
    expect(getEnv('dbSslRejectUnauthorized', { dataSource: 'postgres' })).toEqual(false);
    expect(() => getEnv('dbSslRejectUnauthorized', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_SSL_REJECT_UNAUTHORIZED = 'wrong';
    process.env.CUBEJS_DS_POSTGRES_DB_SSL_REJECT_UNAUTHORIZED = 'wrong';
    process.env.CUBEJS_DS_WRONG_DB_SSL_REJECT_UNAUTHORIZED = 'wrong';
    expect(() => getEnv('dbSslRejectUnauthorized', { dataSource: 'default' })).toThrow(
      'The CUBEJS_DB_SSL_REJECT_UNAUTHORIZED must be either \'true\' or \'false\'.'
    );
    expect(() => getEnv('dbSslRejectUnauthorized', { dataSource: 'postgres' })).toThrow(
      'The CUBEJS_DS_POSTGRES_DB_SSL_REJECT_UNAUTHORIZED must be either \'true\' or \'false\'.'
    );
    expect(() => getEnv('dbSslRejectUnauthorized', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_SSL_REJECT_UNAUTHORIZED;
    delete process.env.CUBEJS_DS_POSTGRES_DB_SSL_REJECT_UNAUTHORIZED;
    delete process.env.CUBEJS_DS_WRONG_DB_SSL_REJECT_UNAUTHORIZED;
    expect(getEnv('dbSslRejectUnauthorized', { dataSource: 'default' })).toEqual(false);
    expect(getEnv('dbSslRejectUnauthorized', { dataSource: 'postgres' })).toEqual(false);
    expect(() => getEnv('dbSslRejectUnauthorized', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbUrl")', () => {
    process.env.CUBEJS_DB_URL = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_URL = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_URL = 'wrong1';
    expect(getEnv('dbUrl', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbUrl', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('dbUrl', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_URL = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_URL = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_URL = 'wrong2';
    expect(getEnv('dbUrl', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbUrl', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('dbUrl', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_URL;
    delete process.env.CUBEJS_DS_POSTGRES_DB_URL;
    delete process.env.CUBEJS_DS_WRONG_DB_URL;
    expect(getEnv('dbUrl', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbUrl', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('dbUrl', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbHost")', () => {
    process.env.CUBEJS_DB_HOST = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_HOST = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_HOST = 'wrong1';
    expect(getEnv('dbHost', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbHost', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('dbHost', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_HOST = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_HOST = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_HOST = 'wrong2';
    expect(getEnv('dbHost', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbHost', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('dbHost', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_HOST;
    delete process.env.CUBEJS_DS_POSTGRES_DB_HOST;
    delete process.env.CUBEJS_DS_WRONG_DB_HOST;
    expect(getEnv('dbHost', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbHost', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('dbHost', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbDomain")', () => {
    process.env.CUBEJS_DB_DOMAIN = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_DOMAIN = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_DOMAIN = 'wrong1';
    expect(getEnv('dbDomain', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbDomain', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('dbDomain', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_DOMAIN = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_DOMAIN = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_DOMAIN = 'wrong2';
    expect(getEnv('dbDomain', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbDomain', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('dbDomain', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_DOMAIN;
    delete process.env.CUBEJS_DS_POSTGRES_DB_DOMAIN;
    delete process.env.CUBEJS_DS_WRONG_DB_DOMAIN;
    expect(getEnv('dbDomain', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbDomain', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('dbDomain', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbPort")', () => {
    process.env.CUBEJS_DB_PORT = '55555';
    process.env.CUBEJS_DS_POSTGRES_DB_PORT = '44444';
    process.env.CUBEJS_DS_WRONG_DB_PORT = '33333';
    expect(getEnv('dbPort', { dataSource: 'default' })).toEqual(55555);
    expect(getEnv('dbPort', { dataSource: 'postgres' })).toEqual(44444);
    expect(() => getEnv('dbPort', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_PORT = '44444';
    process.env.CUBEJS_DS_POSTGRES_DB_PORT = '55555';
    process.env.CUBEJS_DS_WRONG_DB_PORT = '33333';
    expect(getEnv('dbPort', { dataSource: 'default' })).toEqual(44444);
    expect(getEnv('dbPort', { dataSource: 'postgres' })).toEqual(55555);
    expect(() => getEnv('dbPort', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_PORT;
    delete process.env.CUBEJS_DS_POSTGRES_DB_PORT;
    delete process.env.CUBEJS_DS_WRONG_DB_PORT;
    expect(getEnv('dbPort', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbPort', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('dbPort', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbSocketPath")', () => {
    process.env.CUBEJS_DB_SOCKET_PATH = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_SOCKET_PATH = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_SOCKET_PATH = 'wrong1';
    expect(getEnv('dbSocketPath', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbSocketPath', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('dbSocketPath', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_SOCKET_PATH = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_SOCKET_PATH = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_SOCKET_PATH = 'wrong2';
    expect(getEnv('dbSocketPath', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbSocketPath', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('dbSocketPath', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_SOCKET_PATH;
    delete process.env.CUBEJS_DS_POSTGRES_DB_SOCKET_PATH;
    delete process.env.CUBEJS_DS_WRONG_DB_SOCKET_PATH;
    expect(getEnv('dbSocketPath', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbSocketPath', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('dbSocketPath', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbUser")', () => {
    process.env.CUBEJS_DB_USER = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_USER = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_USER = 'wrong1';
    expect(getEnv('dbUser', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbUser', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('dbUser', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_USER = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_USER = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_USER = 'wrong2';
    expect(getEnv('dbUser', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbUser', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('dbUser', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_USER;
    delete process.env.CUBEJS_DS_POSTGRES_DB_USER;
    delete process.env.CUBEJS_DS_WRONG_DB_USER;
    expect(getEnv('dbUser', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbUser', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('dbUser', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbPass")', () => {
    process.env.CUBEJS_DB_PASS = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_PASS = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_PASS = 'wrong1';
    expect(getEnv('dbPass', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbPass', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('dbPass', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_PASS = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_PASS = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_PASS = 'wrong2';
    expect(getEnv('dbPass', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbPass', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('dbPass', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_PASS;
    delete process.env.CUBEJS_DS_POSTGRES_DB_PASS;
    delete process.env.CUBEJS_DS_WRONG_DB_PASS;
    expect(getEnv('dbPass', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbPass', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('dbPass', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbName")', () => {
    process.env.CUBEJS_DB_NAME = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_NAME = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_NAME = 'wrong1';
    expect(getEnv('dbName', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbName', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('dbName', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
    expect(getEnv('dbName', { dataSource: 'default', required: true })).toEqual('default1');
    expect(getEnv('dbName', { dataSource: 'postgres', required: true })).toEqual('postgres1');
    expect(() => getEnv('dbName', { dataSource: 'wrong', required: true })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_NAME = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_NAME = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_NAME = 'wrong2';
    expect(getEnv('dbName', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbName', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('dbName', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
    expect(getEnv('dbName', { dataSource: 'default', required: true })).toEqual('default2');
    expect(getEnv('dbName', { dataSource: 'postgres', required: true })).toEqual('postgres2');
    expect(() => getEnv('dbName', { dataSource: 'wrong', required: true })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_NAME;
    delete process.env.CUBEJS_DS_POSTGRES_DB_NAME;
    delete process.env.CUBEJS_DS_WRONG_DB_NAME;
    expect(getEnv('dbName', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbName', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('dbName', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
    expect(() => getEnv('dbName', {
      dataSource: 'default',
      required: true,
    })).toThrow('The CUBEJS_DB_NAME is required and missing.');
    expect(() => getEnv('dbName', {
      dataSource: 'postgres',
      required: true,
    })).toThrow('The CUBEJS_DS_POSTGRES_DB_NAME is required and missing.');
    expect(() => getEnv('dbName', { dataSource: 'wrong', required: true })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbSchema")', () => {
    process.env.CUBEJS_DB_SCHEMA = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_SCHEMA = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_SCHEMA = 'wrong1';
    expect(getEnv('dbSchema', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbSchema', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('dbSchema', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
    expect(getEnv('dbSchema', { dataSource: 'default', required: true })).toEqual('default1');
    expect(getEnv('dbSchema', { dataSource: 'postgres', required: true })).toEqual('postgres1');
    expect(() => getEnv('dbSchema', { dataSource: 'wrong', required: true })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_SCHEMA = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_SCHEMA = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_SCHEMA = 'wrong2';
    expect(getEnv('dbSchema', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbSchema', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('dbSchema', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
    expect(getEnv('dbSchema', { dataSource: 'default', required: true })).toEqual('default2');
    expect(getEnv('dbSchema', { dataSource: 'postgres', required: true })).toEqual('postgres2');
    expect(() => getEnv('dbSchema', { dataSource: 'wrong', required: true })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_SCHEMA;
    delete process.env.CUBEJS_DS_POSTGRES_DB_SCHEMA;
    delete process.env.CUBEJS_DS_WRONG_DB_SCHEMA;
    expect(getEnv('dbSchema', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbSchema', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('dbSchema', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
    expect(() => getEnv('dbSchema', {
      dataSource: 'default',
      required: true,
    })).toThrow('The CUBEJS_DB_SCHEMA is required and missing.');
    expect(() => getEnv('dbSchema', {
      dataSource: 'postgres',
      required: true,
    })).toThrow('The CUBEJS_DS_POSTGRES_DB_SCHEMA is required and missing.');
    expect(() => getEnv('dbSchema', { dataSource: 'wrong', required: true })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbDatabase")', () => {
    process.env.CUBEJS_DATABASE = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DATABASE = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DATABASE = 'wrong1';
    expect(getEnv('dbDatabase', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbDatabase', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('dbDatabase', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
    expect(getEnv('dbDatabase', { dataSource: 'default', required: true })).toEqual('default1');
    expect(getEnv('dbDatabase', { dataSource: 'postgres', required: true })).toEqual('postgres1');
    expect(() => getEnv('dbDatabase', { dataSource: 'wrong', required: true })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DATABASE = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DATABASE = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DATABASE = 'wrong2';
    expect(getEnv('dbDatabase', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbDatabase', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('dbDatabase', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
    expect(getEnv('dbDatabase', { dataSource: 'default', required: true })).toEqual('default2');
    expect(getEnv('dbDatabase', { dataSource: 'postgres', required: true })).toEqual('postgres2');
    expect(() => getEnv('dbDatabase', { dataSource: 'wrong', required: true })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DATABASE;
    delete process.env.CUBEJS_DS_POSTGRES_DATABASE;
    delete process.env.CUBEJS_DS_WRONG_DATABASE;
    expect(getEnv('dbDatabase', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbDatabase', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('dbDatabase', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
    expect(() => getEnv('dbDatabase', {
      dataSource: 'default',
      required: true,
    })).toThrow('The CUBEJS_DATABASE is required and missing.');
    expect(() => getEnv('dbDatabase', {
      dataSource: 'postgres',
      required: true,
    })).toThrow('The CUBEJS_DS_POSTGRES_DATABASE is required and missing.');
    expect(() => getEnv('dbDatabase', { dataSource: 'wrong', required: true })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbMaxPoolSize")', () => {
    process.env.CUBEJS_DB_MAX_POOL = '55555';
    process.env.CUBEJS_DS_POSTGRES_DB_MAX_POOL = '44444';
    process.env.CUBEJS_DS_WRONG_DB_MAX_POOL = '33333';
    expect(getEnv('dbMaxPoolSize', { dataSource: 'default' })).toEqual(55555);
    expect(getEnv('dbMaxPoolSize', { dataSource: 'postgres' })).toEqual(44444);
    expect(() => getEnv('dbMaxPoolSize', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_MAX_POOL = '44444';
    process.env.CUBEJS_DS_POSTGRES_DB_MAX_POOL = '55555';
    process.env.CUBEJS_DS_WRONG_DB_MAX_POOL = '33333';
    expect(getEnv('dbMaxPoolSize', { dataSource: 'default' })).toEqual(44444);
    expect(getEnv('dbMaxPoolSize', { dataSource: 'postgres' })).toEqual(55555);
    expect(() => getEnv('dbMaxPoolSize', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_MAX_POOL;
    delete process.env.CUBEJS_DS_POSTGRES_DB_MAX_POOL;
    delete process.env.CUBEJS_DS_WRONG_DB_MAX_POOL;
    expect(getEnv('dbMaxPoolSize', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbMaxPoolSize', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('dbMaxPoolSize', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbPollMaxInterval")', () => {
    process.env.CUBEJS_DB_POLL_MAX_INTERVAL = '5';
    process.env.CUBEJS_DS_POSTGRES_DB_POLL_MAX_INTERVAL = '4';
    process.env.CUBEJS_DS_WRONG_DB_POLL_MAX_INTERVAL = '3';
    expect(getEnv('dbPollMaxInterval', { dataSource: 'default' })).toEqual(5);
    expect(getEnv('dbPollMaxInterval', { dataSource: 'postgres' })).toEqual(4);
    expect(() => getEnv('dbPollMaxInterval', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_POLL_MAX_INTERVAL = '4';
    process.env.CUBEJS_DS_POSTGRES_DB_POLL_MAX_INTERVAL = '5';
    process.env.CUBEJS_DS_WRONG_DB_POLL_MAX_INTERVAL = '3';
    expect(getEnv('dbPollMaxInterval', { dataSource: 'default' })).toEqual(4);
    expect(getEnv('dbPollMaxInterval', { dataSource: 'postgres' })).toEqual(5);
    expect(() => getEnv('dbPollMaxInterval', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_POLL_MAX_INTERVAL;
    delete process.env.CUBEJS_DS_POSTGRES_DB_POLL_MAX_INTERVAL;
    delete process.env.CUBEJS_DS_WRONG_DB_POLL_MAX_INTERVAL;
    expect(getEnv('dbPollMaxInterval', { dataSource: 'default' })).toEqual(5);
    expect(getEnv('dbPollMaxInterval', { dataSource: 'postgres' })).toEqual(5);
    expect(() => getEnv('dbPollMaxInterval', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbPollTimeout")', () => {
    process.env.CUBEJS_DB_POLL_TIMEOUT = '5';
    process.env.CUBEJS_DS_POSTGRES_DB_POLL_TIMEOUT = '4';
    process.env.CUBEJS_DS_WRONG_DB_POLL_TIMEOUT = '3';
    expect(getEnv('dbPollTimeout', { dataSource: 'default' })).toEqual(5);
    expect(getEnv('dbPollTimeout', { dataSource: 'postgres' })).toEqual(4);
    expect(() => getEnv('dbPollTimeout', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_POLL_TIMEOUT = '4';
    process.env.CUBEJS_DS_POSTGRES_DB_POLL_TIMEOUT = '5';
    process.env.CUBEJS_DS_WRONG_DB_POLL_TIMEOUT = '3';
    expect(getEnv('dbPollTimeout', { dataSource: 'default' })).toEqual(4);
    expect(getEnv('dbPollTimeout', { dataSource: 'postgres' })).toEqual(5);
    expect(() => getEnv('dbPollTimeout', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_POLL_TIMEOUT;
    delete process.env.CUBEJS_DS_POSTGRES_DB_POLL_TIMEOUT;
    delete process.env.CUBEJS_DS_WRONG_DB_POLL_TIMEOUT;
    expect(getEnv('dbPollTimeout', { dataSource: 'default' })).toEqual(null);
    expect(getEnv('dbPollTimeout', { dataSource: 'postgres' })).toEqual(null);
    expect(() => getEnv('dbPollTimeout', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbQueryTimeout")', () => {
    process.env.CUBEJS_DB_QUERY_TIMEOUT = '5';
    process.env.CUBEJS_DS_POSTGRES_DB_QUERY_TIMEOUT = '4';
    process.env.CUBEJS_DS_WRONG_DB_QUERY_TIMEOUT = '3';
    expect(getEnv('dbQueryTimeout', { dataSource: 'default' })).toEqual(5);
    expect(getEnv('dbQueryTimeout', { dataSource: 'postgres' })).toEqual(4);
    expect(() => getEnv('dbQueryTimeout', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_QUERY_TIMEOUT = '4';
    process.env.CUBEJS_DS_POSTGRES_DB_QUERY_TIMEOUT = '5';
    process.env.CUBEJS_DS_WRONG_DB_QUERY_TIMEOUT = '3';
    expect(getEnv('dbQueryTimeout', { dataSource: 'default' })).toEqual(4);
    expect(getEnv('dbQueryTimeout', { dataSource: 'postgres' })).toEqual(5);
    expect(() => getEnv('dbQueryTimeout', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_QUERY_TIMEOUT;
    delete process.env.CUBEJS_DS_POSTGRES_DB_QUERY_TIMEOUT;
    delete process.env.CUBEJS_DS_WRONG_DB_QUERY_TIMEOUT;
    expect(getEnv('dbQueryTimeout', { dataSource: 'default' })).toEqual(600);
    expect(getEnv('dbQueryTimeout', { dataSource: 'postgres' })).toEqual(600);
    expect(() => getEnv('dbQueryTimeout', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("jdbcUrl")', () => {
    process.env.CUBEJS_JDBC_URL = 'default1';
    process.env.CUBEJS_DS_POSTGRES_JDBC_URL = 'postgres1';
    process.env.CUBEJS_DS_WRONG_JDBC_URL = 'wrong1';
    expect(getEnv('jdbcUrl', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('jdbcUrl', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('jdbcUrl', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_JDBC_URL = 'default2';
    process.env.CUBEJS_DS_POSTGRES_JDBC_URL = 'postgres2';
    process.env.CUBEJS_DS_WRONG_JDBC_URL = 'wrong2';
    expect(getEnv('jdbcUrl', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('jdbcUrl', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('jdbcUrl', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_JDBC_URL;
    delete process.env.CUBEJS_DS_POSTGRES_JDBC_URL;
    delete process.env.CUBEJS_DS_WRONG_JDBC_URL;
    expect(getEnv('jdbcUrl', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('jdbcUrl', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('jdbcUrl', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("jdbcDriver")', () => {
    process.env.CUBEJS_JDBC_DRIVER = 'default1';
    process.env.CUBEJS_DS_POSTGRES_JDBC_DRIVER = 'postgres1';
    process.env.CUBEJS_DS_WRONG_JDBC_DRIVER = 'wrong1';
    expect(getEnv('jdbcDriver', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('jdbcDriver', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('jdbcDriver', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_JDBC_DRIVER = 'default2';
    process.env.CUBEJS_DS_POSTGRES_JDBC_DRIVER = 'postgres2';
    process.env.CUBEJS_DS_WRONG_JDBC_DRIVER = 'wrong2';
    expect(getEnv('jdbcDriver', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('jdbcDriver', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('jdbcDriver', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_JDBC_DRIVER;
    delete process.env.CUBEJS_DS_POSTGRES_JDBC_DRIVER;
    delete process.env.CUBEJS_DS_WRONG_JDBC_DRIVER;
    expect(getEnv('jdbcDriver', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('jdbcDriver', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('jdbcDriver', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbExportBucketCsvEscapeSymbol")', () => {
    process.env.CUBEJS_DB_EXPORT_BUCKET_CSV_ESCAPE_SYMBOL = '"';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_CSV_ESCAPE_SYMBOL = '\'';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_CSV_ESCAPE_SYMBOL = 'wrong1';
    expect(getEnv('dbExportBucketCsvEscapeSymbol', { dataSource: 'default' })).toEqual('"');
    expect(getEnv('dbExportBucketCsvEscapeSymbol', { dataSource: 'postgres' })).toEqual('\'');
    expect(() => getEnv('dbExportBucketCsvEscapeSymbol', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_EXPORT_BUCKET_CSV_ESCAPE_SYMBOL = '\'';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_CSV_ESCAPE_SYMBOL = '"';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_CSV_ESCAPE_SYMBOL = 'wrong2';
    expect(getEnv('dbExportBucketCsvEscapeSymbol', { dataSource: 'default' })).toEqual('\'');
    expect(getEnv('dbExportBucketCsvEscapeSymbol', { dataSource: 'postgres' })).toEqual('"');
    expect(() => getEnv('dbExportBucketCsvEscapeSymbol', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_EXPORT_BUCKET_CSV_ESCAPE_SYMBOL;
    delete process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_CSV_ESCAPE_SYMBOL;
    delete process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_CSV_ESCAPE_SYMBOL;
    expect(getEnv('dbExportBucketCsvEscapeSymbol', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbExportBucketCsvEscapeSymbol', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('dbExportBucketCsvEscapeSymbol', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbExportBucketType")', () => {
    process.env.CUBEJS_DB_EXPORT_BUCKET_TYPE = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_TYPE = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_TYPE = 'wrong1';
    expect(getEnv('dbExportBucketType', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbExportBucketType', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('dbExportBucketType', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
    expect(getEnv('dbExportBucketType', {
      dataSource: 'default',
      supported: ['default1'],
    })).toEqual('default1');
    expect(getEnv('dbExportBucketType', {
      dataSource: 'postgres',
      supported: ['postgres1'],
    })).toEqual('postgres1');
    expect(() => getEnv('dbExportBucketType', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
    expect(() => getEnv('dbExportBucketType', {
      dataSource: 'default',
      supported: [],
    })).toThrow('The CUBEJS_DB_EXPORT_BUCKET_TYPE must be one of the [].');
    expect(() => getEnv('dbExportBucketType', {
      dataSource: 'postgres',
      supported: [],
    })).toThrow('The CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_TYPE must be one of the [].');
    expect(() => getEnv('dbExportBucketType', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_EXPORT_BUCKET_TYPE = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_TYPE = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_TYPE = 'wrong2';
    expect(getEnv('dbExportBucketType', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbExportBucketType', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('dbExportBucketType', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
    expect(getEnv('dbExportBucketType', {
      dataSource: 'default',
      supported: ['default2'],
    })).toEqual('default2');
    expect(getEnv('dbExportBucketType', {
      dataSource: 'postgres',
      supported: ['postgres2'],
    })).toEqual('postgres2');
    expect(() => getEnv('dbExportBucketType', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
    expect(() => getEnv('dbExportBucketType', {
      dataSource: 'default',
      supported: [],
    })).toThrow('The CUBEJS_DB_EXPORT_BUCKET_TYPE must be one of the [].');
    expect(() => getEnv('dbExportBucketType', {
      dataSource: 'postgres',
      supported: [],
    })).toThrow('The CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_TYPE must be one of the [].');
    expect(() => getEnv('dbExportBucketType', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_EXPORT_BUCKET_TYPE;
    delete process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_TYPE;
    delete process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_TYPE;
    expect(getEnv('dbExportBucketType', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbExportBucketType', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('dbExportBucketType', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
    expect(getEnv('dbExportBucketType', {
      dataSource: 'default',
      supported: [],
    })).toBeUndefined();
    expect(getEnv('dbExportBucketType', {
      dataSource: 'postgres',
      supported: [],
    })).toBeUndefined();
    expect(() => getEnv('dbExportBucketType', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbExportBucket")', () => {
    process.env.CUBEJS_DB_EXPORT_BUCKET = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET = 'wrong1';
    expect(getEnv('dbExportBucket', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbExportBucket', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('dbExportBucket', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_EXPORT_BUCKET = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET = 'wrong2';
    expect(getEnv('dbExportBucket', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbExportBucket', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('dbExportBucket', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_EXPORT_BUCKET;
    delete process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET;
    delete process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET;
    expect(getEnv('dbExportBucket', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbExportBucket', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('dbExportBucket', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbExportBucketMountDir")', () => {
    process.env.CUBEJS_DB_EXPORT_BUCKET_MOUNT_DIR = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_MOUNT_DIR = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_MOUNT_DIR = 'wrong1';
    expect(getEnv('dbExportBucketMountDir', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbExportBucketMountDir', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('dbExportBucketMountDir', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_EXPORT_BUCKET_MOUNT_DIR = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_MOUNT_DIR = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_MOUNT_DIR = 'wrong2';
    expect(getEnv('dbExportBucketMountDir', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbExportBucketMountDir', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('dbExportBucketMountDir', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_EXPORT_BUCKET_MOUNT_DIR;
    delete process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_MOUNT_DIR;
    delete process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_MOUNT_DIR;
    expect(getEnv('dbExportBucketMountDir', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbExportBucketMountDir', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('dbExportBucketMountDir', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbExportBucketAwsKey")', () => {
    process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_KEY = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_AWS_KEY = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_AWS_KEY = 'wrong1';
    expect(getEnv('dbExportBucketAwsKey', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbExportBucketAwsKey', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('dbExportBucketAwsKey', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_KEY = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_AWS_KEY = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_AWS_KEY = 'wrong2';
    expect(getEnv('dbExportBucketAwsKey', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbExportBucketAwsKey', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('dbExportBucketAwsKey', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_KEY;
    delete process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_AWS_KEY;
    delete process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_AWS_KEY;
    expect(getEnv('dbExportBucketAwsKey', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbExportBucketAwsKey', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('dbExportBucketAwsKey', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbExportBucketAwsSecret")', () => {
    process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_SECRET = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_AWS_SECRET = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_AWS_SECRET = 'wrong1';
    expect(getEnv('dbExportBucketAwsSecret', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbExportBucketAwsSecret', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('dbExportBucketAwsSecret', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_SECRET = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_AWS_SECRET = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_AWS_SECRET = 'wrong2';
    expect(getEnv('dbExportBucketAwsSecret', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbExportBucketAwsSecret', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('dbExportBucketAwsSecret', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_SECRET;
    delete process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_AWS_SECRET;
    delete process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_AWS_SECRET;
    expect(getEnv('dbExportBucketAwsSecret', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbExportBucketAwsSecret', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('dbExportBucketAwsSecret', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbExportBucketAwsRegion")', () => {
    process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_REGION = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_AWS_REGION = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_AWS_REGION = 'wrong1';
    expect(getEnv('dbExportBucketAwsRegion', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbExportBucketAwsRegion', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('dbExportBucketAwsRegion', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_REGION = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_AWS_REGION = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_AWS_REGION = 'wrong2';
    expect(getEnv('dbExportBucketAwsRegion', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbExportBucketAwsRegion', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('dbExportBucketAwsRegion', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_REGION;
    delete process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_AWS_REGION;
    delete process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_AWS_REGION;
    expect(getEnv('dbExportBucketAwsRegion', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbExportBucketAwsRegion', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('dbExportBucketAwsRegion', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbExportBucketAzureKey")', () => {
    process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_KEY = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_AZURE_KEY = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_AZURE_KEY = 'wrong1';
    expect(getEnv('dbExportBucketAzureKey', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbExportBucketAzureKey', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('dbExportBucketAzureKey', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_KEY = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_AZURE_KEY = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_AZURE_KEY = 'wrong2';
    expect(getEnv('dbExportBucketAzureKey', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbExportBucketAzureKey', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('dbExportBucketAzureKey', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_KEY;
    delete process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_AZURE_KEY;
    delete process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_AZURE_KEY;
    expect(getEnv('dbExportBucketAzureKey', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbExportBucketAzureKey', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('dbExportBucketAzureKey', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbExportBucketAzureTenantId")', () => {
    process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_TENANT_ID = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_AZURE_TENANT_ID = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_AZURE_TENANT_ID = 'wrong1';
    expect(getEnv('dbExportBucketAzureTenantId', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbExportBucketAzureTenantId', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('dbExportBucketAzureTenantId', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_TENANT_ID = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_AZURE_TENANT_ID = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_AZURE_TENANT_ID = 'wrong2';
    expect(getEnv('dbExportBucketAzureTenantId', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbExportBucketAzureTenantId', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('dbExportBucketAzureTenantId', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_TENANT_ID;
    delete process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_AZURE_TENANT_ID;
    delete process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_AZURE_TENANT_ID;
    expect(getEnv('dbExportBucketAzureTenantId', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbExportBucketAzureTenantId', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('dbExportBucketAzureTenantId', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbExportBucketAzureClientId")', () => {
    process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_CLIENT_ID = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_AZURE_CLIENT_ID = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_AZURE_CLIENT_ID = 'wrong1';
    expect(getEnv('dbExportBucketAzureClientId', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbExportBucketAzureClientId', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('dbExportBucketAzureClientId', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_CLIENT_ID = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_AZURE_CLIENT_ID = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_AZURE_CLIENT_ID = 'wrong2';
    expect(getEnv('dbExportBucketAzureClientId', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbExportBucketAzureClientId', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('dbExportBucketAzureClientId', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_CLIENT_ID;
    delete process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_AZURE_CLIENT_ID;
    delete process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_AZURE_CLIENT_ID;
    expect(getEnv('dbExportBucketAzureClientId', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbExportBucketAzureClientId', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('dbExportBucketAzureClientId', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbExportBucketAzureClientSecret")', () => {
    process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_CLIENT_SECRET = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_AZURE_CLIENT_SECRET = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_AZURE_CLIENT_SECRET = 'wrong1';
    expect(getEnv('dbExportBucketAzureClientSecret', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbExportBucketAzureClientSecret', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('dbExportBucketAzureClientSecret', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_CLIENT_SECRET = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_AZURE_CLIENT_SECRET = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_AZURE_CLIENT_SECRET = 'wrong2';
    expect(getEnv('dbExportBucketAzureClientSecret', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbExportBucketAzureClientSecret', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('dbExportBucketAzureClientSecret', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_CLIENT_SECRET;
    delete process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_AZURE_CLIENT_SECRET;
    delete process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_AZURE_CLIENT_SECRET;
    expect(getEnv('dbExportBucketAzureClientSecret', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbExportBucketAzureClientSecret', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('dbExportBucketAzureClientSecret', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbExportIntegration")', () => {
    process.env.CUBEJS_DB_EXPORT_INTEGRATION = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_INTEGRATION = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_INTEGRATION = 'wrong1';
    expect(getEnv('dbExportIntegration', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbExportIntegration', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('dbExportIntegration', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_EXPORT_INTEGRATION = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_INTEGRATION = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_INTEGRATION = 'wrong2';
    expect(getEnv('dbExportIntegration', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbExportIntegration', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('dbExportIntegration', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_EXPORT_INTEGRATION;
    delete process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_INTEGRATION;
    delete process.env.CUBEJS_DS_WRONG_DB_EXPORT_INTEGRATION;
    expect(getEnv('dbExportIntegration', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbExportIntegration', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('dbExportIntegration', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbExportGCSCredentials")', () => {
    process.env.CUBEJS_DB_EXPORT_GCS_CREDENTIALS = 'eyJhIjogMX0=';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_GCS_CREDENTIALS = 'eyJhIjogMn0=';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_GCS_CREDENTIALS = 'wrong1';
    expect(getEnv('dbExportGCSCredentials', { dataSource: 'default' })).toEqual({ a: 1 });
    expect(getEnv('dbExportGCSCredentials', { dataSource: 'postgres' })).toEqual({ a: 2 });
    expect(() => getEnv('dbExportGCSCredentials', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_EXPORT_GCS_CREDENTIALS = 'eyJhIjogMn0=';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_GCS_CREDENTIALS = 'eyJhIjogMX0=';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_GCS_CREDENTIALS = 'wrong2';
    expect(getEnv('dbExportGCSCredentials', { dataSource: 'default' })).toEqual({ a: 2 });
    expect(getEnv('dbExportGCSCredentials', { dataSource: 'postgres' })).toEqual({ a: 1 });
    expect(() => getEnv('dbExportGCSCredentials', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_EXPORT_GCS_CREDENTIALS;
    delete process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_GCS_CREDENTIALS;
    delete process.env.CUBEJS_DS_WRONG_DB_EXPORT_GCS_CREDENTIALS;
    expect(getEnv('dbExportGCSCredentials', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbExportGCSCredentials', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('dbExportGCSCredentials', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("databrickUrl")', () => {
    process.env.CUBEJS_DB_DATABRICKS_URL = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_DATABRICKS_URL = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_DATABRICKS_URL = 'wrong1';
    expect(getEnv('databrickUrl', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('databrickUrl', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('databrickUrl', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_DATABRICKS_URL = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_DATABRICKS_URL = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_DATABRICKS_URL = 'wrong2';
    expect(getEnv('databrickUrl', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('databrickUrl', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('databrickUrl', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_DATABRICKS_URL;
    delete process.env.CUBEJS_DS_POSTGRES_DB_DATABRICKS_URL;
    delete process.env.CUBEJS_DS_WRONG_DB_DATABRICKS_URL;
    expect(() => getEnv('databrickUrl', { dataSource: 'default' })).toThrow(
      'The CUBEJS_DB_DATABRICKS_URL is required and missing.'
    );
    expect(() => getEnv('databrickUrl', { dataSource: 'postgres' })).toThrow(
      'The CUBEJS_DS_POSTGRES_DB_DATABRICKS_URL is required and missing.'
    );
    expect(() => getEnv('databrickUrl', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("databrickToken")', () => {
    process.env.CUBEJS_DB_DATABRICKS_TOKEN = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_DATABRICKS_TOKEN = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_DATABRICKS_TOKEN = 'wrong1';
    expect(getEnv('databrickToken', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('databrickToken', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('databrickToken', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_DATABRICKS_TOKEN = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_DATABRICKS_TOKEN = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_DATABRICKS_TOKEN = 'wrong2';
    expect(getEnv('databrickToken', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('databrickToken', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('databrickToken', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_DATABRICKS_TOKEN;
    delete process.env.CUBEJS_DS_POSTGRES_DB_DATABRICKS_TOKEN;
    delete process.env.CUBEJS_DS_WRONG_DB_DATABRICKS_TOKEN;
    expect(getEnv('databrickToken', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('databrickToken', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('databrickToken', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("databricksCatalog")', () => {
    process.env.CUBEJS_DB_DATABRICKS_CATALOG = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_DATABRICKS_CATALOG = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_DATABRICKS_CATALOG = 'wrong1';
    expect(getEnv('databricksCatalog', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('databricksCatalog', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('databricksCatalog', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_DATABRICKS_CATALOG = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_DATABRICKS_CATALOG = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_DATABRICKS_CATALOG = 'wrong2';
    expect(getEnv('databricksCatalog', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('databricksCatalog', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('databricksCatalog', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_DATABRICKS_CATALOG;
    delete process.env.CUBEJS_DS_POSTGRES_DB_DATABRICKS_CATALOG;
    delete process.env.CUBEJS_DS_WRONG_DB_DATABRICKS_CATALOG;
    expect(getEnv('databricksCatalog', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('databricksCatalog', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('databricksCatalog', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
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
    process.env.CUBEJS_DS_POSTGRES_AWS_KEY = 'postgres1';
    process.env.CUBEJS_DS_WRONG_AWS_KEY = 'wrong1';
    expect(getEnv('athenaAwsKey', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('athenaAwsKey', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('athenaAwsKey', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_AWS_KEY = 'default2';
    process.env.CUBEJS_DS_POSTGRES_AWS_KEY = 'postgres2';
    process.env.CUBEJS_DS_WRONG_AWS_KEY = 'wrong2';
    expect(getEnv('athenaAwsKey', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('athenaAwsKey', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('athenaAwsKey', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_AWS_KEY;
    delete process.env.CUBEJS_DS_POSTGRES_AWS_KEY;
    delete process.env.CUBEJS_DS_WRONG_AWS_KEY;
    expect(getEnv('athenaAwsKey', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('athenaAwsKey', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('athenaAwsKey', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("athenaAwsSecret")', () => {
    process.env.CUBEJS_AWS_SECRET = 'default1';
    process.env.CUBEJS_DS_POSTGRES_AWS_SECRET = 'postgres1';
    process.env.CUBEJS_DS_WRONG_AWS_SECRET = 'wrong1';
    expect(getEnv('athenaAwsSecret', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('athenaAwsSecret', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('athenaAwsSecret', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_AWS_SECRET = 'default2';
    process.env.CUBEJS_DS_POSTGRES_AWS_SECRET = 'postgres2';
    process.env.CUBEJS_DS_WRONG_AWS_SECRET = 'wrong2';
    expect(getEnv('athenaAwsSecret', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('athenaAwsSecret', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('athenaAwsSecret', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_AWS_SECRET;
    delete process.env.CUBEJS_DS_POSTGRES_AWS_SECRET;
    delete process.env.CUBEJS_DS_WRONG_AWS_SECRET;
    expect(getEnv('athenaAwsSecret', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('athenaAwsSecret', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('athenaAwsSecret', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("athenaAwsRegion")', () => {
    process.env.CUBEJS_AWS_REGION = 'default1';
    process.env.CUBEJS_DS_POSTGRES_AWS_REGION = 'postgres1';
    process.env.CUBEJS_DS_WRONG_AWS_REGION = 'wrong1';
    expect(getEnv('athenaAwsRegion', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('athenaAwsRegion', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('athenaAwsRegion', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_AWS_REGION = 'default2';
    process.env.CUBEJS_DS_POSTGRES_AWS_REGION = 'postgres2';
    process.env.CUBEJS_DS_WRONG_AWS_REGION = 'wrong2';
    expect(getEnv('athenaAwsRegion', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('athenaAwsRegion', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('athenaAwsRegion', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_AWS_REGION;
    delete process.env.CUBEJS_DS_POSTGRES_AWS_REGION;
    delete process.env.CUBEJS_DS_WRONG_AWS_REGION;
    expect(getEnv('athenaAwsRegion', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('athenaAwsRegion', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('athenaAwsRegion', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("athenaAwsS3OutputLocation")', () => {
    process.env.CUBEJS_AWS_S3_OUTPUT_LOCATION = 'default1';
    process.env.CUBEJS_DS_POSTGRES_AWS_S3_OUTPUT_LOCATION = 'postgres1';
    process.env.CUBEJS_DS_WRONG_AWS_S3_OUTPUT_LOCATION = 'wrong1';
    expect(getEnv('athenaAwsS3OutputLocation', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('athenaAwsS3OutputLocation', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('athenaAwsS3OutputLocation', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_AWS_S3_OUTPUT_LOCATION = 'default2';
    process.env.CUBEJS_DS_POSTGRES_AWS_S3_OUTPUT_LOCATION = 'postgres2';
    process.env.CUBEJS_DS_WRONG_AWS_S3_OUTPUT_LOCATION = 'wrong2';
    expect(getEnv('athenaAwsS3OutputLocation', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('athenaAwsS3OutputLocation', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('athenaAwsS3OutputLocation', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_AWS_S3_OUTPUT_LOCATION;
    delete process.env.CUBEJS_DS_POSTGRES_AWS_S3_OUTPUT_LOCATION;
    delete process.env.CUBEJS_DS_WRONG_AWS_S3_OUTPUT_LOCATION;
    expect(getEnv('athenaAwsS3OutputLocation', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('athenaAwsS3OutputLocation', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('athenaAwsS3OutputLocation', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("athenaAwsWorkgroup")', () => {
    process.env.CUBEJS_AWS_ATHENA_WORKGROUP = 'default1';
    process.env.CUBEJS_DS_POSTGRES_AWS_ATHENA_WORKGROUP = 'postgres1';
    process.env.CUBEJS_DS_WRONG_AWS_ATHENA_WORKGROUP = 'wrong1';
    expect(getEnv('athenaAwsWorkgroup', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('athenaAwsWorkgroup', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('athenaAwsWorkgroup', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_AWS_ATHENA_WORKGROUP = 'default2';
    process.env.CUBEJS_DS_POSTGRES_AWS_ATHENA_WORKGROUP = 'postgres2';
    process.env.CUBEJS_DS_WRONG_AWS_ATHENA_WORKGROUP = 'wrong2';
    expect(getEnv('athenaAwsWorkgroup', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('athenaAwsWorkgroup', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('athenaAwsWorkgroup', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_AWS_ATHENA_WORKGROUP;
    delete process.env.CUBEJS_DS_POSTGRES_AWS_ATHENA_WORKGROUP;
    delete process.env.CUBEJS_DS_WRONG_AWS_ATHENA_WORKGROUP;
    expect(getEnv('athenaAwsWorkgroup', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('athenaAwsWorkgroup', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('athenaAwsWorkgroup', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("bigqueryProjectId")', () => {
    process.env.CUBEJS_DB_BQ_PROJECT_ID = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_BQ_PROJECT_ID = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_BQ_PROJECT_ID = 'wrong1';
    expect(getEnv('bigqueryProjectId', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('bigqueryProjectId', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('bigqueryProjectId', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_BQ_PROJECT_ID = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_BQ_PROJECT_ID = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_BQ_PROJECT_ID = 'wrong2';
    expect(getEnv('bigqueryProjectId', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('bigqueryProjectId', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('bigqueryProjectId', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_BQ_PROJECT_ID;
    delete process.env.CUBEJS_DS_POSTGRES_DB_BQ_PROJECT_ID;
    delete process.env.CUBEJS_DS_WRONG_DB_BQ_PROJECT_ID;
    expect(getEnv('bigqueryProjectId', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('bigqueryProjectId', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('bigqueryProjectId', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("bigqueryKeyFile")', () => {
    process.env.CUBEJS_DB_BQ_KEY_FILE = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_BQ_KEY_FILE = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_BQ_KEY_FILE = 'wrong1';
    expect(getEnv('bigqueryKeyFile', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('bigqueryKeyFile', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('bigqueryKeyFile', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_BQ_KEY_FILE = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_BQ_KEY_FILE = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_BQ_KEY_FILE = 'wrong2';
    expect(getEnv('bigqueryKeyFile', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('bigqueryKeyFile', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('bigqueryKeyFile', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_BQ_KEY_FILE;
    delete process.env.CUBEJS_DS_POSTGRES_DB_BQ_KEY_FILE;
    delete process.env.CUBEJS_DS_WRONG_DB_BQ_KEY_FILE;
    expect(getEnv('bigqueryKeyFile', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('bigqueryKeyFile', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('bigqueryKeyFile', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("bigqueryCredentials")', () => {
    process.env.CUBEJS_DB_BQ_CREDENTIALS = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_BQ_CREDENTIALS = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_BQ_CREDENTIALS = 'wrong1';
    expect(getEnv('bigqueryCredentials', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('bigqueryCredentials', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('bigqueryCredentials', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_BQ_CREDENTIALS = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_BQ_CREDENTIALS = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_BQ_CREDENTIALS = 'wrong2';
    expect(getEnv('bigqueryCredentials', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('bigqueryCredentials', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('bigqueryCredentials', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_BQ_CREDENTIALS;
    delete process.env.CUBEJS_DS_POSTGRES_DB_BQ_CREDENTIALS;
    delete process.env.CUBEJS_DS_WRONG_DB_BQ_CREDENTIALS;
    expect(getEnv('bigqueryCredentials', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('bigqueryCredentials', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('bigqueryCredentials', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("bigqueryLocation")', () => {
    process.env.CUBEJS_DB_BQ_LOCATION = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_BQ_LOCATION = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_BQ_LOCATION = 'wrong1';
    expect(getEnv('bigqueryLocation', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('bigqueryLocation', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('bigqueryLocation', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_BQ_LOCATION = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_BQ_LOCATION = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_BQ_LOCATION = 'wrong2';
    expect(getEnv('bigqueryLocation', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('bigqueryLocation', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('bigqueryLocation', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_BQ_LOCATION;
    delete process.env.CUBEJS_DS_POSTGRES_DB_BQ_LOCATION;
    delete process.env.CUBEJS_DS_WRONG_DB_BQ_LOCATION;
    expect(getEnv('bigqueryLocation', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('bigqueryLocation', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('bigqueryLocation', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("bigqueryExportBucket")', () => {
    process.env.CUBEJS_DB_BQ_EXPORT_BUCKET = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_BQ_EXPORT_BUCKET = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_BQ_EXPORT_BUCKET = 'wrong1';
    expect(getEnv('bigqueryExportBucket', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('bigqueryExportBucket', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('bigqueryExportBucket', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_BQ_EXPORT_BUCKET = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_BQ_EXPORT_BUCKET = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_BQ_EXPORT_BUCKET = 'wrong2';
    expect(getEnv('bigqueryExportBucket', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('bigqueryExportBucket', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('bigqueryExportBucket', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_BQ_EXPORT_BUCKET;
    delete process.env.CUBEJS_DS_POSTGRES_DB_BQ_EXPORT_BUCKET;
    delete process.env.CUBEJS_DS_WRONG_DB_BQ_EXPORT_BUCKET;
    expect(getEnv('bigqueryExportBucket', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('bigqueryExportBucket', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('bigqueryExportBucket', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("clickhouseReadOnly")', () => {
    process.env.CUBEJS_DB_CLICKHOUSE_READONLY = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_CLICKHOUSE_READONLY = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_CLICKHOUSE_READONLY = 'wrong1';
    expect(getEnv('clickhouseReadOnly', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('clickhouseReadOnly', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('clickhouseReadOnly', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_CLICKHOUSE_READONLY = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_CLICKHOUSE_READONLY = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_CLICKHOUSE_READONLY = 'wrong2';
    expect(getEnv('clickhouseReadOnly', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('clickhouseReadOnly', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('clickhouseReadOnly', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_CLICKHOUSE_READONLY;
    delete process.env.CUBEJS_DS_POSTGRES_DB_CLICKHOUSE_READONLY;
    delete process.env.CUBEJS_DS_WRONG_DB_CLICKHOUSE_READONLY;
    expect(getEnv('clickhouseReadOnly', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('clickhouseReadOnly', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('clickhouseReadOnly', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("elasticApiId")', () => {
    process.env.CUBEJS_DB_ELASTIC_APIKEY_ID = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_ELASTIC_APIKEY_ID = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_ELASTIC_APIKEY_ID = 'wrong1';
    expect(getEnv('elasticApiId', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('elasticApiId', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('elasticApiId', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_ELASTIC_APIKEY_ID = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_ELASTIC_APIKEY_ID = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_ELASTIC_APIKEY_ID = 'wrong2';
    expect(getEnv('elasticApiId', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('elasticApiId', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('elasticApiId', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_ELASTIC_APIKEY_ID;
    delete process.env.CUBEJS_DS_POSTGRES_DB_ELASTIC_APIKEY_ID;
    delete process.env.CUBEJS_DS_WRONG_DB_ELASTIC_APIKEY_ID;
    expect(getEnv('elasticApiId', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('elasticApiId', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('elasticApiId', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("elasticApiKey")', () => {
    process.env.CUBEJS_DB_ELASTIC_APIKEY_KEY = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_ELASTIC_APIKEY_KEY = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_ELASTIC_APIKEY_KEY = 'wrong1';
    expect(getEnv('elasticApiKey', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('elasticApiKey', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('elasticApiKey', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_ELASTIC_APIKEY_KEY = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_ELASTIC_APIKEY_KEY = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_ELASTIC_APIKEY_KEY = 'wrong2';
    expect(getEnv('elasticApiKey', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('elasticApiKey', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('elasticApiKey', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_ELASTIC_APIKEY_KEY;
    delete process.env.CUBEJS_DS_POSTGRES_DB_ELASTIC_APIKEY_KEY;
    delete process.env.CUBEJS_DS_WRONG_DB_ELASTIC_APIKEY_KEY;
    expect(getEnv('elasticApiKey', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('elasticApiKey', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('elasticApiKey', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("elasticOpenDistro")', () => {
    process.env.CUBEJS_DB_ELASTIC_OPENDISTRO = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_ELASTIC_OPENDISTRO = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_ELASTIC_OPENDISTRO = 'wrong1';
    expect(getEnv('elasticOpenDistro', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('elasticOpenDistro', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('elasticOpenDistro', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_ELASTIC_OPENDISTRO = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_ELASTIC_OPENDISTRO = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_ELASTIC_OPENDISTRO = 'wrong2';
    expect(getEnv('elasticOpenDistro', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('elasticOpenDistro', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('elasticOpenDistro', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_ELASTIC_OPENDISTRO;
    delete process.env.CUBEJS_DS_POSTGRES_DB_ELASTIC_OPENDISTRO;
    delete process.env.CUBEJS_DS_WRONG_DB_ELASTIC_OPENDISTRO;
    expect(getEnv('elasticOpenDistro', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('elasticOpenDistro', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('elasticOpenDistro', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("elasticQueryFormat")', () => {
    process.env.CUBEJS_DB_ELASTIC_QUERY_FORMAT = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_ELASTIC_QUERY_FORMAT = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_ELASTIC_QUERY_FORMAT = 'wrong1';
    expect(getEnv('elasticQueryFormat', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('elasticQueryFormat', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('elasticQueryFormat', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_ELASTIC_QUERY_FORMAT = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_ELASTIC_QUERY_FORMAT = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_ELASTIC_QUERY_FORMAT = 'wrong2';
    expect(getEnv('elasticQueryFormat', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('elasticQueryFormat', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('elasticQueryFormat', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_ELASTIC_QUERY_FORMAT;
    delete process.env.CUBEJS_DS_POSTGRES_DB_ELASTIC_QUERY_FORMAT;
    delete process.env.CUBEJS_DS_WRONG_DB_ELASTIC_QUERY_FORMAT;
    expect(getEnv('elasticQueryFormat', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('elasticQueryFormat', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('elasticQueryFormat', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("fireboltApiEndpoint")', () => {
    process.env.CUBEJS_FIREBOLT_API_ENDPOINT = 'default1';
    process.env.CUBEJS_DS_POSTGRES_FIREBOLT_API_ENDPOINT = 'postgres1';
    process.env.CUBEJS_DS_WRONG_FIREBOLT_API_ENDPOINT = 'wrong1';
    expect(getEnv('fireboltApiEndpoint', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('fireboltApiEndpoint', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('fireboltApiEndpoint', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_FIREBOLT_API_ENDPOINT = 'default2';
    process.env.CUBEJS_DS_POSTGRES_FIREBOLT_API_ENDPOINT = 'postgres2';
    process.env.CUBEJS_DS_WRONG_FIREBOLT_API_ENDPOINT = 'wrong2';
    expect(getEnv('fireboltApiEndpoint', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('fireboltApiEndpoint', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('fireboltApiEndpoint', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_FIREBOLT_API_ENDPOINT;
    delete process.env.CUBEJS_DS_POSTGRES_FIREBOLT_API_ENDPOINT;
    delete process.env.CUBEJS_DS_WRONG_FIREBOLT_API_ENDPOINT;
    expect(getEnv('fireboltApiEndpoint', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('fireboltApiEndpoint', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('fireboltApiEndpoint', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("fireboltEngineName")', () => {
    process.env.CUBEJS_FIREBOLT_ENGINE_NAME = 'default1';
    process.env.CUBEJS_DS_POSTGRES_FIREBOLT_ENGINE_NAME = 'postgres1';
    process.env.CUBEJS_DS_WRONG_FIREBOLT_ENGINE_NAME = 'wrong1';
    expect(getEnv('fireboltEngineName', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('fireboltEngineName', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('fireboltEngineName', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_FIREBOLT_ENGINE_NAME = 'default2';
    process.env.CUBEJS_DS_POSTGRES_FIREBOLT_ENGINE_NAME = 'postgres2';
    process.env.CUBEJS_DS_WRONG_FIREBOLT_ENGINE_NAME = 'wrong2';
    expect(getEnv('fireboltEngineName', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('fireboltEngineName', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('fireboltEngineName', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_FIREBOLT_ENGINE_NAME;
    delete process.env.CUBEJS_DS_POSTGRES_FIREBOLT_ENGINE_NAME;
    delete process.env.CUBEJS_DS_WRONG_FIREBOLT_ENGINE_NAME;
    expect(getEnv('fireboltEngineName', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('fireboltEngineName', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('fireboltEngineName', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("fireboltEngineEndpoint")', () => {
    process.env.CUBEJS_FIREBOLT_ENGINE_ENDPOINT = 'default1';
    process.env.CUBEJS_DS_POSTGRES_FIREBOLT_ENGINE_ENDPOINT = 'postgres1';
    process.env.CUBEJS_DS_WRONG_FIREBOLT_ENGINE_ENDPOINT = 'wrong1';
    expect(getEnv('fireboltEngineEndpoint', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('fireboltEngineEndpoint', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('fireboltEngineEndpoint', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_FIREBOLT_ENGINE_ENDPOINT = 'default2';
    process.env.CUBEJS_DS_POSTGRES_FIREBOLT_ENGINE_ENDPOINT = 'postgres2';
    process.env.CUBEJS_DS_WRONG_FIREBOLT_ENGINE_ENDPOINT = 'wrong2';
    expect(getEnv('fireboltEngineEndpoint', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('fireboltEngineEndpoint', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('fireboltEngineEndpoint', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_FIREBOLT_ENGINE_ENDPOINT;
    delete process.env.CUBEJS_DS_POSTGRES_FIREBOLT_ENGINE_ENDPOINT;
    delete process.env.CUBEJS_DS_WRONG_FIREBOLT_ENGINE_ENDPOINT;
    expect(getEnv('fireboltEngineEndpoint', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('fireboltEngineEndpoint', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('fireboltEngineEndpoint', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("hiveType")', () => {
    process.env.CUBEJS_DB_HIVE_TYPE = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_HIVE_TYPE = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_HIVE_TYPE = 'wrong1';
    expect(getEnv('hiveType', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('hiveType', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('hiveType', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_HIVE_TYPE = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_HIVE_TYPE = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_HIVE_TYPE = 'wrong2';
    expect(getEnv('hiveType', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('hiveType', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('hiveType', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_HIVE_TYPE;
    delete process.env.CUBEJS_DS_POSTGRES_DB_HIVE_TYPE;
    delete process.env.CUBEJS_DS_WRONG_DB_HIVE_TYPE;
    expect(getEnv('hiveType', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('hiveType', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('hiveType', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("hiveVer")', () => {
    process.env.CUBEJS_DB_HIVE_VER = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_HIVE_VER = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_HIVE_VER = 'wrong1';
    expect(getEnv('hiveVer', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('hiveVer', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('hiveVer', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_HIVE_VER = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_HIVE_VER = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_HIVE_VER = 'wrong2';
    expect(getEnv('hiveVer', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('hiveVer', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('hiveVer', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_HIVE_VER;
    delete process.env.CUBEJS_DS_POSTGRES_DB_HIVE_VER;
    delete process.env.CUBEJS_DS_WRONG_DB_HIVE_VER;
    expect(getEnv('hiveVer', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('hiveVer', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('hiveVer', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("hiveThriftVer")', () => {
    process.env.CUBEJS_DB_HIVE_THRIFT_VER = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_HIVE_THRIFT_VER = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_HIVE_THRIFT_VER = 'wrong1';
    expect(getEnv('hiveThriftVer', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('hiveThriftVer', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('hiveThriftVer', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_HIVE_THRIFT_VER = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_HIVE_THRIFT_VER = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_HIVE_THRIFT_VER = 'wrong2';
    expect(getEnv('hiveThriftVer', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('hiveThriftVer', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('hiveThriftVer', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_HIVE_THRIFT_VER;
    delete process.env.CUBEJS_DS_POSTGRES_DB_HIVE_THRIFT_VER;
    delete process.env.CUBEJS_DS_WRONG_DB_HIVE_THRIFT_VER;
    expect(getEnv('hiveThriftVer', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('hiveThriftVer', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('hiveThriftVer', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("hiveCdhVer")', () => {
    process.env.CUBEJS_DB_HIVE_CDH_VER = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_HIVE_CDH_VER = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_HIVE_CDH_VER = 'wrong1';
    expect(getEnv('hiveCdhVer', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('hiveCdhVer', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('hiveCdhVer', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_HIVE_CDH_VER = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_HIVE_CDH_VER = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_HIVE_CDH_VER = 'wrong2';
    expect(getEnv('hiveCdhVer', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('hiveCdhVer', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('hiveCdhVer', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_HIVE_CDH_VER;
    delete process.env.CUBEJS_DS_POSTGRES_DB_HIVE_CDH_VER;
    delete process.env.CUBEJS_DS_WRONG_DB_HIVE_CDH_VER;
    expect(getEnv('hiveCdhVer', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('hiveCdhVer', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('hiveCdhVer', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("auroraSecretArn")', () => {
    process.env.CUBEJS_DATABASE_SECRET_ARN = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DATABASE_SECRET_ARN = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DATABASE_SECRET_ARN = 'wrong1';
    expect(getEnv('auroraSecretArn', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('auroraSecretArn', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('auroraSecretArn', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DATABASE_SECRET_ARN = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DATABASE_SECRET_ARN = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DATABASE_SECRET_ARN = 'wrong2';
    expect(getEnv('auroraSecretArn', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('auroraSecretArn', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('auroraSecretArn', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DATABASE_SECRET_ARN;
    delete process.env.CUBEJS_DS_POSTGRES_DATABASE_SECRET_ARN;
    delete process.env.CUBEJS_DS_WRONG_DATABASE_SECRET_ARN;
    expect(getEnv('auroraSecretArn', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('auroraSecretArn', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('auroraSecretArn', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("auroraClusterArn")', () => {
    process.env.CUBEJS_DATABASE_CLUSTER_ARN = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DATABASE_CLUSTER_ARN = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DATABASE_CLUSTER_ARN = 'wrong1';
    expect(getEnv('auroraClusterArn', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('auroraClusterArn', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('auroraClusterArn', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DATABASE_CLUSTER_ARN = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DATABASE_CLUSTER_ARN = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DATABASE_CLUSTER_ARN = 'wrong2';
    expect(getEnv('auroraClusterArn', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('auroraClusterArn', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('auroraClusterArn', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DATABASE_CLUSTER_ARN;
    delete process.env.CUBEJS_DS_POSTGRES_DATABASE_CLUSTER_ARN;
    delete process.env.CUBEJS_DS_WRONG_DATABASE_CLUSTER_ARN;
    expect(getEnv('auroraClusterArn', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('auroraClusterArn', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('auroraClusterArn', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("redshiftUnloadArn")', () => {
    process.env.CUBEJS_DB_EXPORT_BUCKET_REDSHIFT_ARN = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_REDSHIFT_ARN = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_REDSHIFT_ARN = 'wrong1';
    expect(getEnv('redshiftUnloadArn', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('redshiftUnloadArn', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('redshiftUnloadArn', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_EXPORT_BUCKET_REDSHIFT_ARN = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_REDSHIFT_ARN = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_REDSHIFT_ARN = 'wrong2';
    expect(getEnv('redshiftUnloadArn', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('redshiftUnloadArn', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('redshiftUnloadArn', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_EXPORT_BUCKET_REDSHIFT_ARN;
    delete process.env.CUBEJS_DS_POSTGRES_DB_EXPORT_BUCKET_REDSHIFT_ARN;
    delete process.env.CUBEJS_DS_WRONG_DB_EXPORT_BUCKET_REDSHIFT_ARN;
    expect(getEnv('redshiftUnloadArn', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('redshiftUnloadArn', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('redshiftUnloadArn', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("snowflakeAccount")', () => {
    process.env.CUBEJS_DB_SNOWFLAKE_ACCOUNT = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_ACCOUNT = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_ACCOUNT = 'wrong1';
    expect(getEnv('snowflakeAccount', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('snowflakeAccount', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('snowflakeAccount', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_SNOWFLAKE_ACCOUNT = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_ACCOUNT = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_ACCOUNT = 'wrong2';
    expect(getEnv('snowflakeAccount', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('snowflakeAccount', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('snowflakeAccount', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_SNOWFLAKE_ACCOUNT;
    delete process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_ACCOUNT;
    delete process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_ACCOUNT;
    expect(getEnv('snowflakeAccount', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('snowflakeAccount', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('snowflakeAccount', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("snowflakeRegion")', () => {
    process.env.CUBEJS_DB_SNOWFLAKE_REGION = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_REGION = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_REGION = 'wrong1';
    expect(getEnv('snowflakeRegion', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('snowflakeRegion', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('snowflakeRegion', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_SNOWFLAKE_REGION = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_REGION = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_REGION = 'wrong2';
    expect(getEnv('snowflakeRegion', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('snowflakeRegion', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('snowflakeRegion', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_SNOWFLAKE_REGION;
    delete process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_REGION;
    delete process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_REGION;
    expect(getEnv('snowflakeRegion', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('snowflakeRegion', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('snowflakeRegion', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("snowflakeWarehouse")', () => {
    process.env.CUBEJS_DB_SNOWFLAKE_WAREHOUSE = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_WAREHOUSE = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_WAREHOUSE = 'wrong1';
    expect(getEnv('snowflakeWarehouse', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('snowflakeWarehouse', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('snowflakeWarehouse', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_SNOWFLAKE_WAREHOUSE = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_WAREHOUSE = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_WAREHOUSE = 'wrong2';
    expect(getEnv('snowflakeWarehouse', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('snowflakeWarehouse', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('snowflakeWarehouse', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_SNOWFLAKE_WAREHOUSE;
    delete process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_WAREHOUSE;
    delete process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_WAREHOUSE;
    expect(getEnv('snowflakeWarehouse', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('snowflakeWarehouse', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('snowflakeWarehouse', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("snowflakeRole")', () => {
    process.env.CUBEJS_DB_SNOWFLAKE_ROLE = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_ROLE = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_ROLE = 'wrong1';
    expect(getEnv('snowflakeRole', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('snowflakeRole', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('snowflakeRole', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_SNOWFLAKE_ROLE = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_ROLE = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_ROLE = 'wrong2';
    expect(getEnv('snowflakeRole', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('snowflakeRole', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('snowflakeRole', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_SNOWFLAKE_ROLE;
    delete process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_ROLE;
    delete process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_ROLE;
    expect(getEnv('snowflakeRole', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('snowflakeRole', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('snowflakeRole', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("snowflakeSessionKeepAlive")', () => {
    process.env.CUBEJS_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE = 'true';
    process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE = 'true';
    process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE = 'true';
    expect(getEnv('snowflakeSessionKeepAlive', { dataSource: 'default' })).toEqual(true);
    expect(getEnv('snowflakeSessionKeepAlive', { dataSource: 'postgres' })).toEqual(true);
    expect(() => getEnv('snowflakeSessionKeepAlive', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE = 'false';
    process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE = 'false';
    process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE = 'false';
    expect(getEnv('snowflakeSessionKeepAlive', { dataSource: 'default' })).toEqual(false);
    expect(getEnv('snowflakeSessionKeepAlive', { dataSource: 'postgres' })).toEqual(false);
    expect(() => getEnv('snowflakeSessionKeepAlive', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE = 'wrong';
    process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE = 'wrong';
    process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE = 'wrong';
    expect(() => getEnv('snowflakeSessionKeepAlive', { dataSource: 'default' })).toThrow(
      'The CUBEJS_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE must be either \'true\' or \'false\'.'
    );
    expect(() => getEnv('snowflakeSessionKeepAlive', { dataSource: 'postgres' })).toThrow(
      'The CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE must be either \'true\' or \'false\'.'
    );
    expect(() => getEnv('snowflakeSessionKeepAlive', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE;
    delete process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE;
    delete process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE;
    expect(getEnv('snowflakeSessionKeepAlive', { dataSource: 'default' })).toEqual(true);
    expect(getEnv('snowflakeSessionKeepAlive', { dataSource: 'postgres' })).toEqual(true);
    expect(() => getEnv('snowflakeSessionKeepAlive', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("snowflakeAuthenticator")', () => {
    process.env.CUBEJS_DB_SNOWFLAKE_AUTHENTICATOR = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_AUTHENTICATOR = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_AUTHENTICATOR = 'wrong1';
    expect(getEnv('snowflakeAuthenticator', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('snowflakeAuthenticator', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('snowflakeAuthenticator', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_SNOWFLAKE_AUTHENTICATOR = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_AUTHENTICATOR = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_AUTHENTICATOR = 'wrong2';
    expect(getEnv('snowflakeAuthenticator', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('snowflakeAuthenticator', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('snowflakeAuthenticator', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_SNOWFLAKE_AUTHENTICATOR;
    delete process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_AUTHENTICATOR;
    delete process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_AUTHENTICATOR;
    expect(getEnv('snowflakeAuthenticator', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('snowflakeAuthenticator', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('snowflakeAuthenticator', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("snowflakePrivateKey")', () => {
    process.env.CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_PRIVATE_KEY = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_PRIVATE_KEY = 'wrong1';
    expect(getEnv('snowflakePrivateKey', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('snowflakePrivateKey', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('snowflakePrivateKey', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_PRIVATE_KEY = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_PRIVATE_KEY = 'wrong2';
    expect(getEnv('snowflakePrivateKey', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('snowflakePrivateKey', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('snowflakePrivateKey', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY;
    delete process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_PRIVATE_KEY;
    delete process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_PRIVATE_KEY;
    expect(getEnv('snowflakePrivateKey', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('snowflakePrivateKey', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('snowflakePrivateKey', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("snowflakePrivateKeyPath")', () => {
    process.env.CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PATH = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_PRIVATE_KEY_PATH = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_PRIVATE_KEY_PATH = 'wrong1';
    expect(getEnv('snowflakePrivateKeyPath', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('snowflakePrivateKeyPath', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('snowflakePrivateKeyPath', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PATH = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_PRIVATE_KEY_PATH = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_PRIVATE_KEY_PATH = 'wrong2';
    expect(getEnv('snowflakePrivateKeyPath', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('snowflakePrivateKeyPath', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('snowflakePrivateKeyPath', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PATH;
    delete process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_PRIVATE_KEY_PATH;
    delete process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_PRIVATE_KEY_PATH;
    expect(getEnv('snowflakePrivateKeyPath', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('snowflakePrivateKeyPath', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('snowflakePrivateKeyPath', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("snowflakePrivateKeyPass")', () => {
    process.env.CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PASS = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_PRIVATE_KEY_PASS = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_PRIVATE_KEY_PASS = 'wrong1';
    expect(getEnv('snowflakePrivateKeyPass', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('snowflakePrivateKeyPass', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('snowflakePrivateKeyPass', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PASS = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_PRIVATE_KEY_PASS = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_PRIVATE_KEY_PASS = 'wrong2';
    expect(getEnv('snowflakePrivateKeyPass', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('snowflakePrivateKeyPass', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('snowflakePrivateKeyPass', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PASS;
    delete process.env.CUBEJS_DS_POSTGRES_DB_SNOWFLAKE_PRIVATE_KEY_PASS;
    delete process.env.CUBEJS_DS_WRONG_DB_SNOWFLAKE_PRIVATE_KEY_PASS;
    expect(getEnv('snowflakePrivateKeyPass', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('snowflakePrivateKeyPass', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('snowflakePrivateKeyPass', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("dbCatalog")', () => {
    process.env.CUBEJS_DB_CATALOG = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_CATALOG = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_CATALOG = 'wrong1';
    expect(getEnv('dbCatalog', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('dbCatalog', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('dbCatalog', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_CATALOG = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_CATALOG = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_CATALOG = 'wrong2';
    expect(getEnv('dbCatalog', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('dbCatalog', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('dbCatalog', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_CATALOG;
    delete process.env.CUBEJS_DS_POSTGRES_DB_CATALOG;
    delete process.env.CUBEJS_DS_WRONG_DB_CATALOG;
    expect(getEnv('dbCatalog', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('dbCatalog', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('dbCatalog', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });

  test('getEnv("prestoCatalog")', () => {
    process.env.CUBEJS_DB_PRESTO_CATALOG = 'default1';
    process.env.CUBEJS_DS_POSTGRES_DB_PRESTO_CATALOG = 'postgres1';
    process.env.CUBEJS_DS_WRONG_DB_PRESTO_CATALOG = 'wrong1';
    expect(getEnv('prestoCatalog', { dataSource: 'default' })).toEqual('default1');
    expect(getEnv('prestoCatalog', { dataSource: 'postgres' })).toEqual('postgres1');
    expect(() => getEnv('prestoCatalog', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    process.env.CUBEJS_DB_PRESTO_CATALOG = 'default2';
    process.env.CUBEJS_DS_POSTGRES_DB_PRESTO_CATALOG = 'postgres2';
    process.env.CUBEJS_DS_WRONG_DB_PRESTO_CATALOG = 'wrong2';
    expect(getEnv('prestoCatalog', { dataSource: 'default' })).toEqual('default2');
    expect(getEnv('prestoCatalog', { dataSource: 'postgres' })).toEqual('postgres2');
    expect(() => getEnv('prestoCatalog', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );

    delete process.env.CUBEJS_DB_PRESTO_CATALOG;
    delete process.env.CUBEJS_DS_POSTGRES_DB_PRESTO_CATALOG;
    delete process.env.CUBEJS_DS_WRONG_DB_PRESTO_CATALOG;
    expect(getEnv('prestoCatalog', { dataSource: 'default' })).toBeUndefined();
    expect(getEnv('prestoCatalog', { dataSource: 'postgres' })).toBeUndefined();
    expect(() => getEnv('prestoCatalog', { dataSource: 'wrong' })).toThrow(
      'The wrong data source is missing in the declared CUBEJS_DATASOURCES.'
    );
  });
});
