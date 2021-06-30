import { BaseDriver } from '../../src';

class BaseDriverImplementedMock extends BaseDriver {
}

describe('BaseDriver', () => {
  afterEach(() => {
    delete process.env.CUBEJS_DB_SSL;
    delete process.env.CUBEJS_DB_SSL_CA;
    delete process.env.CUBEJS_DB_SSL_CERT;
    delete process.env.CUBEJS_DB_SSL_KEY;
    delete process.env.CUBEJS_DB_SSL_CIPHERS;
    delete process.env.CUBEJS_DB_SSL_PASSPHRASE;
    delete process.env.CUBEJS_DB_SSL_SERVERNAME;
    delete process.env.CUBEJS_DB_SSL_REJECT_UNAUTHORIZED;
  });

  test('downloadQueryResults - test type detection', async () => {
    const driver = new BaseDriverImplementedMock({});

    jest.spyOn(driver, 'query').mockImplementation(async () => [{
      bigint: 21474836479,
      bigint_because_int_max: 2147483648,
      bigint_because_int_min: -2147483649,
      bigint_str_because_int_max: '2147483648',
      bigint_str_because_int_min: '-2147483649',
      int: 1,
      int_as_str: '1',
      int_as_str_zero: '0',
      int_as_str_negative: '-1',
      decimal_as_str: '1.000000000001',
      decimal_as_str_zero: '0.0000000',
      decimal_as_str_negative: '-1.000000000001',
      decimal_because_bigint_max: '9223372036854775808',
      decimal_because_bigint_min: '-9223372036854775809',
      string: 'str',
    }]);

    expect((await driver.downloadQueryResults()).types).toEqual([
      { name: 'bigint', type: 'bigint' },
      { name: 'bigint_because_int_max', type: 'bigint' },
      { name: 'bigint_because_int_min', type: 'bigint' },
      { name: 'bigint_str_because_int_max', type: 'bigint' },
      { name: 'bigint_str_because_int_min', type: 'bigint' },
      { name: 'int', type: 'int' },
      { name: 'int_as_str', type: 'int' },
      { name: 'int_as_str_zero', type: 'int' },
      { name: 'int_as_str_negative', type: 'int' },
      { name: 'decimal_as_str', type: 'decimal' },
      { name: 'decimal_as_str_zero', type: 'decimal' },
      { name: 'decimal_as_str_negative', type: 'decimal' },
      { name: 'decimal_because_bigint_max', type: 'decimal' },
      { name: 'decimal_because_bigint_min', type: 'decimal' },
      { name: 'string', type: 'string' }
    ]);
  });

  test('getSslOptions success load keys/ca from files', async () => {
    process.env.CUBEJS_DB_SSL = 'true';
    process.env.CUBEJS_DB_SSL_CA = './test/fixtures/simple.cert';
    process.env.CUBEJS_DB_SSL_CERT = './test/fixtures/simple.cert';
    process.env.CUBEJS_DB_SSL_KEY = './test/fixtures/simple.key';
    process.env.CUBEJS_DB_SSL_CIPHERS = '';
    process.env.CUBEJS_DB_SSL_PASSPHRASE = '';
    process.env.CUBEJS_DB_SSL_SERVERNAME = '';

    const driver = new BaseDriverImplementedMock({});
    expect(driver.getSslOptions()).toEqual({
      ca: '-----BEGIN CERTIFICATE-----\nHEHEHEHEH\n',
      cert: '-----BEGIN CERTIFICATE-----\nHEHEHEHEH\n',
      key: '-----BEGIN RSA PRIVATE KEY-----\nHEHEHEHEH\n',
      rejectUnauthorized: false,
    });
  });

  test('getSslOptions success load keys/ca from env', async () => {
    process.env.CUBEJS_DB_SSL = 'true';
    process.env.CUBEJS_DB_SSL_CA = '-----BEGIN CERTIFICATE-----\nHEHEHEHEH\n';
    process.env.CUBEJS_DB_SSL_CERT = '-----BEGIN CERTIFICATE-----\nHEHEHEHEH\n';
    process.env.CUBEJS_DB_SSL_KEY = '-----BEGIN RSA PRIVATE KEY-----\nHEHEHEHEH\n';
    process.env.CUBEJS_DB_SSL_CIPHERS = '';
    process.env.CUBEJS_DB_SSL_PASSPHRASE = '';
    process.env.CUBEJS_DB_SSL_SERVERNAME = '';

    const driver = new BaseDriverImplementedMock({});
    expect(driver.getSslOptions()).toEqual({
      ca: '-----BEGIN CERTIFICATE-----\nHEHEHEHEH\n',
      cert: '-----BEGIN CERTIFICATE-----\nHEHEHEHEH\n',
      key: '-----BEGIN RSA PRIVATE KEY-----\nHEHEHEHEH\n',
      rejectUnauthorized: false,
    });
  });

  test('getSslOptions reject unauthorized', async () => {
    process.env.CUBEJS_DB_SSL_REJECT_UNAUTHORIZED = 'true';

    const driver = new BaseDriverImplementedMock({});
    expect(driver.getSslOptions()).toEqual({
      rejectUnauthorized: true,
    });
  });

  test('mapSSLOptions success', async () => {
    const sslOptions = {
      ca: './test/fixtures/simple.cert',
      cert: './test/fixtures/simple.cert',
      key: './test/fixtures/simple.key',
      rejectUnauthorized: true
    };

    const driver = new BaseDriverImplementedMock({});
    expect(driver.mapSSLOptions(sslOptions)).toEqual({
      ca: '-----BEGIN CERTIFICATE-----\nHEHEHEHEH\n',
      cert: '-----BEGIN CERTIFICATE-----\nHEHEHEHEH\n',
      key: '-----BEGIN RSA PRIVATE KEY-----\nHEHEHEHEH\n',
      rejectUnauthorized: true,
    });
  });
});
