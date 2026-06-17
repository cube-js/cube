import { ClickHouseDriver } from '../src';

// These tests exercise how TLS/SSL options are resolved into the shape that
// `@clickhouse/client` expects. They do not require a running ClickHouse
// instance: the client only establishes a connection lazily, so we can inspect
// the resolved driver config after construction.
describe('ClickHouseDriver TLS options', () => {
  const ca = '-----BEGIN CERTIFICATE-----\nca\n-----END CERTIFICATE-----';
  const cert = '-----BEGIN CERTIFICATE-----\nclient\n-----END CERTIFICATE-----';
  const key = '-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----';

  const SSL_ENV_KEYS = [
    'CUBEJS_DB_SSL',
    'CUBEJS_DB_SSL_CA',
    'CUBEJS_DB_SSL_CERT',
    'CUBEJS_DB_SSL_KEY',
    'CUBEJS_DB_SSL_REJECT_UNAUTHORIZED',
  ];

  const tlsOf = async (config: any) => {
    const driver = new ClickHouseDriver({ host: 'localhost', port: '8123', ...config });
    try {
      return {
        tls: (driver as any).config.tls,
        url: (driver as any).config.url,
      };
    } finally {
      await driver.release();
    }
  };

  beforeEach(() => {
    SSL_ENV_KEYS.forEach((k) => { delete process.env[k]; });
  });

  afterEach(() => {
    SSL_ENV_KEYS.forEach((k) => { delete process.env[k]; });
  });

  it('does not configure TLS and uses http when no SSL is provided', async () => {
    const { tls, url } = await tlsOf({});
    expect(tls).toBeUndefined();
    expect(url).toMatch(/^http:\/\//);
  });

  it('enables basic TLS (ca only) over https', async () => {
    const { tls, url } = await tlsOf({ ssl: { ca } });
    expect(tls).toEqual({ ca_cert: Buffer.from(ca) });
    expect(url).toMatch(/^https:\/\//);
  });

  it('enables mutual TLS when ca, cert and key are provided', async () => {
    const { tls, url } = await tlsOf({ ssl: { ca, cert, key } });
    expect(tls).toEqual({
      ca_cert: Buffer.from(ca),
      cert: Buffer.from(cert),
      key: Buffer.from(key),
    });
    expect(url).toMatch(/^https:\/\//);
  });

  it('preserves Buffer values as-is', async () => {
    const caBuf = Buffer.from(ca);
    const { tls } = await tlsOf({ ssl: { ca: caBuf } });
    expect((tls as any).ca_cert).toBe(caBuf);
  });

  it('falls back to basic TLS when only cert/key (no ca) are provided', async () => {
    // @clickhouse/client requires a CA certificate to build a `tls` object,
    // so cert/key without a ca cannot enable mutual TLS.
    const { tls } = await tlsOf({ ssl: { cert, key } });
    expect(tls).toBeUndefined();
  });

  it('reads TLS material from CUBEJS_DB_SSL_* environment variables', async () => {
    process.env.CUBEJS_DB_SSL = 'true';
    process.env.CUBEJS_DB_SSL_CA = ca;
    process.env.CUBEJS_DB_SSL_CERT = cert;
    process.env.CUBEJS_DB_SSL_KEY = key;

    const { tls, url } = await tlsOf({});
    expect(tls).toEqual({
      ca_cert: Buffer.from(ca),
      cert: Buffer.from(cert),
      key: Buffer.from(key),
    });
    expect(url).toMatch(/^https:\/\//);
  });

  it('prefers explicit ssl config over environment variables', async () => {
    process.env.CUBEJS_DB_SSL = 'true';
    process.env.CUBEJS_DB_SSL_CA = ca;

    const otherCa = '-----BEGIN CERTIFICATE-----\nother\n-----END CERTIFICATE-----';
    const { tls } = await tlsOf({ ssl: { ca: otherCa } });
    expect(tls).toEqual({ ca_cert: Buffer.from(otherCa) });
  });
});
