import { Agent as HttpAgent } from 'http';
import { Agent as HttpsAgent } from 'https';
import { TrinoDriver } from '../../src';

const mockFetch: jest.Mock = jest.fn();

jest.mock('node-fetch', () => ({
  __esModule: true,
  default: (...args: any[]) => mockFetch(...args),
}));

jest.mock('@cubejs-backend/schema-compiler', () => ({
  PrestodbQuery: class {},
  TrinoQuery: class {},
}));

describe('TrinoDriver SSL', () => {
  beforeEach(() => {
    mockFetch.mockReset();
    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      text: async () => '',
    });
  });

  it('passes SSL options to fetch via an https agent on testConnection()', async () => {
    const ca = '-----BEGIN CERTIFICATE-----\nMIIC...\n-----END CERTIFICATE-----';
    const driver = new TrinoDriver({
      host: 'trino.local',
      port: '8443',
      ssl: { ca, rejectUnauthorized: false },
    });

    await driver.testConnection();

    expect(mockFetch).toHaveBeenCalledTimes(1);
    const [url, options] = mockFetch.mock.calls[0];
    expect(url).toBe('https://trino.local:8443/v1/info');
    expect(options.agent).toBeInstanceOf(HttpsAgent);
    expect(options.agent.options).toMatchObject({
      ca,
      rejectUnauthorized: false,
      keepAlive: true,
    });

    await driver.release();
  });

  it('uses a keep-alive http agent when SSL is disabled', async () => {
    const driver = new TrinoDriver({
      host: 'trino.local',
      port: '8080',
    });

    await driver.testConnection();

    expect(mockFetch).toHaveBeenCalledTimes(1);
    const [url, options] = mockFetch.mock.calls[0];
    expect(url).toBe('http://trino.local:8080/v1/info');
    expect(options.agent).toBeInstanceOf(HttpAgent);
    expect(options.agent.options).toMatchObject({ keepAlive: true });

    await driver.release();
  });
});
