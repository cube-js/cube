import { TrinoDriver } from '../../src/TrinoDriver';

const mockFetch: jest.Mock = jest.fn();

jest.mock('node-fetch', () => ({
  __esModule: true,
  default: (...args: any[]) => mockFetch(...args),
}));

jest.mock('@cubejs-backend/schema-compiler', () => ({
  PrestodbQuery: class { },
}));

describe('TrinoDriver headers', () => {
  beforeEach(() => {
    mockFetch.mockReset();
    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      text: async () => '',
    });
  });

  it('forwards configured custom headers on testConnection()', async () => {
    const driver = new TrinoDriver({
      host: 'trino.local',
      port: '8080',
      // See https://trino.io/docs/current/develop/client-protocol.html for
      // the upstream list of `X-Trino-*` headers accepted by the coordinator.
      headers: {
        'X-Trino-Source': 'cube',
        'X-Trino-Routing-Group': 'etl',
        'X-Trino-Client-Tags': 'user=alice@example.com',
        'X-Mozart-User-Token': 'abc.def.ghi',
      },
    });

    await driver.testConnection();

    expect(mockFetch).toHaveBeenCalledTimes(1);
    const [url, options] = mockFetch.mock.calls[0];
    expect(url).toBe('http://trino.local:8080/v1/info');
    expect(options.method).toBe('GET');
    expect(options.headers).toMatchObject({
      'X-Trino-Source': 'cube',
      'X-Trino-Routing-Group': 'etl',
      'X-Trino-Client-Tags': 'user=alice@example.com',
      'X-Mozart-User-Token': 'abc.def.ghi',
    });
  });
});
