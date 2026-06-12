import { TrinoDriver } from '../../src/TrinoDriver';

const mockFetch: jest.Mock = jest.fn();
const mockExecute: jest.Mock = jest.fn();

jest.mock('node-fetch', () => ({
  __esModule: true,
  default: (...args: any[]) => mockFetch(...args),
}));

jest.mock('@cubejs-backend/schema-compiler', () => ({
  PrestodbQuery: class { },
}));

jest.mock('presto-client', () => ({
  Client: jest.fn().mockImplementation(() => ({
    execute: (...args: any[]) => mockExecute(...args),
    nodes: jest.fn(),
  })),
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
    mockExecute.mockReset();
    // Default: synthesize a successful query result with no rows.
    mockExecute.mockImplementation((opts: any) => {
      opts.success?.();
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

  it('forwards configured custom headers when useSelectTestConnection is enabled', async () => {
    const driver = new TrinoDriver({
      host: 'trino.local',
      port: '8080',
      useSelectTestConnection: true,
      headers: {
        'X-Trino-Source': 'cube',
        'X-Trino-Routing-Group': 'etl',
      },
    });

    await driver.testConnection();

    expect(mockFetch).not.toHaveBeenCalled();
    expect(mockExecute).toHaveBeenCalledTimes(1);
    const [executeOpts] = mockExecute.mock.calls[0];
    expect(executeOpts.query).toBe('SELECT 1');
    expect(executeOpts.headers).toEqual({
      'X-Trino-Source': 'cube',
      'X-Trino-Routing-Group': 'etl',
    });
  });
});
