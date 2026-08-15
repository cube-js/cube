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

function mockResponse(body: object, status = 200): any {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: status === 200 ? 'OK' : 'Error',
    json: async () => body,
    text: async () => JSON.stringify(body),
  };
}

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

    await driver.release();
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

    mockFetch.mockResolvedValueOnce(
      mockResponse({ columns: [{ name: 'x', type: 'integer' }], data: [[1]], stats: { state: 'FINISHED' } }),
    );

    await driver.testConnection();

    expect(mockFetch).toHaveBeenCalledTimes(1);
    const [url, options] = mockFetch.mock.calls[0];
    expect(url).toBe('http://trino.local:8080/v1/statement');
    expect(options.method).toBe('POST');
    expect(options.body).toBe('SELECT 1');
    expect(options.headers).toMatchObject({
      'X-Trino-Source': 'cube',
      'X-Trino-Routing-Group': 'etl',
    });

    await driver.release();
  });

  it('forwards custom headers on nextUri polls, including a different host', async () => {
    const driver = new TrinoDriver({
      host: 'coordinator.local',
      port: '8080',
      catalog: 'test',
      schema: 'default',
      headers: {
        'X-Custom-Header': 'custom-value',
        'Proxy-Authorization': 'Basic dGVzdA==',
      },
      pollBackoff: {
        initialInterval: 1,
        incrementStep: 1,
        maxInterval: 1,
        triesBeforeIncrement: 1,
      },
    });

    jest.spyOn(driver as any, 'sleep').mockResolvedValue(undefined);

    mockFetch.mockResolvedValueOnce(
      mockResponse({
        nextUri: 'http://worker.internal:8081/v1/statement/q1/1',
        columns: [{ name: 'one', type: 'integer' }],
        stats: { state: 'QUEUED' },
      }),
    );
    mockFetch.mockResolvedValueOnce(
      mockResponse({
        columns: [{ name: 'one', type: 'integer' }],
        data: [[1]],
        stats: { state: 'FINISHED' },
      }),
    );

    const rows = await driver.query('SELECT 1', []);
    expect(rows).toEqual([{ one: 1 }]);

    expect(mockFetch).toHaveBeenCalledTimes(2);
    const [postUrl, postOpts] = mockFetch.mock.calls[0];
    const [pollUrl, pollOpts] = mockFetch.mock.calls[1];

    expect(postUrl).toBe('http://coordinator.local:8080/v1/statement');
    expect(postOpts.headers['X-Custom-Header']).toBe('custom-value');
    expect(postOpts.headers['Proxy-Authorization']).toBe('Basic dGVzdA==');

    expect(pollUrl).toBe('http://worker.internal:8081/v1/statement/q1/1');
    expect(pollOpts.method).toBe('GET');
    expect(pollOpts.headers['X-Custom-Header']).toBe('custom-value');
    expect(pollOpts.headers['Proxy-Authorization']).toBe('Basic dGVzdA==');

    await driver.release();
  });
});
