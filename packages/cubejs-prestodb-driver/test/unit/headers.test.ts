import { EventEmitter } from 'events';

import { PrestoDriver } from '../../src/PrestoDriver';

/**
 * Regression test for custom headers being dropped on `nextUri` poll requests.
 *
 * The upstream `presto-client` only applies custom headers to the initial
 * `POST /v1/statement` request. The `nextUri` GET polls were issued with empty
 * headers, so proxy/gateway auth headers were lost after the first request and
 * the query failed with "User authentication failed".
 *
 * Rather than standing up a real Presto/Trino server, we mock the HTTP
 * transport that `presto-client` uses (`follow-redirects/http`) and let the
 * real client + driver logic run on top of it. The mock records the headers it
 * sees on every request, so we can assert the custom headers reach the
 * `nextUri` poll — not just the initial POST.
 */

type RecordedRequest = {
  protocol: string;
  method: string;
  host: string;
  port: string | number;
  path: string;
  headers: Record<string, string>;
};

const mockRecorded: RecordedRequest[] = [];

// Drives a single fake HTTP round-trip the way `presto-client` expects:
// `request(options, onResponse)` returns a writable request emitter; once it is
// `end()`ed we invoke `onResponse(res)` and stream a JSON body back.
const mockHttpRequest = jest.fn((protocol: string, opts: any, onResponse: (res: any) => void) => {
  mockRecorded.push({
    protocol,
    method: opts.method,
    host: opts.host,
    port: opts.port,
    path: opts.path,
    headers: { ...opts.headers },
  });

  const res: any = new EventEmitter();
  res.statusCode = 200;
  res.setEncoding = () => res;

  const req: any = new EventEmitter();
  req.write = () => true;
  req.destroy = () => req;
  req.end = () => {
    process.nextTick(() => {
      onResponse(res);

      const body = opts.method === 'POST'
        ? JSON.stringify({
          id: 'q1',
          infoUri: 'http://coordinator.local:8080/v1/query/q1',
          // Point the poll at a different host to also assert that the
          // upstream nextUri host-following behaviour is preserved.
          nextUri: 'http://worker.internal:8081/v1/statement/q1/1',
          stats: { state: 'QUEUED' },
        })
        : JSON.stringify({
          id: 'q1',
          infoUri: 'http://coordinator.local:8080/v1/query/q1',
          stats: { state: 'FINISHED' },
          columns: [{ name: 'one', type: 'integer' }],
          data: [[1]],
        });

      res.emit('data', body);
      res.emit('end');
    });
  };

  return req;
});

jest.mock('follow-redirects/http', () => ({
  Agent: class {},
  request: (opts: any, onResponse: any) => mockHttpRequest('http:', opts, onResponse),
}));

jest.mock('follow-redirects/https', () => ({
  Agent: class {},
  request: (opts: any, onResponse: any) => mockHttpRequest('https:', opts, onResponse),
}));

describe('PrestoDriver custom headers', () => {
  beforeEach(() => {
    mockRecorded.length = 0;
    mockHttpRequest.mockClear();
  });

  it('sends custom headers on every request, including nextUri polls', async () => {
    const driver = new PrestoDriver({
      host: 'coordinator.local',
      port: '8080',
      catalog: 'test',
      schema: 'default',
      dataSource: 'default',
      // Poll fast so the test doesn't wait on the default 800ms interval.
      checkInterval: 1,
      headers: {
        'X-Custom-Header': 'custom-value',
        'Proxy-Authorization': 'Basic dGVzdA==',
      },
    } as any);

    const rows = await driver.query('SELECT 1', []);
    expect(rows).toEqual([{ one: 1 }]);

    const post = mockRecorded.find((r) => r.method === 'POST');
    const poll = mockRecorded.find((r) => r.method === 'GET');

    expect(post).toBeDefined();
    expect(poll).toBeDefined();

    // The initial POST goes to the configured coordinator with the headers.
    expect(post!.host).toBe('coordinator.local');
    expect(post!.headers['X-Custom-Header']).toBe('custom-value');
    expect(post!.headers['Proxy-Authorization']).toBe('Basic dGVzdA==');

    // The nextUri poll must follow the nextUri host *and* carry the custom
    // headers — the header drop is what regressed.
    expect(poll!.host).toBe('worker.internal');
    expect(poll!.port).toBe('8081');
    expect(poll!.headers['X-Custom-Header']).toBe('custom-value');
    expect(poll!.headers['Proxy-Authorization']).toBe('Basic dGVzdA==');
  });
});
