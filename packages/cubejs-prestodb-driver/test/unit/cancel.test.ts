import { EventEmitter } from 'events';

import { PrestoDriver } from '../../src/PrestoDriver';

/**
 * The query queue cancels a running query by calling `.cancel()` on the promise returned by
 * `query()` / `stream()`. Without it the query kept running on the coordinator after Cube had
 * dropped it. We mock the HTTP transport that `presto-client` uses (as in `headers.test.ts`)
 * and assert that cancelling sends the protocol's DELETE to the current `nextUri`.
 */

type RecordedRequest = {
  method: string;
  path: string;
};

const mockRecorded: RecordedRequest[] = [];

// Keeps the query RUNNING until `mockFinishAfterPolls` GETs have been served (Infinity = never),
// so the test controls whether the query is still in flight when it is cancelled.
let mockFinishAfterPolls = Infinity;

const mockBody = (method: string, path: string) => {
  if (method === 'POST') {
    return {
      id: 'q1',
      infoUri: 'http://coordinator.local:8080/v1/query/q1',
      nextUri: 'http://coordinator.local:8080/v1/statement/executing/q1/1',
      stats: { state: 'QUEUED' },
    };
  }

  const poll = Number(path.split('/').pop());
  const columns = [{ name: 'one', type: 'integer' }];

  if (poll >= mockFinishAfterPolls) {
    return { id: 'q1', infoUri: 'http://coordinator.local:8080/v1/query/q1', stats: { state: 'FINISHED' }, columns, data: [[1]] };
  }

  return {
    id: 'q1',
    infoUri: 'http://coordinator.local:8080/v1/query/q1',
    nextUri: `http://coordinator.local:8080/v1/statement/executing/q1/${poll + 1}`,
    stats: { state: 'RUNNING' },
    columns,
  };
};

const mockHttpRequest = jest.fn((opts: any, onResponse: (res: any) => void) => {
  mockRecorded.push({ method: opts.method, path: opts.path });

  const res: any = new EventEmitter();
  res.statusCode = opts.method === 'DELETE' ? 204 : 200;
  res.setEncoding = () => res;

  const req: any = new EventEmitter();
  req.write = () => true;
  req.destroy = () => req;
  req.end = () => {
    process.nextTick(() => {
      onResponse(res);
      if (opts.method !== 'DELETE') {
        res.emit('data', JSON.stringify(mockBody(opts.method, opts.path)));
      }
      res.emit('end');
    });
  };

  return req;
});

jest.mock('follow-redirects/http', () => ({
  Agent: class {},
  request: (opts: any, onResponse: any) => mockHttpRequest(opts, onResponse),
}));

jest.mock('follow-redirects/https', () => ({
  Agent: class {},
  request: (opts: any, onResponse: any) => mockHttpRequest(opts, onResponse),
}));

const waitFor = async (condition: () => boolean) => {
  for (let i = 0; i < 1000 && !condition(); i++) {
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  expect(condition()).toBe(true);
};

const polls = () => mockRecorded.filter((r) => r.method === 'GET').length;
const deletes = () => mockRecorded.filter((r) => r.method === 'DELETE');

describe('PrestoDriver query cancellation', () => {
  const createDriver = () => new PrestoDriver({
    host: 'coordinator.local',
    port: '8080',
    catalog: 'test',
    schema: 'default',
    dataSource: 'default',
    checkInterval: 1,
  } as any);

  beforeEach(() => {
    mockRecorded.length = 0;
    mockHttpRequest.mockClear();
    mockFinishAfterPolls = Infinity;
  });

  it('cancels a running query() on the coordinator', async () => {
    const promise = createDriver().query('SELECT 1', []);
    await waitFor(() => polls() > 0);

    await promise.cancel();

    await expect(promise).rejects.toThrow('query fetch canceled by operation');
    expect(deletes()).toHaveLength(1);
    expect(deletes()[0].path).toMatch(/\/v1\/statement\/executing\/q1\/\d+$/);
  });

  it('cancels a running stream() and errors its rowStream', async () => {
    const promise = createDriver().stream('SELECT 1', [], { highWaterMark: 1 });
    const { rowStream } = await promise;

    await promise.cancel();

    const drain = async () => {
      for await (const _row of rowStream) {
        // drain
      }
    };
    await expect(drain()).rejects.toThrow('query fetch canceled by operation');
    expect(deletes()).toHaveLength(1);
  });

  it('does not send a DELETE when cancel() is called after the query finished', async () => {
    mockFinishAfterPolls = 2;
    const promise = createDriver().query('SELECT 1', []);

    await expect(promise).resolves.toEqual([{ one: 1 }]);
    await promise.cancel();

    expect(deletes()).toHaveLength(0);
  });
});
