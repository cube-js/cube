import { EventEmitter } from 'events';
import { PassThrough } from 'stream';

import { PrestoDriver } from '../../src/PrestoDriver';

/**
 * Mocks the HTTP transport that `presto-client` uses (as in `headers.test.ts`) and asserts that
 * cancelling a query sends DELETE to its current `nextUri`.
 */

type RecordedRequest = {
  method: string;
  path: string;
};

const mockRecorded: RecordedRequest[] = [];

let mockFinishAfterPolls = Infinity;
let mockFailAtPoll = Infinity;

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

  if (poll >= mockFailAtPoll) {
    return { id: 'q1', infoUri: 'http://coordinator.local:8080/v1/query/q1', stats: { state: 'FAILED' }, error: { message: 'mock failure' } };
  }

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
    mockFailAtPoll = Infinity;
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

  const settle = () => new Promise((resolve) => setTimeout(resolve, 20));

  it('rejects a stream() that fails before columns, without an uncaught error', async () => {
    mockFailAtPoll = 1;

    await expect(createDriver().stream('SELECT 1', [], { highWaterMark: 1 })).rejects.toThrow('mock failure');
    await settle();
  });

  it('cancels a stream() that is only piped, without an uncaught error', async () => {
    const promise = createDriver().stream('SELECT 1', [], { highWaterMark: 1 });
    const { rowStream } = await promise;
    rowStream.pipe(new PassThrough({ objectMode: true }));
    const closed = new Promise((resolve) => rowStream.on('close', resolve));

    await promise.cancel();

    await closed;
    await settle();
    expect(deletes()).toHaveLength(1);
  });

  it('stops the query once queryColumnTypes() has the types', async () => {
    const types = await createDriver().queryColumnTypes('SELECT 1', []);

    expect(types).toEqual([{ name: 'one', type: 'int' }]);
    await waitFor(() => deletes().length === 1);
    await settle();
  });
});
