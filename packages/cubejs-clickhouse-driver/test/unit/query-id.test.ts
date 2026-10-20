import { createClient } from '@clickhouse/client';

import { ClickHouseDriver } from '../../src';

const mockQuery = jest.fn(async (_params: any) => ({
  response_headers: {} as Record<string, string>,
  json: async () => ({ data: [], meta: [] }),
}));
const mockCommand = jest.fn(async (_params: any) => ({}));
const mockInsert = jest.fn(async (_params: any) => ({}));
const mockPing = jest.fn(async () => ({ success: true }));

jest.mock('@clickhouse/client', () => ({
  ...jest.requireActual('@clickhouse/client'),
  createClient: jest.fn(() => ({
    ping: mockPing,
    query: mockQuery,
    command: mockCommand,
    insert: mockInsert,
    close: jest.fn(),
  })),
}));

const createClientMock = createClient as unknown as jest.Mock;

const UUID = '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}';
const UUID_RE = new RegExp(`^${UUID}$`);

function createDriver(): ClickHouseDriver {
  return new ClickHouseDriver({
    host: 'localhost',
    port: '8123',
    dataSource: 'default',
  });
}

describe('ClickHouseDriver query id', () => {
  beforeEach(() => {
    createClientMock.mockClear();
    mockQuery.mockClear();
    mockCommand.mockClear();
    mockInsert.mockClear();
  });

  it('prefixes query_id with the Cube request id, without the span suffix', async () => {
    await createDriver().query('SELECT 1', [], { requestId: '5c2c96a1-d0b3-4a9e-8f52-2b6b7f0b1e11-span-3' });

    expect(mockQuery.mock.calls[0][0]).toMatchObject({
      query_id: expect.stringMatching(new RegExp(`^5c2c96a1-d0b3-4a9e-8f52-2b6b7f0b1e11-${UUID}$`)),
    });
  });

  it('generates a unique query_id per statement of the same request', async () => {
    const driver = createDriver();
    const requestId = 'req-0-span-1';

    await driver.query('SELECT 1', [], { requestId });
    await driver.query('SELECT 2', [], { requestId });

    const [first, second] = mockQuery.mock.calls.map(([params]) => params.query_id);
    expect(first).not.toEqual(second);
  });

  it('keeps a request id without a span suffix as the prefix', async () => {
    await createDriver().query('SELECT 1', [], { requestId: 'my-request-id' });

    expect(mockQuery.mock.calls[0][0]).toMatchObject({
      query_id: expect.stringMatching(new RegExp(`^my-request-id-${UUID}$`)),
    });
  });

  it('clamps an over-long request id, which would overflow the response header', async () => {
    await createDriver().query('SELECT 1', [], { requestId: `${'x'.repeat(500)}-span-1` });

    const { query_id: queryId } = mockQuery.mock.calls[0][0];
    expect(queryId).toMatch(new RegExp(`^x{63}-${UUID}$`));
  });

  it('falls back to a generated uuid when the request id is all span suffix', async () => {
    await createDriver().query('SELECT 1', [], { requestId: '-span-1' });

    expect(mockQuery.mock.calls[0][0].query_id).toMatch(UUID_RE);
  });

  it('falls back to a generated uuid when there is no request id', async () => {
    await createDriver().query('SELECT 1', []);

    const { query_id: queryId } = mockQuery.mock.calls[0][0];
    expect(queryId).toMatch(UUID_RE);
  });

  it('escapes the query id in the KILL QUERY statement', async () => {
    const driver = createDriver();
    const promise = driver.query('SELECT 1', [], { requestId: 'a\'b-span-1' }) as Promise<unknown> & { cancel(): Promise<void> };
    await Promise.all([promise.cancel(), promise.catch(() => undefined)]);

    const { query_id: queryId } = mockQuery.mock.calls[0][0];
    expect(queryId).toMatch(new RegExp(`^a'b-${UUID}$`));
    expect(mockCommand.mock.calls[0][0]).toMatchObject({
      query: `KILL QUERY WHERE query_id = 'a''${queryId.slice(2)}' SYNC`,
    });
  });

  it('rejects a cancelled query only after it is killed', async () => {
    const events: string[] = [];
    mockQuery.mockImplementationOnce(({ abort_signal: signal }) => new Promise((_, reject) => {
      signal.addEventListener('abort', () => reject(new Error('aborted')));
    }));
    mockCommand.mockImplementationOnce(async () => {
      await new Promise((resolve) => setTimeout(resolve, 20));
      events.push('killed');
      return {};
    });

    const promise = createDriver().query('SELECT 1', []) as Promise<unknown> & { cancel(): Promise<void> };
    const settled = promise.catch(() => events.push('rejected'));
    await promise.cancel();
    await settled;

    expect(events).toEqual(['killed', 'rejected']);
  });

  it('settles a cancelled query when the kill does not finish in time', async () => {
    class ShortKillDriver extends ClickHouseDriver {
      protected readonly killQueryTimeout = 50;
    }
    mockQuery.mockImplementationOnce(({ abort_signal: signal }) => new Promise((_, reject) => {
      signal.addEventListener('abort', () => reject(new Error('aborted')));
    }));
    mockCommand.mockImplementationOnce(({ abort_signal: signal }) => new Promise((_, reject) => {
      signal.addEventListener('abort', () => reject(new Error('kill timed out')));
    }));

    const driver = new ShortKillDriver({ host: 'localhost', port: '8123', dataSource: 'default' });
    const promise = driver.query('SELECT 1', []) as Promise<unknown> & { cancel(): Promise<void> };

    await Promise.all([
      expect(promise.cancel()).rejects.toThrow('kill timed out'),
      expect(promise).rejects.toThrow('aborted'),
    ]);
  });

  it('does not kill a query that already finished', async () => {
    const promise = createDriver().query('SELECT 1', []) as Promise<unknown> & { cancel(): Promise<void> };
    await promise;

    await promise.cancel();
    expect(mockCommand).not.toHaveBeenCalled();
  });

  it('exposes a cancel handle on query(), downloadQueryResults() and dropTable()', async () => {
    const driver = createDriver();

    for (const promise of [
      driver.query('SELECT 1', []),
      driver.query('CREATE TABLE t AS SELECT 1', []),
      driver.downloadQueryResults('SELECT 1', [], { highWaterMark: 100 }),
      driver.dropTable('t'),
    ]) {
      expect((promise as any).cancel).toBeInstanceOf(Function);
      await promise;
    }
  });

  it('tags streaming queries', async () => {
    const driver = createDriver();

    await expect(driver.stream('SELECT 1', [], { highWaterMark: 100, requestId: 'req-1-span-1' }))
      .rejects.toThrow();

    expect(mockQuery.mock.calls[0][0]).toMatchObject({
      query_id: expect.stringMatching(new RegExp(`^req-1-${UUID}$`)),
    });
  });

  it('tags commands and inserts', async () => {
    const driver = createDriver();

    await driver.command('DROP TABLE t', { requestId: 'req-2-span-1' });
    expect(mockCommand.mock.calls[0][0]).toMatchObject({
      query_id: expect.stringMatching(new RegExp(`^req-2-${UUID}$`)),
    });

    await driver.insert('t', [[1]], { requestId: 'req-3-span-1' });
    expect(mockInsert.mock.calls[0][0]).toMatchObject({
      query_id: expect.stringMatching(new RegExp(`^req-3-${UUID}$`)),
    });
  });

  it('tags dropTable through the options it receives', async () => {
    await createDriver().dropTable('t', { requestId: 'req-4-span-1' });

    expect(mockCommand.mock.calls[0][0]).toMatchObject({
      query: 'DROP TABLE t',
      query_id: expect.stringMatching(new RegExp(`^req-4-${UUID}$`)),
    });
  });
});
