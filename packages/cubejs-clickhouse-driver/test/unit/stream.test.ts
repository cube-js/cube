import { Readable } from 'node:stream';
import { createClient } from '@clickhouse/client';

import { ClickHouseDriver } from '../../src';

const FORMAT = 'JSONCompactEachRowWithNamesAndTypes';

const NAMES = ['id', 'name', 'created_at'];
const TYPES = ['Int64', 'String', 'DateTime'];

type Batch = Array<Array<unknown>>;

interface SourceState {
  pulledRows: number;
  destroyed: boolean;
  stream: Readable;
}

let source: SourceState;

/**
 * Mimics @clickhouse/client: a Readable of batches, every batch an array of `Row` objects.
 * A batch given as a promise is pushed once it settles, which keeps the source parked on that
 * fetch until the test releases it.
 */
function rawSource(batches: Array<Batch | Promise<Batch>>, error?: Error): SourceState {
  const remaining = [...batches];
  const state: Partial<SourceState> = { pulledRows: 0, destroyed: false };

  const toRows = (batch: Batch) => batch.map((row) => ({
    json: () => {
      state.pulledRows! += 1;
      return row;
    },
  }));

  const stream = new Readable({
    objectMode: true,
    highWaterMark: 1,
    read() {
      const batch = remaining.shift();
      if (!batch) {
        if (error) {
          this.destroy(error);
        } else {
          this.push(null);
        }
        return;
      }

      if (batch instanceof Promise) {
        batch.then((settled) => this.push(toRows(settled)));
      } else {
        this.push(toRows(batch));
      }
    },
  });
  stream.on('close', () => { state.destroyed = true; });

  state.stream = stream;
  return state as SourceState;
}

/** `rawSource` with the names and types header rows prepended to the first batch. */
function mockSource(batches: Array<Batch>, error?: Error): SourceState {
  return rawSource(
    batches.length ? [[NAMES, TYPES, ...batches[0]], ...batches.slice(1)] : [],
    error,
  );
}

const mockQuery = jest.fn(async (_params: any) => ({
  response_headers: { 'x-clickhouse-format': FORMAT } as Record<string, string>,
  stream: () => source.stream,
}));

jest.mock('@clickhouse/client', () => ({
  ...jest.requireActual('@clickhouse/client'),
  createClient: jest.fn(() => ({
    query: mockQuery,
    close: jest.fn(),
  })),
}));

const createClientMock = createClient as unknown as jest.Mock;

function createDriver(): ClickHouseDriver {
  return new ClickHouseDriver({ host: 'localhost', port: '8123', dataSource: 'default' });
}

/** `StreamTableData` types the stream loosely and marks `release` optional; narrow it once here. */
async function openStream(options: any) {
  const res = await createDriver().stream('SELECT 1', [], options);
  return {
    ...res,
    rowStream: res.rowStream as unknown as Readable,
    release: res.release as () => Promise<void>,
  };
}

function rows(count: number): Array<Batch> {
  return [Array.from({ length: count }, (_, i) => [i, `name-${i}`, '2020-01-01 00:00:00'])];
}

/** Lets the stream machinery run without consuming anything. */
const settle = () => new Promise((resolve) => { setTimeout(resolve, 10); });

describe('ClickHouseDriver stream', () => {
  beforeEach(() => {
    createClientMock.mockClear();
    mockQuery.mockClear();
    delete process.env.CUBEJS_DB_QUERY_STREAM_HIGH_WATER_MARK;
    // A test that forgets to set up its source must fail, not reuse the previous one's drained stream
    source = undefined as unknown as SourceState;
  });

  it('sizes the row stream with the requested highWaterMark', async () => {
    source = mockSource(rows(1));
    const { rowStream, release } = await openStream({ highWaterMark: 1234 });

    expect(rowStream.readableHighWaterMark).toEqual(1234);
    expect(rowStream.readableObjectMode).toEqual(true);

    rowStream.destroy();
    await release();
  });

  it('falls back to CUBEJS_DB_QUERY_STREAM_HIGH_WATER_MARK when the option is missing', async () => {
    source = mockSource(rows(1));
    const { rowStream, release } = await openStream({});

    expect(rowStream.readableHighWaterMark).toEqual(8192);

    rowStream.destroy();
    await release();

    source = mockSource(rows(1));
    process.env.CUBEJS_DB_QUERY_STREAM_HIGH_WATER_MARK = '256';
    const withEnv = await openStream({});

    expect(withEnv.rowStream.readableHighWaterMark).toEqual(256);

    withEnv.rowStream.destroy();
    await withEnv.release();
  });

  it('reads the header rows and hydrates every data row', async () => {
    source = mockSource(rows(3));
    const { rowStream, types, release } = await openStream({ highWaterMark: 100 });

    expect(types).toEqual([
      { name: 'id', type: 'bigint' },
      { name: 'name', type: 'text' },
      { name: 'created_at', type: 'timestamp' },
    ]);

    const received: Array<unknown> = [];
    for await (const row of rowStream) {
      received.push(row);
    }

    expect(received).toEqual([
      { id: '0', name: 'name-0', created_at: '2020-01-01T00:00:00.000' },
      { id: '1', name: 'name-1', created_at: '2020-01-01T00:00:00.000' },
      { id: '2', name: 'name-2', created_at: '2020-01-01T00:00:00.000' },
    ]);

    await release();
  });

  it('reads rows spread over multiple batches', async () => {
    source = mockSource([
      [[1, 'a', '2020-01-01 00:00:00']],
      [[2, 'b', '2020-01-02 00:00:00'], [3, 'c', '2020-01-03 00:00:00']],
    ]);
    const { rowStream, release } = await openStream({ highWaterMark: 100 });

    const received: Array<any> = [];
    for await (const row of rowStream) {
      received.push(row);
    }

    expect(received.map((r) => r.id)).toEqual(['1', '2', '3']);

    await release();
  });

  it('stops reading ahead of the consumer once highWaterMark is reached', async () => {
    source = mockSource(rows(500));
    const { rowStream, release } = await openStream({ highWaterMark: 4 });

    // Nothing is consumed yet, only the two header rows should have been pulled
    await settle();
    expect(source.pulledRows).toEqual(2);

    rowStream.resume();
    rowStream.pause();
    await settle();

    // 2 header rows + at most highWaterMark + 1 buffered rows
    expect(source.pulledRows).toBeLessThanOrEqual(2 + 4 + 1);

    rowStream.destroy();
    await release();
  });

  it('reads ahead up to a large highWaterMark', async () => {
    source = mockSource(rows(500));
    const { rowStream, release } = await openStream({ highWaterMark: 8192 });

    rowStream.resume();
    rowStream.pause();
    await settle();

    expect(source.pulledRows).toEqual(2 + 500);

    rowStream.destroy();
    await release();
  });

  it('wraps an exception raised after the first block with the query id', async () => {
    source = mockSource(rows(2), new Error('Code: 241. DB::Exception: Memory limit exceeded'));
    const { rowStream, release } = await openStream({ highWaterMark: 100, requestId: 'req-9-span-1' });

    const { query_id: queryId } = mockQuery.mock.calls[0][0];

    const received: Array<unknown> = [];
    await expect((async () => {
      for await (const row of rowStream) {
        received.push(row);
      }
    })()).rejects.toThrow(`Stream query failed: Error: Code: 241. DB::Exception: Memory limit exceeded; query id: ${queryId}`);

    // The rows that did arrive before the exception are still delivered
    expect(received).toHaveLength(2);

    await release();
  });

  it('destroys the underlying result stream when the row stream is destroyed', async () => {
    source = mockSource(rows(500));
    const { rowStream, release } = await openStream({ highWaterMark: 4 });

    expect(source.destroyed).toEqual(false);

    rowStream.destroy();
    await settle();

    expect(source.destroyed).toEqual(true);

    await release();
  });

  it('stops pushing when destroyed while waiting on the source', async () => {
    let releaseBatch!: (batch: Batch) => void;
    const inFlight = new Promise<Batch>((resolve) => { releaseBatch = resolve; });
    // Header batch arrives at once; the next batch stays in flight until the test releases it
    source = rawSource([[NAMES, TYPES], inFlight]);
    const { rowStream } = await openStream({ highWaterMark: 100 });

    const received: Array<unknown> = [];
    rowStream.on('data', (row) => received.push(row));
    await settle();
    // Only the two header rows, i.e. the loop is parked on the in-flight second batch
    expect(source.pulledRows).toEqual(2);

    rowStream.destroy();
    releaseBatch([[1, 'a', '2020-01-01 00:00:00']]);
    await settle();

    expect(received).toEqual([]);
    expect(source.pulledRows).toEqual(2);
  });

  it('wraps a source error raised before the header rows and closes the client', async () => {
    source = mockSource([], new Error('Code: 60. DB::Exception: Table default.missing does not exist'));

    await expect(createDriver().stream('SELECT 1', [], { highWaterMark: 100, requestId: 'req-10-span-1' }))
      .rejects.toThrow(/^Stream query failed: Error: Code: 60\. DB::Exception: Table default\.missing does not exist; query id: req-10-/);

    // open() failed before the stream was handed out, so the dedicated client (created after the
    // constructor's shared one) is released here
    expect(createClientMock).toHaveBeenCalledTimes(2);
    // open() failed before the stream was handed out, so the dedicated client (created after the
    // constructor's shared one) is released here
    const client = createClientMock.mock.results.at(-1)!.value;
    expect(client.close).toHaveBeenCalledTimes(1);
  });

  it('fails when the stream ends before the header rows', async () => {
    source = mockSource([]);
    await expect(createDriver().stream('SELECT 1', [], { highWaterMark: 100 }))
      .rejects.toThrow('Unexpected stream end before row with names');
  });

  it('releases the result stream when the header rows are malformed', async () => {
    // A names row shorter than the types row fails validation inside open(), after the
    // stream already owns the source iterator
    source = rawSource([[['only_names'], ['Int64', 'String']]]);

    await expect(createDriver().stream('SELECT 1', [], { highWaterMark: 100 }))
      .rejects.toThrow('Unexpected names and types length mismatch');

    await settle();
    expect(source.destroyed).toEqual(true);
  });
});
