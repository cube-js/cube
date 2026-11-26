import http from 'http';
import { Readable } from 'stream';
import type { AddressInfo } from 'net';
import { streamToArray } from '@cubejs-backend/shared';
import { PinotDriver } from '../../src/PinotDriver';

type RecordedRequest = { method: string, url: string, authorization?: string, body?: any };

type BrokerOptions = {
  rows: unknown[][],
  // Brokers before 1.3 ignore the cursor parameters and return the whole result.
  cursors?: boolean,
  exception?: { message: string, errorCode: number },
  failResultsWith?: { status: number, body: string },
  failDeleteWith?: { status: number, body: string },
  // Accepts the page request and never answers it.
  hangResults?: boolean,
  // Accepts the delete request and never answers it.
  hangDelete?: boolean,
  // Rejects the `LIMIT 0` query that reads the schema of an empty result.
  failSchemaQuery?: boolean,
  // Reported as the broker that ran the query.
  owner?: { host: string, port: number },
};

const DATA_SCHEMA = { columnNames: ['id', 'name'], columnDataTypes: ['LONG', 'STRING'] };
const CURSOR_ID = '2101488704000000005';

const readBody = async (req: http.IncomingMessage): Promise<string> => {
  const chunks: Buffer[] = [];

  for await (const chunk of req) {
    chunks.push(chunk);
  }

  return Buffer.concat(chunks).toString();
};

// Replays the responses of an Apache Pinot 1.4.0 broker.
class FakeBroker {
  public readonly requests: RecordedRequest[] = [];

  protected readonly server: http.Server;

  public constructor(protected readonly options: BrokerOptions) {
    this.server = http.createServer((req, res) => {
      this.handle(req, res).catch((error) => {
        res.writeHead(500).end(String(error));
      });
    });
  }

  public async listen(): Promise<string> {
    await new Promise<void>((resolve) => {
      this.server.listen(0, '127.0.0.1', resolve);
    });

    return String((<AddressInfo> this.server.address()).port);
  }

  public async close(): Promise<void> {
    this.server.closeAllConnections();

    await new Promise((resolve) => {
      this.server.close(resolve);
    });
  }

  protected page(offset: number, numRows: number) {
    const rows = this.options.rows.slice(offset, offset + numRows);

    return {
      // An empty cursor response carries no table at all.
      ...(rows.length ? { resultTable: { dataSchema: DATA_SCHEMA, rows } } : {}),
      exceptions: [],
      requestId: CURSOR_ID,
      numRowsResultSet: this.options.rows.length,
      offset,
      numRows: rows.length,
      ...(this.options.owner ? { brokerHost: this.options.owner.host, brokerPort: this.options.owner.port } : {}),
    };
  }

  protected async handle(req: http.IncomingMessage, res: http.ServerResponse) {
    const url = new URL(req.url || '/', 'http://broker');
    const body = await readBody(req);

    this.requests.push({
      method: req.method || '',
      url: req.url || '',
      authorization: req.headers.authorization,
      body: body ? JSON.parse(body) : undefined,
    });

    const json = (payload: unknown) => {
      res.writeHead(200, { 'Content-Type': 'application/json' }).end(JSON.stringify(payload));
    };

    if (req.method === 'POST' && url.pathname === '/query/sql') {
      if (this.options.failSchemaQuery && JSON.parse(body).sql.endsWith('LIMIT 0')) {
        return json({ exceptions: [{ message: 'QueryPlanningError', errorCode: 150 }] });
      }

      if (this.options.exception) {
        return json({ exceptions: [this.options.exception], numRowsResultSet: 0, requestId: CURSOR_ID });
      }

      if (this.options.cursors !== false && url.searchParams.get('getCursor') === 'true') {
        return json(this.page(0, Number(url.searchParams.get('numRows'))));
      }

      return json({
        resultTable: { dataSchema: DATA_SCHEMA, rows: this.options.rows },
        exceptions: [],
        requestId: CURSOR_ID,
        numRowsResultSet: this.options.rows.length,
      });
    }

    if (req.method === 'GET' && url.pathname === `/responseStore/${CURSOR_ID}/results`) {
      if (this.options.hangResults) {
        return undefined;
      }

      if (this.options.failResultsWith) {
        return res.writeHead(this.options.failResultsWith.status).end(this.options.failResultsWith.body);
      }

      return json(this.page(Number(url.searchParams.get('offset')), Number(url.searchParams.get('numRows'))));
    }

    if (req.method === 'DELETE' && url.pathname === `/responseStore/${CURSOR_ID}`) {
      if (this.options.hangDelete) {
        return undefined;
      }

      if (this.options.failDeleteWith) {
        return res.writeHead(this.options.failDeleteWith.status).end(this.options.failDeleteWith.body);
      }

      return res.writeHead(200).end(`Query Results for ${CURSOR_ID} deleted.`);
    }

    return res.writeHead(404).end('Not found');
  }
}

describe('PinotDriver stream', () => {
  const ROWS = [[1, 'a'], [2, 'b'], [3, 'c'], [4, 'd'], [5, 'e']];
  const EXPECTED = ROWS.map(([id, name]) => ({ id, name }));
  const TYPES = [{ name: 'id', type: 'bigint' }, { name: 'name', type: 'text' }];

  let brokers: FakeBroker[] = [];

  const startBroker = async (options: BrokerOptions) => {
    const broker = new FakeBroker(options);
    brokers.push(broker);

    return { port: await broker.listen(), requests: broker.requests };
  };

  const start = async (options: BrokerOptions, queryTimeout = 30) => {
    const { port, requests } = await startBroker(options);

    const driver = new PinotDriver({
      host: '127.0.0.1',
      port,
      dataSource: 'default',
      basicAuth: { user: 'admin', password: 'mysecret' },
      nullHandling: false,
      queryTimeout,
    });

    return { driver, requests };
  };

  const waitForRequests = async (requests: RecordedRequest[], count: number) => {
    while (requests.length < count) {
      await new Promise((resolve) => { setTimeout(resolve, 5); });
    }
  };

  const calls = (requests: RecordedRequest[]) => requests.map(({ method, url }) => `${method} ${url}`);

  afterEach(async () => {
    await Promise.all(brokers.map((broker) => broker.close()));
    brokers = [];
  });

  it('pages through a cursor and deletes it on release', async () => {
    const { driver, requests } = await start({ rows: ROWS });

    const source = await driver.stream('SELECT id, name FROM t WHERE name <> ?', ['z'], { highWaterMark: 2 });

    expect(source.types).toEqual(TYPES);
    expect(await streamToArray(<any> source.rowStream)).toEqual(EXPECTED);

    await source.release?.();
    await source.release?.();

    expect(calls(requests)).toEqual([
      'POST /query/sql?getCursor=true&numRows=2',
      `GET /responseStore/${CURSOR_ID}/results?offset=2&numRows=2`,
      `GET /responseStore/${CURSOR_ID}/results?offset=4&numRows=2`,
      `DELETE /responseStore/${CURSOR_ID}`,
    ]);
    expect(requests[0].body.sql).toBe('SELECT id, name FROM t WHERE name <> \'z\'');
    expect(new Set(requests.map((request) => request.authorization))).toEqual(
      new Set([`Basic ${Buffer.from('admin:mysecret').toString('base64')}`])
    );
  });

  it('leaves the remaining pages on the broker when the consumer stops early', async () => {
    const { driver, requests } = await start({ rows: ROWS });

    const source = await driver.stream('SELECT id, name FROM t', [], { highWaterMark: 2 });
    const rowStream = <Readable> source.rowStream;

    const row = await new Promise((resolve) => {
      rowStream.once('data', (data) => {
        rowStream.destroy();
        resolve(data);
      });
    });
    await source.release?.();

    expect(row).toEqual(EXPECTED[0]);
    expect(calls(requests)).toEqual([
      'POST /query/sql?getCursor=true&numRows=2',
      `DELETE /responseStore/${CURSOR_ID}`,
    ]);
  });

  it('sends the request id as clientQueryId', async () => {
    const { driver, requests } = await start({ rows: ROWS });
    const requestId = '22cf5b37-2351-4eab-a501-617fcf8baac6';

    const source = await driver.stream('SELECT id, name FROM t', [], { highWaterMark: 10, requestId: `${requestId}-span-1` });
    await streamToArray(<any> source.rowStream);

    expect(requests[0].body.queryOptions).toContain(`clientQueryId=${requestId}`);
  });

  it('streams the whole response from a broker without cursors', async () => {
    const { driver, requests } = await start({ rows: ROWS, cursors: false });

    const source = await driver.stream('SELECT id, name FROM t', [], { highWaterMark: 2 });

    expect(source.types).toEqual(TYPES);
    expect(await streamToArray(<any> source.rowStream)).toEqual(EXPECTED);

    await source.release?.();

    expect(calls(requests)).toEqual(['POST /query/sql?getCursor=true&numRows=2']);
  });

  it('reads the column types of an empty result without running the query again', async () => {
    const { driver, requests } = await start({ rows: [] });

    const source = await driver.stream('SELECT id, name FROM t', [], { highWaterMark: 2 });

    expect(source.types).toEqual(TYPES);
    expect(await streamToArray(<any> source.rowStream)).toEqual([]);

    await source.release?.();

    expect(calls(requests)[0]).toBe('POST /query/sql?getCursor=true&numRows=2');
    // The schema query and the delete are sent together.
    expect(calls(requests).slice(1).sort()).toEqual([
      `DELETE /responseStore/${CURSOR_ID}`,
      'POST /query/sql',
    ]);
    expect(requests.filter(({ body }) => body).map(({ body }) => body.sql)).toEqual([
      'SELECT id, name FROM t',
      'SELECT * FROM (SELECT id, name FROM t) LIMIT 0',
    ]);
  });

  it('runs an empty query again when its schema cannot be read with LIMIT 0', async () => {
    const { driver, requests } = await start({ rows: [], failSchemaQuery: true });

    const source = await driver.stream('SELECT id, name FROM t', [], { highWaterMark: 2 });

    expect(source.types).toEqual(TYPES);
    expect(await streamToArray(<any> source.rowStream)).toEqual([]);
    expect(requests.filter(({ body }) => body).map(({ body }) => body.sql)).toEqual([
      'SELECT id, name FROM t',
      'SELECT * FROM (SELECT id, name FROM t) LIMIT 0',
      'SELECT id, name FROM t',
    ]);
  });

  it('deletes the cursor of a single-page result without waiting for the consumer', async () => {
    const { driver, requests } = await start({ rows: ROWS });

    const source = await driver.stream('SELECT id, name FROM t', [], { highWaterMark: 5 });

    await waitForRequests(requests, 2);

    expect(calls(requests)).toEqual([
      'POST /query/sql?getCursor=true&numRows=5',
      `DELETE /responseStore/${CURSOR_ID}`,
    ]);
    // The rows of the first page are already in memory.
    expect(await streamToArray(<any> source.rowStream)).toEqual(EXPECTED);

    await source.release?.();
    expect(requests).toHaveLength(2);
  });

  it('downloads query results as a stream for a stream import', async () => {
    const { driver, requests } = await start({ rows: ROWS });

    const streamed: any = await driver.downloadQueryResults('SELECT id, name FROM t', [], { highWaterMark: 2, streamImport: true });

    expect(streamed.rows).toBeUndefined();
    expect(streamed.types).toEqual(TYPES);
    expect(await streamToArray(streamed.rowStream)).toEqual(EXPECTED);
    await streamed.release();

    const buffered: any = await driver.downloadQueryResults('SELECT id, name FROM t', [], { highWaterMark: 2 });

    expect(buffered.rows).toEqual(EXPECTED);
    expect(buffered.types).toEqual(TYPES);
    expect(calls(requests).at(-1)).toBe('POST /query/sql');
  });

  it('rejects when the query fails', async () => {
    const { driver } = await start({ rows: ROWS, exception: { message: 'QueryValidationError: boom', errorCode: 700 } });

    await expect(driver.stream('SELECT nope FROM t', [], { highWaterMark: 2 }))
      .rejects.toThrow('QueryValidationError: boom');
  });

  it('fails the stream when a page cannot be fetched', async () => {
    const { driver } = await start({
      rows: ROWS,
      failResultsWith: { status: 404, body: `Query results for ${CURSOR_ID} not found.` },
    });

    const source = await driver.stream('SELECT id, name FROM t', [], { highWaterMark: 2 });

    await expect(streamToArray(<any> source.rowStream)).rejects.toThrow(
      `Unable to fetch Pinot cursor ${CURSOR_ID} at offset 2\nQuery results for ${CURSOR_ID} not found.`
    );
  });

  it('deletes the cursor when the stream is destroyed without a release', async () => {
    const { driver, requests } = await start({ rows: ROWS });

    const source = await driver.stream('SELECT id, name FROM t', [], { highWaterMark: 2 });
    const rowStream = <Readable> source.rowStream;

    await new Promise((resolve) => {
      rowStream.once('close', resolve);
      rowStream.destroy();
    });
    await waitForRequests(requests, 2);

    expect(calls(requests)).toEqual([
      'POST /query/sql?getCursor=true&numRows=2',
      `DELETE /responseStore/${CURSOR_ID}`,
    ]);
  });

  it('reports a failure without waiting for the cursor to be deleted', async () => {
    const { driver, requests } = await start({
      rows: ROWS,
      hangDelete: true,
      failResultsWith: { status: 500, body: 'Response store is down' },
    });

    const source = await driver.stream('SELECT id, name FROM t', [], { highWaterMark: 2 });
    const rowStream = <Readable> source.rowStream;
    const closed = new Promise((resolve) => {
      rowStream.once('close', resolve);
    });

    await expect(streamToArray(<any> rowStream)).rejects.toThrow('Response store is down');
    await closed;
    await waitForRequests(requests, 3);

    expect(calls(requests).at(-1)).toBe(`DELETE /responseStore/${CURSOR_ID}`);
  });

  it('deletes the cursor once the stream has been read to the end', async () => {
    const { driver, requests } = await start({ rows: ROWS });

    const source = await driver.stream('SELECT id, name FROM t', [], { highWaterMark: 2 });
    const rowStream = <Readable> source.rowStream;
    const closed = new Promise((resolve) => {
      rowStream.once('close', resolve);
    });

    expect(await streamToArray(<any> rowStream)).toEqual(EXPECTED);
    await closed;
    await waitForRequests(requests, 4);

    expect(calls(requests).at(-1)).toBe(`DELETE /responseStore/${CURSOR_ID}`);
  });

  it('follows the cursor to the broker that owns it', async () => {
    const owner = await startBroker({ rows: ROWS });
    const { driver, requests } = await start({
      rows: ROWS,
      owner: { host: '127.0.0.1', port: Number(owner.port) },
      failResultsWith: { status: 404, body: `Query results for ${CURSOR_ID} not found.` },
    });

    const source = await driver.stream('SELECT id, name FROM t', [], { highWaterMark: 2 });

    expect(await streamToArray(<any> source.rowStream)).toEqual(EXPECTED);
    await source.release?.();

    // The configured address is asked once, every later request goes to the owner.
    expect(calls(requests)).toEqual([
      'POST /query/sql?getCursor=true&numRows=2',
      `GET /responseStore/${CURSOR_ID}/results?offset=2&numRows=2`,
    ]);
    expect(calls(owner.requests)).toEqual([
      `GET /responseStore/${CURSOR_ID}/results?offset=2&numRows=2`,
      `GET /responseStore/${CURSOR_ID}/results?offset=4&numRows=2`,
      `DELETE /responseStore/${CURSOR_ID}`,
    ]);
  });

  it('keeps the answer of the configured address when the owning broker is unreachable', async () => {
    const unreachable = await startBroker({ rows: ROWS });
    await brokers.pop()?.close();

    const { driver } = await start({
      rows: ROWS,
      owner: { host: '127.0.0.1', port: Number(unreachable.port) },
      failResultsWith: { status: 404, body: `Query results for ${CURSOR_ID} not found.` },
    });

    const source = await driver.stream('SELECT id, name FROM t', [], { highWaterMark: 2 });

    await expect(streamToArray(<any> source.rowStream)).rejects.toThrow(
      `Unable to fetch Pinot cursor ${CURSOR_ID} at offset 2\nQuery results for ${CURSOR_ID} not found.`
    );
  });

  it('fails the stream when a page is not answered within the query timeout', async () => {
    const { driver } = await start({ rows: ROWS, hangResults: true }, 0.2);

    const source = await driver.stream('SELECT id, name FROM t', [], { highWaterMark: 2 });

    await expect(streamToArray(<any> source.rowStream)).rejects.toThrow(/timeout/);
  });

  it('aborts a page request in flight when the stream is destroyed', async () => {
    const { driver, requests } = await start({ rows: ROWS, hangResults: true });

    const source = await driver.stream('SELECT id, name FROM t', [], { highWaterMark: 2 });
    const rowStream = <Readable> source.rowStream;
    rowStream.resume();

    await waitForRequests(requests, 2);

    await new Promise((resolve) => {
      rowStream.once('close', resolve);
      rowStream.destroy();
    });
    await waitForRequests(requests, 3);

    expect(calls(requests).at(-1)).toBe(`DELETE /responseStore/${CURSOR_ID}`);
  });

  it('reports a cursor that could not be deleted', async () => {
    const { driver } = await start({ rows: ROWS, failDeleteWith: { status: 500, body: 'Response store is down' } });
    const logger = jest.fn();
    driver.setLogger(logger);

    const source = await driver.stream('SELECT id, name FROM t', [], { highWaterMark: 2 });
    await source.release?.();

    expect(logger).toHaveBeenCalledWith('Pinot Cursor Delete Failed', {
      cursorId: CURSOR_ID,
      error: '500 Response store is down',
    });
  });
});
