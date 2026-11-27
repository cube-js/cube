import {
  DriverInterface,
  StreamTableData,
  DownloadStreamTableData,
  DownloadQueryResultsOptions,
  DownloadQueryResultsResult,
  QueryOptions,
  StreamOptions,
  TableStructure,
  BaseDriver
} from '@cubejs-backend/base-driver';
import {
  getEnv,
  assertDataSource,
  buildObjectShape,
  extractRequestUUID,
  formatAnsi,
  isValidRequestId,
  Required,
} from '@cubejs-backend/shared';

import querystring from 'node:querystring';
import type { ConnectionOptions as TLSConnectionOptions } from 'tls';

import fetch, { Headers, Request, Response } from 'node-fetch';
import { PinotQuery } from './PinotQuery';
import { PinotRow, PinotRowStream } from './PinotRowStream';

export type PinotDriverConfiguration = {
  host?: string;
  port?: string;
  user?: string;
  database?: string;
  basicAuth?: { user: string, password: string };
  authToken?: string;
  ssl?: string | TLSConnectionOptions;
  dataSource?: string;
  queryTimeout?: number;
  nullHandling?: boolean;
  /**
   * Sent as the `applicationName` query option, which puts the queries under that
   * application's QPS quota. Queries without it are exempt from application quotas.
   * @see https://docs.pinot.apache.org/build-with-pinot/querying-and-sql/query-execution-controls/query-quotas
   */
  applicationName?: string;
  /**
   * Sent as the `workloadName` query option, which charges the queries to that
   * workload's CPU and memory budget when query workload isolation is enabled.
   * @see https://docs.pinot.apache.org/operate-pinot/tuning/workload-query-isolation
   */
  workloadName?: string;
  preAggregations?: boolean;
};

type AuthorizationHeaders = {
  Authorization: string;
  database?: string;
};

type PinotResultTable = {
  dataSchema: {
    columnDataTypes: string[],
    columnNames: string[]
  },
  rows: any[][]
};

type PinotResponse = {
  exceptions: any[],
  minConsumingFreshnessTimeMs: number,
  numConsumingSegmentsQueried: number,
  numDocsScanned: number,
  numEntriesScannedInFilter: number,
  numEntriesScannedPostFilter: number,
  numGroupsLimitReached: boolean,
  numSegmentsMatched: number,
  numSegmentsProcessed: number,
  numSegmentsQueried: number,
  numServersQueried: number,
  numServersResponded: number,
  // Pinot omits the table from a cursor response when the result is empty.
  resultTable?: PinotResultTable,
  segmentStatistics: any[],
  timeUsedMs: number,
  totalDocs: number,
  traceInfo: any,
  requestId?: string,
  numRowsResultSet?: number,
  brokerHost?: string,
  brokerPort?: number,
  offset?: number,
  numRows?: number,
};

type PinotCursorResponse = PinotResponse & {
  requestId: string,
  numRowsResultSet: number,
  offset: number,
  numRows: number,
};

const isCursorResponse = (response: PinotResponse): response is PinotCursorResponse => (
  typeof response.offset === 'number' &&
  typeof response.numRows === 'number' &&
  typeof response.numRowsResultSet === 'number' &&
  Boolean(response.requestId)
);

type PinotCursor = {
  id: string,
  storeUrl: string,
  brokerStoreUrl: string | null,
};

type SendOptions = {
  method: string,
  body?: string,
  timeout?: number,
  signal?: AbortSignal,
};

// Deleting a cursor is a courtesy to the broker, which expires it anyway, so it must not
// hold a caller for as long as a query may run.
const CURSOR_DELETE_TIMEOUT_MS = 10 * 1000;

const PinotTypeToGenericType: Record<string, string> = {
  string: 'text',
  int: 'int',
  long: 'bigint',
  float: 'double',
  double: 'double',
  big_decimal: 'decimal',
  boolean: 'boolean',
  timestamp: 'timestamp',
  json: 'text',
  bytes: 'text',
};

export class PinotDriver extends BaseDriver implements DriverInterface {
  public dbType(): string {
    return 'pinot';
  }

  public static getDefaultConcurrency() {
    return 10;
  }

  protected readonly config: Required<PinotDriverConfiguration, 'queryTimeout'>;

  protected readonly url: string;

  protected readonly responseStoreUrl: string;

  public static dialectClass() {
    return PinotQuery;
  }

  /**
   * Class constructor.
   */
  public constructor(config: PinotDriverConfiguration = {}) {
    super();

    const dataSource =
      config.dataSource ||
      assertDataSource('default');
    const preAggregations = config.preAggregations || false;

    this.config = {
      host: getEnv('dbHost', { dataSource, preAggregations }),
      port: getEnv('dbPort', { dataSource, preAggregations }),
      user: getEnv('dbUser', { dataSource, preAggregations }),
      database: getEnv('dbName', { dataSource, preAggregations }),
      basicAuth: getEnv('dbPass', { dataSource, preAggregations })
        ? {
          user: getEnv('dbUser', { dataSource, preAggregations }),
          password: getEnv('dbPass', { dataSource, preAggregations }),
        }
        : undefined,
      authToken: getEnv('pinotAuthToken', { dataSource, preAggregations }),
      ssl: this.getSslOptions(dataSource, preAggregations),
      nullHandling: getEnv('pinotNullHandling', { dataSource, preAggregations }),
      queryTimeout: getEnv('dbQueryTimeout', { dataSource, preAggregations }),
      ...config
    };

    this.assertQueryOptionValue('applicationName', this.config.applicationName);
    this.assertQueryOptionValue('workloadName', this.config.workloadName);

    const useSsl = getEnv('dbSsl', { dataSource, preAggregations });
    const rawHost = this.config.host || '';
    const host = /^https?:\/\//i.test(rawHost)
      ? rawHost
      : `${useSsl ? 'https' : 'http'}://${rawHost}`;
    this.url = `${host}:${this.config.port}/query/sql`;
    this.responseStoreUrl = `${host}:${this.config.port}/responseStore`;
  }

  public readOnly(): boolean {
    return true;
  }

  public testConnection() {
    return (<Promise<any[]>> this.queryPromised('select 1'))
      .then(response => {
        if (response.length === 0) {
          throw new Error('Unable to connect to your Pinot instance');
        }
      });
  }

  public query(query: string, values: unknown[], options?: QueryOptions): Promise<any[]> {
    return <Promise<any[]>> this.queryPromised(this.prepareQueryWithParams(query, values), options?.requestId);
  }

  protected prepareQueryWithParams(query: string, values: unknown[]) {
    return formatAnsi(query, values || []);
  }

  public authorizationHeaders(): AuthorizationHeaders | {} {
    if (this.config.authToken) {
      const res: AuthorizationHeaders = { Authorization: `Bearer ${this.config.authToken}` };

      if (this.config.database) {
        res.database = this.config.database;
      }

      return res;
    }

    if (!this.config.basicAuth) {
      return {};
    }

    const encodedCredentials = Buffer.from(`${this.config.basicAuth.user}:${this.config.basicAuth.password}`).toString('base64');

    return { Authorization: `Basic ${encodedCredentials}` };
  }

  /**
   * The queryOptions string has no escaping, so a value with `;` or `=` would
   * be parsed by Pinot as extra options.
   */
  protected assertQueryOptionValue(name: string, value?: unknown): void {
    if (value !== undefined && (typeof value !== 'string' || value.trim() === '' || /[;=]/.test(value))) {
      throw new TypeError(
        `Pinot ${name} must be a non-empty string without ';' and '=', got: ${JSON.stringify(value)}`
      );
    }
  }

  protected queryOptions(requestId?: string): string {
    const options: Record<string, string | number | boolean> = {
      useMultistageEngine: true,
      enableNullHandling: Boolean(this.config.nullHandling),
      timeoutMs: this.config.queryTimeout * 1000,
    };

    if (this.config.applicationName) {
      options.applicationName = this.config.applicationName;
    }

    if (this.config.workloadName) {
      options.workloadName = this.config.workloadName;
    }

    const clientQueryId = requestId && extractRequestUUID(requestId);
    if (clientQueryId && isValidRequestId(clientQueryId) && !clientQueryId.includes('=')) {
      options.clientQueryId = clientQueryId;
    }

    // Pinot splits the string on ';' and '=' without decoding it, so the values are not escaped.
    return querystring.stringify(options, ';', '=', { encodeURIComponent: (value) => value });
  }

  protected toError(error: any): Error {
    return new Error(error.error ? `${error.message}\n${error.error}` : error.message);
  }

  protected async send(url: string, { signal, ...init }: SendOptions): Promise<Response> {
    const request: Request = new Request(url, {
      ...init,
      // node-fetch ships its own AbortSignal type, which the global one satisfies at runtime.
      signal: <any> signal,
      headers: new Headers({
        'Content-Type': 'application/json',
        ...this.authorizationHeaders()
      }),
    });

    try {
      return await fetch(request);
    } catch (error: any) {
      throw this.toError(error);
    }
  }

  protected async readResponse(response: Response): Promise<PinotResponse> {
    const result: PinotResponse = await response.json();

    if (result?.exceptions?.length) {
      throw this.toError(result.exceptions[0]);
    }

    return result;
  }

  /**
   * With `cursorPageSize` the broker keeps the result in its response store and
   * returns only the first page. Brokers without cursor support (before 1.3)
   * ignore the parameters and return the whole result.
   */
  protected async request(query: string, requestId?: string, cursorPageSize?: number): Promise<PinotResponse> {
    const url = cursorPageSize ? `${this.url}?getCursor=true&numRows=${cursorPageSize}` : this.url;

    const response = await this.send(url, {
      method: 'POST',
      body: JSON.stringify({
        sql: query,
        queryOptions: this.queryOptions(requestId)
      })
    });

    if (!response.ok) {
      await this.discardBody(response);

      throw this.toError({ message: response.status === 401 ? 'Unauthorized request' : 'Unexpected error' });
    }

    return this.readResponse(response);
  }

  /**
   * node-fetch keeps the socket of an unread response busy until the body is collected.
   */
  protected async discardBody(response: Response): Promise<void> {
    try {
      await response.text();
    } catch {
      // Nothing to release when the body cannot be read.
    }
  }

  protected responseStoreTimeout(): number {
    return this.config.queryTimeout * 1000;
  }

  protected createCursor(response: PinotCursorResponse): PinotCursor {
    let brokerStoreUrl: string | null = null;

    if (response.brokerHost && response.brokerPort) {
      const { protocol } = new URL(this.responseStoreUrl);
      brokerStoreUrl = `${protocol}//${response.brokerHost}:${response.brokerPort}/responseStore`;
    }

    return {
      id: response.requestId,
      storeUrl: this.responseStoreUrl,
      brokerStoreUrl: brokerStoreUrl === this.responseStoreUrl ? null : brokerStoreUrl,
    };
  }

  /**
   * The configured address may balance over several brokers, while a cursor is known only
   * to the broker that ran the query.
   */
  protected async sendToResponseStore(
    cursor: PinotCursor,
    path: string,
    options: SendOptions,
  ): Promise<Response> {
    const response = await this.send(`${cursor.storeUrl}/${encodeURIComponent(cursor.id)}${path}`, options);
    if (response.status !== 404 || cursor.brokerStoreUrl === null) {
      return response;
    }

    const { brokerStoreUrl } = cursor;
    // One attempt only: an expired cursor is missing on the owning broker too.
    cursor.brokerStoreUrl = null;

    let brokerResponse: Response;

    try {
      brokerResponse = await this.send(`${brokerStoreUrl}/${encodeURIComponent(cursor.id)}${path}`, options);
    } catch {
      // The broker may be unreachable from here; the answer of the configured address stands.
      return response;
    }

    await this.discardBody(response);
    cursor.storeUrl = brokerStoreUrl;

    return brokerResponse;
  }

  /**
   * Pinot plans `LIMIT 0` without scanning anything, so the schema of an empty result
   * costs planning instead of a second execution.
   */
  protected async requestEmptyResult(query: string, requestId?: string): Promise<PinotResponse> {
    try {
      const response = await this.request(`SELECT * FROM (${query}) LIMIT 0`, requestId);
      if (response.resultTable) {
        return response;
      }
    } catch {
      // Falls through to the query itself, which reports its own error if it has one.
    }

    return this.request(query, requestId);
  }

  protected async fetchCursorPage(
    cursor: PinotCursor,
    offset: number,
    numRows: number,
    signal?: AbortSignal,
  ): Promise<PinotCursorResponse> {
    const response = await this.sendToResponseStore(
      cursor,
      `/results?offset=${offset}&numRows=${numRows}`,
      { method: 'GET', timeout: this.responseStoreTimeout(), signal }
    );

    if (!response.ok) {
      if (response.status === 401) {
        await this.discardBody(response);

        throw this.toError({ message: 'Unauthorized request' });
      }

      // The response store answers errors in plain text, e.g. for a cursor that has
      // expired or that belongs to another broker behind the same address.
      throw this.toError({
        message: `Unable to fetch Pinot cursor ${cursor.id} at offset ${offset}`,
        error: (await response.text()).trim(),
      });
    }

    const page = await this.readResponse(response);
    if (!isCursorResponse(page) || page.numRows === 0) {
      throw this.toError({ message: `Unexpected Pinot cursor ${cursor.id} response at offset ${offset}` });
    }

    return page;
  }

  /**
   * Never rejects: the broker drops the cursor on its own once it expires, so a failed
   * delete is reported and otherwise left alone.
   */
  protected async deleteCursor(cursor: PinotCursor): Promise<void> {
    let failure: string | null = null;

    try {
      const response = await this.sendToResponseStore(
        cursor,
        '',
        { method: 'DELETE', timeout: Math.min(this.responseStoreTimeout(), CURSOR_DELETE_TIMEOUT_MS) }
      );

      if (response.ok) {
        await this.discardBody(response);
      } else {
        failure = `${response.status} ${(await response.text()).trim()}`.trim();
      }
    } catch (error: any) {
      failure = error.message;
    }

    if (failure !== null && this.logger) {
      this.logger('Pinot Cursor Delete Failed', { cursorId: cursor.id, error: failure });
    }
  }

  protected resultTableRows(resultTable?: PinotResultTable): PinotRow[] {
    if (!resultTable) {
      return [];
    }

    return this.normalizeResultOverColumns(resultTable.rows, resultTable.dataSchema.columnNames);
  }

  protected resultTableTypes(resultTable?: PinotResultTable): TableStructure {
    if (!resultTable) {
      return [];
    }

    const { columnNames, columnDataTypes } = resultTable.dataSchema;

    return columnNames.map((name, index) => ({
      name,
      type: this.toGenericType(columnDataTypes[index]),
    }));
  }

  public async stream(
    query: string,
    values: unknown[],
    { highWaterMark, requestId }: StreamOptions,
  ): Promise<DownloadStreamTableData> {
    const pageSize = highWaterMark || getEnv('dbQueryStreamHighWaterMark');
    const sql = this.prepareQueryWithParams(query, values);
    let first = await this.request(sql, requestId, pageSize);

    if (isCursorResponse(first) && !first.resultTable) {
      // An empty cursor result carries no schema, while a pre-aggregation build needs the
      // column types even for an empty table. A plain response always has them.
      [first] = await Promise.all([
        this.requestEmptyResult(sql, requestId),
        this.deleteCursor(this.createCursor(first)),
      ]);
    }

    // The closures below outlive the first page, so they hold the cursor fields they need
    // rather than the response, which would keep its rows in memory for the whole stream.
    const cursor = isCursorResponse(first) ? this.createCursor(first) : null;
    const totalRows = isCursorResponse(first) ? first.numRowsResultSet : 0;
    let nextOffset = isCursorResponse(first) ? first.offset + first.numRows : 0;
    let pendingRows: PinotRow[] | null = this.resultTableRows(first.resultTable);

    const abortController = new AbortController();

    const nextPage = async () => {
      if (pendingRows) {
        const rows = pendingRows;
        pendingRows = null;

        return rows;
      }

      if (cursor === null || nextOffset >= totalRows) {
        return null;
      }

      const page = await this.fetchCursorPage(cursor, nextOffset, pageSize, abortController.signal);
      nextOffset = page.offset + page.numRows;

      return this.resultTableRows(page.resultTable);
    };

    let released: Promise<void> | null = null;

    const release = (): Promise<void> => {
      if (released === null) {
        // A page still in flight would otherwise race the delete and hold its socket.
        abortController.abort();
        released = cursor === null ? Promise.resolve() : this.deleteCursor(cursor);
      }

      return released;
    };

    if (cursor !== null && nextOffset >= totalRows) {
      // Saves the consumer a round trip at the end.
      release();
    }

    return {
      // A consumer that never calls `release` must not leave the result in the broker's
      // response store.
      rowStream: new PinotRowStream(nextPage, pageSize, release),
      types: this.resultTableTypes(first.resultTable),
      release,
    };
  }

  public async queryPromised(query: string, requestId?: string): Promise<any[] | StreamTableData> {
    const { resultTable } = await this.request(query, requestId);
    return this.resultTableRows(resultTable);
  }

  public async downloadQueryResults(
    query: string,
    values: unknown[],
    options: DownloadQueryResultsOptions,
  ): Promise<DownloadQueryResultsResult> {
    if (options?.streamImport) {
      return this.stream(query, values, options);
    }

    const { resultTable } = await this.request(this.prepareQueryWithParams(query, values), options?.requestId);

    return {
      rows: this.resultTableRows(resultTable),
      types: this.resultTableTypes(resultTable),
    };
  }

  protected override toGenericType(columnType: string): string {
    return PinotTypeToGenericType[columnType.toLowerCase()] || super.toGenericType(columnType);
  }

  protected override quoteIdentifier(identifier: string): string {
    return identifier;
  }

  public normalizeResultOverColumns(data: any[], columns: string[]): PinotRow[] {
    // Cloning a shape that already holds every column keeps the rows in fast-properties mode.
    const objectShape = buildObjectShape(columns);

    return (data || []).map((values) => {
      const row: PinotRow = objectShape === null ? {} : { ...objectShape };

      for (let i = 0; i < columns.length; i++) {
        row[columns[i]] = values[i];
      }

      return row;
    });
  }
}
