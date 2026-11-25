import {
  DriverInterface,
  StreamTableData,
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
  // Set only on responses served from a cursor.
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

  protected async send(url: string, init: { method: string, body?: string }): Promise<Response> {
    const request: Request = new Request(url, {
      ...init,
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
      throw this.toError({ message: response.status === 401 ? 'Unauthorized request' : 'Unexpected error' });
    }

    return this.readResponse(response);
  }

  protected async fetchCursorPage(cursorId: string, offset: number, numRows: number): Promise<PinotCursorResponse> {
    const response = await this.send(
      `${this.responseStoreUrl}/${encodeURIComponent(cursorId)}/results?offset=${offset}&numRows=${numRows}`,
      { method: 'GET' }
    );

    if (!response.ok) {
      if (response.status === 401) {
        throw this.toError({ message: 'Unauthorized request' });
      }

      // The response store answers errors in plain text, e.g. for a cursor that has
      // expired or that belongs to another broker behind the same address.
      throw this.toError({
        message: `Unable to fetch Pinot cursor ${cursorId} at offset ${offset}`,
        error: (await response.text()).trim(),
      });
    }

    const page = await this.readResponse(response);
    if (!isCursorResponse(page) || page.numRows === 0) {
      throw this.toError({ message: `Unexpected Pinot cursor ${cursorId} response at offset ${offset}` });
    }

    return page;
  }

  protected async deleteCursor(cursorId: string): Promise<void> {
    try {
      await this.send(`${this.responseStoreUrl}/${encodeURIComponent(cursorId)}`, { method: 'DELETE' });
    } catch {
      // The broker drops the cursor on its own once it expires.
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
  ): Promise<StreamTableData> {
    const pageSize = highWaterMark || getEnv('dbQueryStreamHighWaterMark');
    const first = await this.request(this.prepareQueryWithParams(query, values), requestId, pageSize);

    const cursor = isCursorResponse(first) ? first : null;
    let pending: PinotResponse | null = first;
    let nextOffset = cursor ? cursor.offset + cursor.numRows : 0;

    const nextPage = async () => {
      let page: PinotResponse | null = pending;
      pending = null;

      if (!page) {
        if (!cursor || nextOffset >= cursor.numRowsResultSet) {
          return null;
        }

        const fetched = await this.fetchCursorPage(cursor.requestId, nextOffset, pageSize);
        nextOffset = fetched.offset + fetched.numRows;
        page = fetched;
      }

      return this.resultTableRows(page.resultTable);
    };

    let released = false;

    return {
      rowStream: new PinotRowStream(nextPage, pageSize),
      types: this.resultTableTypes(first.resultTable),
      release: async () => {
        if (cursor && !released) {
          released = true;
          await this.deleteCursor(cursor.requestId);
        }
      },
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
