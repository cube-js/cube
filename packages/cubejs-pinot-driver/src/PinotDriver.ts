import {
  DriverInterface,
  StreamTableData,
  DownloadQueryResultsOptions,
  DownloadQueryResultsResult,
  QueryOptions,
  BaseDriver
} from '@cubejs-backend/base-driver';
import {
  getEnv,
  assertDataSource,
  extractRequestUUID,
  formatAnsi,
  isValidRequestId,
  Required,
} from '@cubejs-backend/shared';

import querystring from 'node:querystring';
import type { ConnectionOptions as TLSConnectionOptions } from 'tls';

import {
  map, zipObj
} from 'ramda';
import fetch, { Headers, Request, Response } from 'node-fetch';
import { PinotQuery } from './PinotQuery';

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
  resultTable: {
    dataSchema: {
      columnDataTypes: string[],
      columnNames: string[]
    },
    rows: any[][]
  },
  segmentStatistics: any[],
  timeUsedMs: number,
  totalDocs: number,
  traceInfo: any
};

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

  protected async request(query: string, requestId?: string): Promise<PinotResponse> {
    const toError = (error: any) => new Error(error.error ? `${error.message}\n${error.error}` : error.message);

    const request: Request = new Request(this.url, {
      method: 'POST',
      headers: new Headers({
        'Content-Type': 'application/json',
        ...this.authorizationHeaders()
      }),
      body: JSON.stringify({
        sql: query,
        queryOptions: this.queryOptions(requestId)
      })
    });

    let response: Response;

    try {
      response = await fetch(request);
    } catch (error: any) {
      throw toError(error);
    }

    if (!response.ok) {
      throw toError({ message: response.status === 401 ? 'Unauthorized request' : 'Unexpected error' });
    }

    const result: PinotResponse = await response.json();

    if (result?.exceptions?.length) {
      throw toError(result.exceptions[0]);
    }

    return result;
  }

  public async queryPromised(query: string, requestId?: string): Promise<any[] | StreamTableData> {
    const { resultTable } = await this.request(query, requestId);
    return this.normalizeResultOverColumns(resultTable.rows, resultTable.dataSchema.columnNames);
  }

  public async downloadQueryResults(
    query: string,
    values: unknown[],
    options: DownloadQueryResultsOptions,
  ): Promise<DownloadQueryResultsResult> {
    const { resultTable } = await this.request(this.prepareQueryWithParams(query, values), options?.requestId);
    const { rows, dataSchema } = resultTable;

    return {
      rows: this.normalizeResultOverColumns(rows, dataSchema.columnNames),
      types: dataSchema.columnNames.map((name, index) => ({
        name,
        type: this.toGenericType(dataSchema.columnDataTypes[index]),
      })),
    };
  }

  protected override toGenericType(columnType: string): string {
    return PinotTypeToGenericType[columnType.toLowerCase()] || super.toGenericType(columnType);
  }

  protected override quoteIdentifier(identifier: string): string {
    return identifier;
  }

  public normalizeResultOverColumns(data: any[], columns: string[]) {
    const arrayToObject = zipObj(columns);
    return map(arrayToObject, data || []);
  }
}
