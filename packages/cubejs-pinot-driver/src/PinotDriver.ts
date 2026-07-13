import {
  DriverInterface,
  StreamTableData,
  DownloadQueryResultsOptions,
  DownloadQueryResultsResult,
  BaseDriver
} from '@cubejs-backend/base-driver';
import {
  getEnv,
  assertDataSource, Required,
} from '@cubejs-backend/shared';

import type { ConnectionOptions as TLSConnectionOptions } from 'tls';

import {
  map, zipObj
} from 'ramda';
import SqlString from 'sqlstring';
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
    const query = SqlString.format('select 1');

    return (<Promise<any[]>> this.queryPromised(query))
      .then(response => {
        if (response.length === 0) {
          throw new Error('Unable to connect to your Pinot instance');
        }
      });
  }

  public query(query: string, values: unknown[]): Promise<any[]> {
    return <Promise<any[]>> this.queryPromised(this.prepareQueryWithParams(query, values));
  }

  public prepareQueryWithParams(query: string, values: unknown[]) {
    return SqlString.format(query, (values || []).map(value => (typeof value === 'string' ? {
      toSqlString: () => SqlString.escape(value).replace(/\\\\([_%])/g, '\\$1'),
    } : value)));
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

  protected async request(query: string): Promise<PinotResponse> {
    const toError = (error: any) => new Error(error.error ? `${error.message}\n${error.error}` : error.message);

    const request: Request = new Request(this.url, {
      method: 'POST',
      headers: new Headers({
        'Content-Type': 'application/json',
        ...this.authorizationHeaders()
      }),
      body: JSON.stringify({
        sql: query,
        queryOptions: `useMultistageEngine=true;enableNullHandling=${this.config.nullHandling};timeoutMs=${this.config.queryTimeout * 1000}`
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

  public async queryPromised(query: string): Promise<any[] | StreamTableData> {
    const { resultTable } = await this.request(query);
    return this.normalizeResultOverColumns(resultTable.rows, resultTable.dataSchema.columnNames);
  }

  public async downloadQueryResults(
    query: string,
    values: unknown[],
    _options: DownloadQueryResultsOptions,
  ): Promise<DownloadQueryResultsResult> {
    const { resultTable } = await this.request(this.prepareQueryWithParams(query, values));
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
