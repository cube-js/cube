/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `PinotDriver` and related types declaration.
 */

import {
  DriverInterface,
  StreamTableData,
  BaseDriver
} from '@cubejs-backend/base-driver';
import {
  getEnv,
  assertDataSource,
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

/**
 * Presto driver class.
 */
export class PinotDriver extends BaseDriver implements DriverInterface {
  /**
   * Returns default concurrency value.
   */
  public static getDefaultConcurrency() {
    return 10;
  }

  private config: PinotDriverConfiguration;

  private url: string;

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

    this.config = {
      host: getEnv('dbHost', { dataSource }),
      port: getEnv('dbPort', { dataSource }),
      user: getEnv('dbUser', { dataSource }),
      database: getEnv('dbName', { dataSource }),
      basicAuth: getEnv('dbPass', { dataSource })
        ? {
          user: getEnv('dbUser', { dataSource }),
          password: getEnv('dbPass', { dataSource }),
        }
        : undefined,
      authToken: getEnv('pinotAuthToken', { dataSource }),
      ssl: this.getSslOptions(dataSource),
      queryTimeout: getEnv('dbQueryTimeout', { dataSource }),
      ...config
    };

    this.url = `${this.config.host}:${this.config.port}/query/sql`;
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

  public queryPromised(query: string): Promise<any[] | StreamTableData> {
    const toError = (error: any) => new Error(error.error ? `${error.message}\n${error.error}` : error.message);

    const request: Request = new Request(this.url, {
      method: 'POST',
      headers: new Headers({
        'Content-Type': 'application/json',
        ...this.authorizationHeaders()
      }),
      body: JSON.stringify({
        sql: query,
        queryOptions: `useMultistageEngine=true;timeoutMs=${this.config.queryTimeout}`
      })
    });

    return new Promise((resolve, reject) => {
      fetch(request)
        .then(async (response: Response) => {
          if (!response.ok) {
            if (response.status === 401) {
              return reject(toError({ message: 'Unauthorized request' }));
            }

            return reject(toError({ message: 'Unexpected error' }));
          }
          const pinotResponse: PinotResponse = await response.json();

          if (pinotResponse?.exceptions?.length) {
            return reject(toError(pinotResponse.exceptions[0]));
          }

          return resolve(this.normalizeResultOverColumns(pinotResponse.resultTable.rows, pinotResponse.resultTable.dataSchema.columnNames));
        })
        .catch((error: any) => reject(toError(error)));
    });
  }

  protected override quoteIdentifier(identifier: string): string {
    return identifier;
  }

  public normalizeResultOverColumns(data: any[], columns: string[]) {
    const arrayToObject = zipObj(columns);
    return map(arrayToObject, data || []);
  }
}
