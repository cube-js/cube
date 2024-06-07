/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `PrestoDriver` and related types declaration.
 */

import {
  DownloadQueryResultsOptions, DownloadQueryResultsResult,
  DriverCapabilities, DriverInterface,
  StreamOptions,
  StreamTableData,
  TableStructure,
  BaseDriver
} from '@cubejs-backend/base-driver';
import {
  getEnv,
  assertDataSource,
} from '@cubejs-backend/shared';

import { Transform, TransformCallback } from 'stream';
import type { ConnectionOptions as TLSConnectionOptions } from 'tls';

import {
  map, zipObj, prop, concat
} from 'ramda';
import SqlString from 'sqlstring';

const presto = require('presto-client');

export type PrestoDriverConfiguration = {
  host?: string;
  port?: string;
  catalog?: string;
  schema?: string;
  user?: string;
  // eslint-disable-next-line camelcase
  basic_auth?: { user: string, password: string };
  ssl?: string | TLSConnectionOptions;
  dataSource?: string;
  queryTimeout?: number;
};

/**
 * Presto driver class.
 */
export class PrestoDriver extends BaseDriver implements DriverInterface {
  /**
   * Returns default concurrency value.
   */
  public static getDefaultConcurrency() {
    return 2;
  }

  private config: PrestoDriverConfiguration;

  private catalog: string | undefined;

  private client: any;

  /**
   * Class constructor.
   */
  public constructor(config: PrestoDriverConfiguration = {}) {
    super();

    const dataSource =
      config.dataSource ||
      assertDataSource('default');

    this.config = {
      host: getEnv('dbHost', { dataSource }),
      port: getEnv('dbPort', { dataSource }),
      catalog:
        getEnv('prestoCatalog', { dataSource }) ||
        getEnv('dbCatalog', { dataSource }),
      schema:
        getEnv('dbName', { dataSource }) ||
        getEnv('dbSchema', { dataSource }),
      user: getEnv('dbUser', { dataSource }),
      basic_auth: getEnv('dbPass', { dataSource })
        ? {
          user: getEnv('dbUser', { dataSource }),
          password: getEnv('dbPass', { dataSource }),
        }
        : undefined,
      ssl: this.getSslOptions(dataSource),
      ...config
    };
    this.catalog = this.config.catalog;
    this.client = new presto.Client(this.config);
  }

  public testConnection() {
    const query = SqlString.format('show catalogs like ?', [`%${this.catalog}%`]);

    return (<Promise<any[]>> this.queryPromised(query, false))
      .then(catalogs => {
        if (catalogs.length === 0) {
          throw new Error(`Catalog not found '${this.catalog}'`);
        }
      });
  }

  public query(query: string, values: unknown[]): Promise<any[]> {
    return <Promise<any[]>> this.queryPromised(this.prepareQueryWithParams(query, values), false);
  }

  public prepareQueryWithParams(query: string, values: unknown[]) {
    return SqlString.format(query, (values || []).map(value => (typeof value === 'string' ? {
      toSqlString: () => SqlString.escape(value).replace(/\\\\([_%])/g, '\\$1'),
    } : value)));
  }

  public queryPromised(query: string, streaming: boolean): Promise<any[] | StreamTableData> {
    const toError = (error: any) => new Error(error.error ? `${error.message}\n${error.error}` : error.message);
    if (streaming) {
      const rowStream = new Transform({
        writableObjectMode: true,
        readableObjectMode: true,

        transform(obj: any, encoding: string, callback: TransformCallback) {
          callback(null, obj);
        }
      });

      return new Promise((resolve, reject) => {
        this.client.execute({
          query,
          schema: this.config.schema || 'default',
          session: this.config.queryTimeout ? `query_max_run_time=${this.config.queryTimeout}s` : undefined,
          columns: (error: any, columns: TableStructure) => {
            resolve({
              rowStream,
              types: columns
            });
          },
          data: (error: any, data: any[], columns: TableStructure) => {
            const normalData = this.normalizeResultOverColumns(data, columns);
            for (const obj of normalData) {
              rowStream.write(obj);
            }
          },
          success: () => {
            rowStream.end();
          },
          error: (error: any) => {
            reject(toError(error));
          }
        });
      });
    } else {
      return new Promise((resolve, reject) => {
        let fullData: any[] = [];

        this.client.execute({
          query,
          schema: this.config.schema || 'default',
          data: (error: any, data: any[], columns: TableStructure) => {
            const normalData = this.normalizeResultOverColumns(data, columns);
            fullData = concat(normalData, fullData);
          },
          success: () => {
            resolve(fullData);
          },
          error: (error: any) => {
            reject(toError(error));
          }
        });
      });
    }
  }

  public downloadQueryResults(query: string, values: unknown[], options: DownloadQueryResultsOptions): Promise<DownloadQueryResultsResult> {
    if (options.streamImport) {
      return <Promise<DownloadQueryResultsResult>> this.stream(query, values, options);
    }
    return super.downloadQueryResults(query, values, options);
  }

  public informationSchemaQuery() {
    if (this.config.schema) {
      return `${super.informationSchemaQuery()} AND columns.table_schema = '${this.config.schema}'`;
    }
    return super.informationSchemaQuery();
  }

  public normalizeResultOverColumns(data: any[], columns: TableStructure) {
    const columnNames = map(prop('name'), columns || []);
    const arrayToObject = zipObj(columnNames);
    return map(arrayToObject, data || []);
  }

  public stream(query: string, values: unknown[], _options: StreamOptions): Promise<StreamTableData> {
    const queryWithParams = this.prepareQueryWithParams(query, values);

    return <Promise<StreamTableData>> this.queryPromised(queryWithParams, true);
  }

  public capabilities(): DriverCapabilities {
    return {
      unloadWithoutTempTable: true
    };
  }
}
