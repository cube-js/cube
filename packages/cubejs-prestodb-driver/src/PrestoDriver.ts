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
  BaseDriver,
  UnloadOptions
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

export type PrestoDriverExportBucket = {
  exportBucket?: string,
  bucketType?: 'gcs',
  credentials?: any,
  exportBucketCsvEscapeSymbol?: string,
};

export type PrestoDriverConfiguration = PrestoDriverExportBucket & {
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

const SUPPORTED_BUCKET_TYPES = ['gcs'];
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
      bucketType: getEnv('dbExportBucketType', { supported: ['gcs'], dataSource }),
      exportBucket: getEnv('dbExportBucket', { dataSource }),
      credentials: getEnv('dbExportGCSCredentials', { dataSource }),
      ...config
    };
    this.catalog = this.config.catalog;
    this.client = new presto.Client(this.config);
  }

  public async testConnection(): Promise<void> {
    return new Promise((resolve, reject) => {
      // Get node list of presto cluster and return it.
      // @see https://prestodb.io/docs/current/rest/node.html
      this.client.nodes(null, (error: any, _result: any[]) => {
        if (error) {
          reject(error);
        } else {
          resolve();
        }
      });
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

  public async createSchemaIfNotExists(schemaName: string) {
    await this.query(
      `CREATE SCHEMA IF NOT EXISTS ${this.config.catalog}.${schemaName}`,
      [],
    );
  }

  // Export bucket methods
  public async isUnloadSupported() {
    return this.config.exportBucket !== undefined;
  }

  public async unload(tableName: string, options: UnloadOptions) {
    if (!this.config.exportBucket) {
      throw new Error('Export bucket is not configured.');
    }

    if (!SUPPORTED_BUCKET_TYPES.includes(this.config.bucketType as string)) {
      throw new Error(`Unsupported export bucket type: ${
        this.config.bucketType
      }`);
    }

    const types = options.query
      ? await this.unloadWithSql(tableName, options.query.sql, options.query.params)
      : await this.unloadWithTable(tableName);

    const csvFile = await this.getCsvFiles(tableName);

    return {
      exportBucketCsvEscapeSymbol: this.config.exportBucketCsvEscapeSymbol,
      csvFile,
      types,
      csvNoHeader: true,
    };
  }

  private splitTableFullName(tableFullName: string) {
    const [schema, tableName] = tableFullName.split('.');
    return { schema, tableName };
  }

  private generateTableColumnsForExport(types: {name: string, type: string}[]) {
    return types.map((c) => `CAST(${c.name} AS varchar) ${c.name}`).join(', ');
  }

  private async unloadWithSql(tableFullName: string, sql: string, params: any[]) {
    return this.unloadGeneric({
      tableFullName,
      typeSql: sql,
      typeParams: params,
      fromSql: sql,
      fromParams: params
    });
  }

  private async unloadWithTable(tableFullName: string) {
    return this.unloadGeneric({
      tableFullName,
      typeSql: `SELECT * FROM ${tableFullName}`,
      typeParams: [],
      fromSql: tableFullName,
      fromParams: []
    });
  }

  private async unloadGeneric(params: {tableFullName: string, typeSql: string, typeParams: any[], fromSql: string, fromParams: any[]}) {
    if (!this.config.exportBucket) {
      throw new Error('Export bucket is not configured.');
    }

    const { bucketType, exportBucket } = this.config;
    const types = await this.queryColumnTypes(params.typeSql, params.typeParams);

    const { schema, tableName } = this.splitTableFullName(params.tableFullName);
    const tableWithCatalogAndSchema = `${this.config.catalog}.${schema}.${tableName}`;
    const protocol = bucketType === 'gcs' ? 'gs' : bucketType;
    const externalLocation = `${protocol}://${exportBucket}/${schema}/${tableName}`;
    const withParams = `( external_location = '${externalLocation}', format = 'CSV')`;
    const select = `SELECT ${this.generateTableColumnsForExport(types)} FROM (${params.fromSql})`;
    const createTableQuery = `CREATE TABLE ${tableWithCatalogAndSchema} WITH ${withParams} AS (${select})`;

    try {
      await this.query(
        createTableQuery,
        params.fromParams,
      );
    } finally {
      await this.query(`DROP TABLE IF EXISTS ${tableWithCatalogAndSchema}`, []);
    }

    return types;
  }

  public async queryColumnTypes(sql: string, params: unknown[]): Promise<{ name: string; type: string; }[]> {
    const response = await this.stream(`${sql} LIMIT 0`, params || [], { highWaterMark: 1 });
    const result = [];
    for (const column of response.types || []) {
      result.push({ name: column.name, type: this.toGenericType(column.type) });
    }
    return result;
  }

  private async getCsvFiles(
    tableFullName: string,
  ): Promise<string[]> {
    if (!this.config.exportBucket) {
      throw new Error('Export bucket is not configured.');
    }
    const { bucketType, exportBucket, credentials } = this.config;
    const { schema, tableName } = this.splitTableFullName(tableFullName);

    switch (bucketType) {
      case 'gcs':
        return this.extractFilesFromGCS({ credentials }, exportBucket, `${schema}/${tableName}`);
      default:
        throw new Error(`Unsupported export bucket type: ${bucketType}`);
    }
  }
}
