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
  UnloadOptions,
  DownloadTableCSVData,
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
import { S3, GetObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';

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
  unloadCatalog?: string;
  unloadSchema?: string;
  unloadBucket?: string;
  unloadPrefix?: string;
  region?: string;
  exportBucketCsvEscapeSymbol?: string
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
      region: config.region || getEnv('prestoAwsRegion', { dataSource }),
      unloadBucket: config.unloadBucket || getEnv('prestoUnloadBucket', { dataSource }),
      unloadPrefix: config.unloadPrefix || getEnv('prestoUnloadPrefix', { dataSource }),
      unloadCatalog: config.unloadCatalog || getEnv('prestoUnloadCatalog', { dataSource }),
      unloadSchema: config.unloadSchema || getEnv('prestoUnloadSchema', { dataSource }),
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

  public async isUnloadSupported() {
    return this.config.unloadBucket !== undefined
      && this.config.unloadPrefix !== undefined
      && this.config.unloadCatalog !== undefined
      && this.config.unloadSchema !== undefined;
  }

  public async unload(tableName: string, options: UnloadOptions): Promise<DownloadTableCSVData> {
    /*
      "tableName" is a bit misleading since it also includes schema name. Ex: dev_pre_aggregations.your_table_name, 
      if using this name directly on trino, remember to quote it like its done with CREATE TABLE AS query for unloading
    */
    const columns = await this.unloadWithSql(tableName, options)
    const files = await this.getCsvFiles(tableName)

    return {
      csvFile: files,
      types: columns,
      csvNoHeader: true,
      csvDelimiter: '^A'
    }
  }

  private async unloadWithSql(
    tableName: string,
    unloadOptions: UnloadOptions,): Promise<TableStructure> {
      const unloadSchema = this.config.unloadSchema!;
      const unloadCatalog = this.config.unloadCatalog!;
      const trinoTable = `${unloadCatalog}.${unloadSchema}."${tableName}"`

      const dropIfExistsSql = /* sql */`
        DROP TABLE IF EXISTS ${trinoTable}
      `
      await this.query(dropIfExistsSql, [])

      const unloadSql = /* sql */`
        CREATE TABLE ${unloadCatalog}.${unloadSchema}."${tableName}"
        WITH (FORMAT='TEXTFILE') AS ${unloadOptions.query!.sql}
      `
      await this.query(unloadSql, unloadOptions.query!.params)
      const columns = await this.tableColumns(unloadCatalog, unloadSchema, tableName)

      return columns;
  }

  /*
    This is based on on super.tableColumnTypes. The problem with the original method
    was that it assumed that tableName did not contain dots and it extracted the schema
    from there. Also it didn't consider trino's catalog.
  */
  private async tableColumns(catalog: string, schema: string, table: string): Promise<TableStructure> {
    const columns = await this.query(
      `SELECT columns.column_name as ${this.quoteIdentifier('column_name')},
             columns.table_name as ${this.quoteIdentifier('table_name')},
             columns.table_schema as ${this.quoteIdentifier('table_schema')},
             columns.data_type  as ${this.quoteIdentifier('data_type')}
      FROM information_schema.columns
      WHERE table_catalog = ${this.param(0)} AND table_schema = ${this.param(1)} AND table_name = ${this.param(2)}`,
      [catalog, schema, table]
    );

    return columns.map(c => ({ name: c.column_name, type: this.toGenericType(c.data_type) }));
  }

  /**
   * Returns an array of signed URLs of the unloaded csv files.
   *
   * Copied from athena driver
   */
  public async getCsvFiles(tableName: string): Promise<string[]> {
    const client = new S3({
      region: this.config.region!,
    });
    const list = await client.listObjectsV2({
      Bucket: this.config.unloadBucket!,
      Prefix: `${this.config.unloadPrefix}/${tableName}`,
    });
    if (!list.Contents) {
      return [];
    } else {
      const files = await Promise.all(
        list.Contents.map(async (file) => {
          const command = new GetObjectCommand({
            Bucket: this.config.unloadBucket,
            Key: file.Key,
          });
          return getSignedUrl(client, command, { expiresIn: 3600 });
        })
      );

      return files;
    }
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
