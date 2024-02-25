/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `ClickHouseDriver` and related types declaration.
 */

import {
  getEnv,
  assertDataSource,
} from '@cubejs-backend/shared';
import {
  BaseDriver,
  DownloadQueryResultsOptions,
  DownloadQueryResultsResult,
  DriverCapabilities,
  DriverInterface,
  QuerySchemasResult,
  StreamOptions,
  StreamTableDataWithTypes,
} from '@cubejs-backend/base-driver';
import genericPool, { Pool } from 'generic-pool';
import { v4 as uuidv4 } from 'uuid';
import sqlstring from 'sqlstring';
import * as moment from 'moment';

import { HydrationStream, transformRow } from './HydrationStream';

const ClickHouse = require('@apla/clickhouse');

const ClickhouseTypeToGeneric: Record<string, string> = {
  enum: 'text',
  string: 'text',
  datetime: 'timestamp',
  datetime64: 'timestamp',
  date: 'date',
  decimal: 'decimal',
  // integers
  int8: 'int',
  int16: 'int',
  int32: 'int',
  int64: 'bigint',
  // unsigned int
  uint8: 'int',
  uint16: 'int',
  uint32: 'int',
  uint64: 'bigint',
  // floats
  float32: 'float',
  float64: 'double',
};

interface ClickHouseDriverOptions {
  host?: string,
  port?: string,
  auth?: string,
  protocol?: string,
  database?: string,
  readOnly?: boolean,
  queryOptions?: object,
}

/**
 * ClickHouse driver class.
 */
export class ClickHouseDriver extends BaseDriver implements DriverInterface {
  /**
   * Returns default concurrency value.
   */
  public static getDefaultConcurrency(): number {
    return 5;
  }

  protected readonly pool: Pool<any>;

  protected readonly readOnlyMode: boolean;

  protected readonly config: any;

  /**
   * Class constructor.
   */
  public constructor(
    config: ClickHouseDriverOptions & {
      /**
       * Data source name.
       */
      dataSource?: string,

      /**
       * Max pool size value for the [cube]<-->[db] pool.
       */
      maxPoolSize?: number,

      /**
       * Time to wait for a response from a connection after validation
       * request before determining it as not valid. Default - 10000 ms.
       */
      testConnectionTimeout?: number,
    } = {},
  ) {
    super({
      testConnectionTimeout: config.testConnectionTimeout,
    });

    const dataSource =
      config.dataSource ||
      assertDataSource('default');

    this.config = {
      host: getEnv('dbHost', { dataSource }),
      port: getEnv('dbPort', { dataSource }),
      auth:
        getEnv('dbUser', { dataSource }) ||
        getEnv('dbPass', { dataSource })
          ? `${
            getEnv('dbUser', { dataSource })
          }:${
            getEnv('dbPass', { dataSource })
          }`
          : '',
      protocol: getEnv('dbSsl', { dataSource }) ? 'https:' : 'http:',
      queryOptions: {
        database:
          getEnv('dbName', { dataSource }) ||
          config && config.database ||
          'default'
      },
      ...config
    };
    this.readOnlyMode =
      getEnv('clickhouseReadOnly', { dataSource }) === 'true';
    this.pool = genericPool.createPool({
      create: async () => new ClickHouse({
        ...this.config,
        queryOptions: {
          //
          //
          // If ClickHouse user's permissions are restricted with "readonly = 1",
          // change settings queries are not allowed. Thus, "join_use_nulls" setting
          // can not be changed
          //
          //
          ...(this.readOnlyMode ? {} : { join_use_nulls: 1 }),
          session_id: uuidv4(),
          ...this.config.queryOptions,
        }
      }),
      destroy: () => Promise.resolve()
    }, {
      min: 0,
      max:
        config.maxPoolSize ||
        getEnv('dbMaxPoolSize', { dataSource }) ||
        8,
      evictionRunIntervalMillis: 10000,
      softIdleTimeoutMillis: 30000,
      idleTimeoutMillis: 30000,
      acquireTimeoutMillis: 20000
    });
  }

  protected withConnection(fn: (con: any, queryId: string) => Promise<any>) {
    const self = this;
    const connectionPromise = this.pool.acquire();
    const queryId = uuidv4();

    let cancelled = false;
    const cancelObj: any = {};

    const promise: any = connectionPromise.then((connection: any) => {
      cancelObj.cancel = async () => {
        cancelled = true;
        await self.withConnection(async conn => {
          await conn.querying(`KILL QUERY WHERE query_id = '${queryId}'`);
        });
      };
      return fn(connection, queryId)
        .then(res => this.pool.release(connection).then(() => {
          if (cancelled) {
            throw new Error('Query cancelled');
          }
          return res;
        }))
        .catch((err) => this.pool.release(connection).then(() => {
          if (cancelled) {
            throw new Error('Query cancelled');
          }
          throw err;
        }));
    });
    promise.cancel = () => cancelObj.cancel();

    return promise;
  }

  public async testConnection() {
    await this.query('SELECT 1', []);
  }

  public readOnly() {
    return (this.config.readOnly != null || this.readOnlyMode) ?
      (!!this.config.readOnly || this.readOnlyMode) :
      true;
  }

  public async query(query: string, values: unknown[]) {
    return this.queryResponse(query, values).then((res: any) => this.normaliseResponse(res));
  }

  protected queryResponse(query: string, values: unknown[]) {
    const formattedQuery = sqlstring.format(query, values);

    return this.withConnection((connection, queryId) => connection.querying(formattedQuery, {
      dataObjects: true,
      queryOptions: {
        query_id: queryId,
        //
        //
        // If ClickHouse user's permissions are restricted with "readonly = 1",
        // change settings queries are not allowed. Thus, "join_use_nulls" setting
        // can not be changed
        //
        //
        ...(this.readOnlyMode ? {} : { join_use_nulls: 1 }),
      }
    }));
  }

  protected normaliseResponse(res: any) {
    if (res.data) {
      const meta = res.meta.reduce(
        (state: any, element: any) => ({ [element.name]: element, ...state }),
        {}
      );

      res.data.forEach((row: any) => {
        transformRow(row, meta);
      });
    }
    return res.data;
  }

  public async release() {
    await this.pool.drain();
    await this.pool.clear();
  }

  public informationSchemaQuery() {
    return `
      SELECT name as column_name,
             table as table_name,
             database as table_schema,
             type as data_type
        FROM system.columns
       WHERE database = '${this.config.queryOptions.database}'
    `;
  }

  protected override getTablesForSpecificSchemasQuery(schemasPlaceholders: string) {
    const query = `
      SELECT database as schema_name,
            name as table_name
      FROM system.tables
      WHERE database IN (${schemasPlaceholders})
    `;
    return query;
  }

  protected override getColumnsForSpecificTablesQuery(conditionString: string) {
    const query = `
      SELECT name as ${this.quoteIdentifier('column_name')},
             table as ${this.quoteIdentifier('table_name')},
             database as ${this.quoteIdentifier('schema_name')},
             type as ${this.quoteIdentifier('data_type')}
      FROM system.columns
      WHERE ${conditionString}
    `;
    return query;
  }

  protected override getColumnNameForSchemaName() {
    return 'database';
  }

  protected override getColumnNameForTableName() {
    return 'table';
  }

  public override async getSchemas(): Promise<QuerySchemasResult[]> {
    return [{ schema_name: this.config.queryOptions.database }];
  }

  public async stream(
    query: string,
    values: unknown[],
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    { highWaterMark }: StreamOptions
  ): Promise<StreamTableDataWithTypes> {
    // eslint-disable-next-line no-underscore-dangle
    const conn = await (<any> this.pool)._factory.create();

    try {
      const formattedQuery = sqlstring.format(query, values);

      return await new Promise((resolve, reject) => {
        const options = {
          queryOptions: {
            query_id: uuidv4(),
            //
            //
            // If ClickHouse user's permissions are restricted with "readonly = 1",
            // change settings queries are not allowed. Thus, "join_use_nulls" setting
            // can not be changed
            //
            //
            ...(this.readOnlyMode ? {} : { join_use_nulls: 1 }),
          }
        };

        const originalStream = conn.query(formattedQuery, options, (err: Error | null, result: any) => {
          if (err) {
            reject(err);
          } else {
            const rowStream = new HydrationStream(result.meta);
            originalStream.pipe(rowStream);

            resolve({
              rowStream,
              types: result.meta.map((field: any) => ({
                name: field.name,
                type: this.toGenericType(field.type),
              })),
              release: async () => {
                // eslint-disable-next-line no-underscore-dangle
                await (<any> this.pool)._factory.destroy(conn);
              }
            });
          }
        });
      });
    } catch (e) {
      // eslint-disable-next-line no-underscore-dangle
      await (<any> this.pool)._factory.destroy(conn);

      throw e;
    }
  }

  public async downloadQueryResults(
    query: string,
    values: unknown[],
    options: DownloadQueryResultsOptions
  ): Promise<DownloadQueryResultsResult> {
    if ((options || {}).streamImport) {
      return this.stream(query, values, options);
    }

    const response = await this.queryResponse(query, values);

    return {
      rows: this.normaliseResponse(response),
      types: response.meta.map((field: any) => ({
        name: field.name,
        type: this.toGenericType(field.type),
      })),
    };
  }

  public toGenericType(columnType: string) {
    if (columnType.toLowerCase() in ClickhouseTypeToGeneric) {
      return ClickhouseTypeToGeneric[columnType.toLowerCase()];
    }

    /**
     * Example of types:
     *
     * Int64
     * Nullable(Int64) / Nullable(String)
     * Nullable(DateTime('UTC'))
     */
    if (columnType.includes('(')) {
      const types = columnType.toLowerCase().match(/([a-z0-9']+)/g);
      if (types) {
        for (const type of types) {
          if (type in ClickhouseTypeToGeneric) {
            return ClickhouseTypeToGeneric[type];
          }
        }
      }
    }

    return super.toGenericType(columnType);
  }

  public async createSchemaIfNotExists(schemaName: string): Promise<void> {
    await this.query(`CREATE DATABASE IF NOT EXISTS ${schemaName}`, []);
  }

  public getTablesQuery(schemaName: string) {
    return this.query('SELECT name as table_name FROM system.tables WHERE database = ?', [schemaName]);
  }

  public capabilities(): DriverCapabilities {
    return {
      incrementalSchemaLoading: true,
    };
  }
}
