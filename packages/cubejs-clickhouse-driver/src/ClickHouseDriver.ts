/* eslint-disable no-restricted-syntax */
import {
  BaseDriver,
  DownloadQueryResultsOptions,
  DownloadQueryResultsResult,
  DriverInterface,
  StreamOptions,
  StreamTableDataWithTypes,
} from '@cubejs-backend/base-driver';
import { getEnv } from '@cubejs-backend/shared';
import genericPool, { Pool } from 'generic-pool';
import { v4 as uuidv4 } from 'uuid';
import sqlstring from 'sqlstring';
import { HydrationStream } from './HydrationStream';

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
  maxPoolSize?: number,
}

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

  public constructor(config: ClickHouseDriverOptions = {}) {
    super();

    this.config = {
      host: process.env.CUBEJS_DB_HOST,
      port: process.env.CUBEJS_DB_PORT,
      auth: process.env.CUBEJS_DB_USER || process.env.CUBEJS_DB_PASS ? `${process.env.CUBEJS_DB_USER}:${process.env.CUBEJS_DB_PASS}` : '',
      protocol: getEnv('dbSsl') ? 'https:' : 'http:',
      queryOptions: {
        database: process.env.CUBEJS_DB_NAME || config && config.database || 'default'
      },
      ...config
    };
    this.readOnlyMode = process.env.CUBEJS_DB_CLICKHOUSE_READONLY === 'true';
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
      max: this.config.maxPoolSize || 8,
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
    return !!this.config.readOnly || this.readOnlyMode;
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
    //
    //
    //  ClickHouse returns DateTime as strings in format "YYYY-DD-MM HH:MM:SS"
    //  cube.js expects them in format "YYYY-DD-MMTHH:MM:SS.000", so translate them based on the metadata returned
    //
    //  ClickHouse returns some number types as js numbers, others as js string, normalise them all to strings
    //
    //
    if (res.data) {
      res.data.forEach((row: any) => {
        Object.keys(row).forEach(field => {
          const value = row[field];
          if (value !== null) {
            const meta = res.meta.find((m: any) => m.name === field);
            if (meta.type.includes('DateTime')) {
              row[field] = `${value.substring(0, 10)}T${value.substring(11, 22)}.000`;
            } else if (meta.type.includes('Date')) {
              row[field] = `${value}T00:00:00.000`;
            } else if (meta.type.includes('Int') || meta.type.includes('Float') || meta.type.includes('Decimal')) {
              // convert all numbers into strings
              row[field] = `${value}`;
            }
          }
        });
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
      const types = columnType.toLowerCase().match(/([a-z']+)/g);
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

  public async createSchemaIfNotExists(schemaName: string): Promise<unknown[]> {
    return this.query(`CREATE DATABASE IF NOT EXISTS ${schemaName}`, []);
  }

  public getTablesQuery(schemaName: string) {
    return this.query('SELECT name as table_name FROM system.tables WHERE database = ?', [schemaName]);
  }
}
