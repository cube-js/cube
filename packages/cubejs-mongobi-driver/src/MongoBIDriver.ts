import { createConnection, Connection, ConnectionOptions, RowDataPacket, Field } from 'mysql2';
import genericPool, { Pool } from 'generic-pool';
import {
  BaseDriver, DownloadQueryResultsOptions,
  DownloadQueryResultsResult,
  DriverInterface, StreamOptions,
} from '@cubejs-backend/base-driver';
import { Readable } from 'stream';

import { getNativeTypeName } from './MySQLType';

export interface MongoBIDriverConfiguration extends ConnectionOptions {
  storeTimezone?: string;
  maxPoolSize?: number;
}

export class MongoBIDriver extends BaseDriver implements DriverInterface {
  /**
   * Returns default concurrency value.
   */
  public static getDefaultConcurrency(): number {
    return 2;
  }

  protected readonly config: MongoBIDriverConfiguration;

  protected readonly pool: Pool<Connection>;

  public constructor(config: MongoBIDriverConfiguration = {}) {
    super();

    this.config = {
      host: process.env.CUBEJS_DB_HOST,
      database: process.env.CUBEJS_DB_NAME,
      port: <any>process.env.CUBEJS_DB_PORT,
      user: process.env.CUBEJS_DB_USER,
      password: process.env.CUBEJS_DB_PASS,
      ssl: this.getSslOptions(),
      authPlugins: {
        mysql_clear_password: () => async () => {
          const password = config.password || process.env.CUBEJS_DB_PASS || '';
          return Buffer.from((password).concat('\0')).toString();
        }
      },
      typeCast: (field: Field, next) => {
        if (field.type === 'DATETIME') {
          // Example value 1998-08-02 00:00:00
          // Here we just omit Date parsing and avoiding Date.toString() done by driver. MongoBI original format is just fine.
          return field.string();
        }

        return next();
      },
      ...config
    };
    this.pool = genericPool.createPool({
      create: async () => {
        const conn: Connection = createConnection(this.config);

        if (conn.on) {
          conn.on('error', () => {
            conn.destroy();
          });
        }

        await conn.promise().connect();

        return conn;
      },
      destroy: async (connection) => connection.promise().end(),
      validate: async (connection) => {
        try {
          await connection.promise().query({
            sql: 'SELECT 1',
          });
        } catch (e) {
          this.databasePoolError(e);
          return false;
        }

        return true;
      }
    }, {
      min: 0,
      max: this.config.maxPoolSize || 8,
      evictionRunIntervalMillis: 10000,
      softIdleTimeoutMillis: 30000,
      idleTimeoutMillis: 30000,
      testOnBorrow: true,
      acquireTimeoutMillis: 20000
    });
  }

  protected withConnection<T>(fn: (conn: Connection) => Promise<T>): Promise<T> {
    const self = this;
    const connectionPromise = this.pool.acquire();

    let cancelled = false;
    const cancelObj: any = {};
    const promise: any = connectionPromise.then(conn => {
      cancelObj.cancel = async () => {
        cancelled = true;
        await self.withConnection(async processConnection => {
          const processRows: any = await processConnection.promise().query({
            sql: 'SHOW PROCESSLIST'
          });
          await Promise.all(processRows.filter((row: any) => row.Time >= 599)
            .map((row: any) => processConnection.promise().query({
              sql: `KILL ${row.Id}`
            })));
        });
      };
      return fn(conn)
        .then(res => this.pool.release(conn).then(() => {
          if (cancelled) {
            throw new Error('Query cancelled');
          }
          return res;
        }))
        .catch((err) => this.pool.release(conn).then(() => {
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
    // eslint-disable-next-line no-underscore-dangle
    const conn: Connection = await (<any> this.pool)._factory.create();
    try {
      await conn.promise().query('SELECT 1', []);
    } finally {
      // eslint-disable-next-line no-underscore-dangle
      await (<any> this.pool)._factory.destroy(conn);
    }
  }

  public async query<R = unknown>(sql: string, values: unknown[]): Promise<R[]> {
    return <any> this.withConnection(async (conn) => {
      await this.prepareConnection(conn);

      const [rows] = await conn.promise().query({
        sql,
        values,
      });

      return rows;
    });
  }

  protected async prepareConnection(conn: Connection) {
    await conn.promise().query({
      sql: `SET time_zone = '${this.config.storeTimezone || '+00:00'}'`,
      values: [],
    });
  }

  public async stream(query: string, values: unknown[], options: StreamOptions): Promise<any> {
    // eslint-disable-next-line no-underscore-dangle
    const conn: Connection = await (<any> this.pool)._factory.create();

    try {
      await this.prepareConnection(conn);

      const [rowStream, fields] = await (
        new Promise<[Readable, any[]]>((resolve, reject) => {
          const stream = conn.query(query, values).stream(options);

          stream.on('fields', (f) => {
            resolve([stream, f]);
          });
          stream.on('error', (e) => {
            reject(e);
          });
        })
      );

      return {
        rowStream,
        types: fields.map((field: any) => ({
          name: field.name,
          type: this.toGenericType(getNativeTypeName(field.columnType)),
        })),
        release: async () => {
          // eslint-disable-next-line no-underscore-dangle
          await (<any> this.pool)._factory.destroy(conn);
        }
      };
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

    return this.withConnection(async (conn) => {
      await this.prepareConnection(conn);

      const [rows, fields] = await conn.promise().query<RowDataPacket[]>(query, values);

      return {
        rows,
        types: fields.map((field: any) => ({
          name: field.name,
          type: this.toGenericType(getNativeTypeName(field.columnType)),
        })),
      };
    });
  }

  public async release() {
    await this.pool.drain();
    await this.pool.clear();
  }

  public informationSchemaQuery() {
    if (this.config.database) {
      return `${super.informationSchemaQuery()} AND columns.table_schema = '${this.config.database}'`;
    } else {
      return super.informationSchemaQuery();
    }
  }

  public quoteIdentifier(identifier: string) {
    return `\`${identifier}\``;
  }

  public readOnly() {
    // Mongo BI doesn't support table creation
    return true;
  }
}
