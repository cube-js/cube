import { createConnection, Connection, ConnectionOptions, RowDataPacket } from 'mysql2/promise';
import {} from 'mysql2/'
import genericPool, { Pool } from 'generic-pool';
import {
  BaseDriver, DownloadQueryResultsOptions,
  DownloadQueryResultsResult,
  DriverInterface,
} from '@cubejs-backend/query-orchestrator';
import { type } from 'ramda';
import { getNativeTypeName } from './MySQLType';

export interface MongoBIDriverConfiguration extends ConnectionOptions {
  storeTimezone?: string;
}

export class MongoBIDriver extends BaseDriver implements DriverInterface {
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
      ...config
    };
    this.pool = genericPool.createPool({
      create: async () => {
        const conn = await createConnection(this.config);

        if (conn.on) {
          conn.on('error', () => {
            conn.destroy();
          });
        }

        return conn;
      },
      destroy: async (connection) => connection.end(),
      validate: async (connection) => {
        try {
          await connection.query({
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
      max: 8,
      evictionRunIntervalMillis: 10000,
      softIdleTimeoutMillis: 30000,
      idleTimeoutMillis: 30000,
      testOnBorrow: true,
      acquireTimeoutMillis: 20000
    });
  }

  withConnection<T>(fn: (conn: Connection) => Promise<T>): Promise<T> {
    const self = this;
    const connectionPromise = this.pool.acquire();

    let cancelled = false;
    const cancelObj: any = {};
    const promise: any = connectionPromise.then(conn => {
      cancelObj.cancel = async () => {
        cancelled = true;
        await self.withConnection(async processConnection => {
          const processRows: any = await processConnection.query({
            sql: 'SHOW PROCESSLIST'
          });
          await Promise.all(processRows.filter((row: any) => row.Time >= 599)
            .map((row: any) => processConnection.query({
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

  async testConnection() {
    // eslint-disable-next-line no-underscore-dangle
    const conn: Connection = await (<any> this.pool)._factory.create();
    try {
      await conn.query('SELECT 1');
    } finally {
      // eslint-disable-next-line no-underscore-dangle
      await (<any> this.pool)._factory.destroy(conn);
    }
  }

  public async query<R = unknown>(query: string, values: unknown[]): Promise<R[]> {
    return <any> this.withConnection(async (conn) => {
      await this.prepareConnection(conn);

      const [rows] = await conn.query({
        sql: query,
        values,
      });

      return rows;
    });
  }

  protected async prepareConnection(conn: Connection) {
    await conn.query({
      sql: `SET time_zone = '${this.config.storeTimezone || '+00:00'}'`,
      values: [],
    });
  }

  public async downloadQueryResults(
    query: string,
    values: unknown[],
    options: DownloadQueryResultsOptions
  ): Promise<DownloadQueryResultsResult> {
    return this.withConnection(async (conn) => {
      await this.prepareConnection(conn);

      const [rows, fields] = await conn.query<RowDataPacket[]>(query, values);

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
    return `${super.informationSchemaQuery()} AND columns.table_schema = '${this.config.database}'`;
  }

  public quoteIdentifier(identifier: string) {
    return `\`${identifier}\``;
  }

  public readOnly() {
    return true;
  }
}
