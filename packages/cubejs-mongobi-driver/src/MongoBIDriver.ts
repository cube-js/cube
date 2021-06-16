import mysql, { Connection, ConnectionOptions } from 'mysql2';
import genericPool, { Pool } from 'generic-pool';
import { promisify } from 'util';
import { BaseDriver, DriverInterface } from '@cubejs-backend/query-orchestrator';
import { ResolveAwait } from '@cubejs-backend/shared';

export interface MongoBIDriverConfiguration extends ConnectionOptions {
  storeTimezone?: string;
}

async function createConnection(config: MongoBIDriverConfiguration) {
  const conn = mysql.createConnection(config);
  const connect = promisify(conn.connect.bind(conn));

  if (conn.on) {
    conn.on('error', () => {
      conn.destroy();
    });
  }

  await connect();

  return {
    ...conn,
    execute: promisify(conn.query).bind(conn),
    close: promisify(conn.end).bind(conn)
  };
}

type MySQLConnection = ResolveAwait<ReturnType<typeof createConnection>>;

export class MongoBIDriver extends BaseDriver implements DriverInterface {
  protected readonly config: MongoBIDriverConfiguration;

  protected readonly pool: Pool<MySQLConnection>;

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
      create: async () => createConnection(this.config),
      destroy: async (connection) => {
        await connection.close();
      },
      validate: async (connection) => {
        try {
          await connection.execute({
            sql: 'SELECT 1',
          });
        } catch (e) {
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

  withConnection<T>(fn: (conn: MySQLConnection) => Promise<T>): Promise<T> {
    const self = this;
    const connectionPromise = this.pool.acquire();

    let cancelled = false;
    const cancelObj: any = {};
    const promise: any = connectionPromise.then(conn => {
      cancelObj.cancel = async () => {
        cancelled = true;
        await self.withConnection(async processConnection => {
          const processRows: any = await processConnection.execute({
            sql: 'SHOW PROCESSLIST'
          });
          await Promise.all(processRows.filter((row: any) => row.Time >= 599)
            .map((row: any) => processConnection.execute({
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
    const conn = await (<any> this.pool)._factory.create();
    try {
      return await conn.execute('SELECT 1');
    } finally {
      // eslint-disable-next-line no-underscore-dangle
      await (<any> this.pool)._factory.destroy(conn);
    }
  }

  public async query<R = unknown>(query: string, values: unknown[]): Promise<R[]> {
    const self = this;

    return <any> this.withConnection(async (conn) => {
      await conn.execute({
        sql: `SET time_zone = '${self.config.storeTimezone || '+00:00'}'`,
        values: [],
      });

      return conn.execute({
        sql: query,
        values,
      });
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
