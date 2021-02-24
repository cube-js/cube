import { BaseDriver } from '@cubejs-backend/query-orchestrator';
import genericPool, { Pool } from 'generic-pool';

import { CubeStoreQuery } from './CubeStoreQuery';
import { AsyncConnection, createConnection } from './connection';
import { ConnectionConfig } from './types';

const GenericTypeToCubeStore: Record<string, string> = {
  string: 'varchar(255)',
  text: 'varchar(255)'
};

export class CubeStoreDriver extends BaseDriver {
  protected readonly config: any;

  protected readonly pool: Pool<AsyncConnection>;

  public constructor(config?: Partial<ConnectionConfig>) {
    super();
    const { pool, ...restConfig } = config || {};

    this.config = {
      host: process.env.CUBEJS_DB_HOST,
      database: process.env.CUBEJS_DB_NAME,
      port: process.env.CUBEJS_DB_PORT,
      user: process.env.CUBEJS_DB_USER,
      password: process.env.CUBEJS_DB_PASS,
      socketPath: process.env.CUBEJS_DB_SOCKET_PATH,
      timezone: 'Z',
      ...restConfig,
    };
    this.pool = genericPool.createPool<AsyncConnection>({
      create: async () => createConnection(this.config),
      destroy: async (connection) => connection.close(),
      validate: async (connection) => {
        try {
          await connection.execute('SELECT 1');
        } catch (e) {
          return false;
        }
        return true;
      }
    }, {
      min: 0,
      max: process.env.CUBEJS_DB_MAX_POOL && parseInt(process.env.CUBEJS_DB_MAX_POOL, 10) || 8,
      evictionRunIntervalMillis: 10000,
      softIdleTimeoutMillis: 30000,
      idleTimeoutMillis: 30000,
      testOnBorrow: true,
      acquireTimeoutMillis: 20000,
      ...pool
    });
  }

  public withConnection(fn: (connection: AsyncConnection) => Promise<unknown>) {
    const self = this;
    const connectionPromise = this.pool.acquire();

    let cancelled = false;
    const cancelObj: any = {};
    const promise = connectionPromise.then(async conn => {
      const [{ connectionId }]: any = await conn.execute('select connection_id() as connectionId');
      cancelObj.cancel = async () => {
        cancelled = true;
        await self.withConnection(async processConnection => {
          await processConnection.execute(`KILL ${connectionId}`);
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
    (<any>promise).cancel = () => cancelObj.cancel();
    return promise;
  }

  public async testConnection() {
    // @ts-ignore
    // eslint-disable-next-line no-underscore-dangle
    const conn = await this.pool._factory.create();

    try {
      return await conn.execute('SELECT 1');
    } finally {
      // @ts-ignore
      // eslint-disable-next-line no-underscore-dangle
      await this.pool._factory.destroy(conn);
    }
  }

  // @ts-ignore
  public async query(query, values) {
    // @ts-ignore I am not able to resolve it quick, @todo fixit!
    return this.withConnection(db => db.execute(query, values));
  }

  public async release() {
    await this.pool.drain();
    await this.pool.clear();
  }

  public informationSchemaQuery() {
    return `${super.informationSchemaQuery()} AND columns.table_schema = '${this.config.database}'`;
  }

  public quoteIdentifier(identifier: string): string {
    return `\`${identifier}\``;
  }

  public fromGenericType(columnType: string): string {
    return GenericTypeToCubeStore[columnType] || super.fromGenericType(columnType);
  }

  public toColumnValue(value: any, genericType: any) {
    if (genericType === 'timestamp' && typeof value === 'string') {
      return value && value.replace('Z', '');
    }
    if (genericType === 'boolean' && typeof value === 'string') {
      if (value.toLowerCase() === 'true') {
        return true;
      }
      if (value.toLowerCase() === 'false') {
        return false;
      }
    }
    return super.toColumnValue(value, genericType);
  }

  public async uploadTableWithIndexes(table: any, columns: any, tableData: any, indexesSql: any) {
    if (tableData.csvFile) {
      const files = Array.isArray(tableData.csvFile) ? tableData.csvFile : [tableData.csvFile];
      const createTableSql = this.createTableSql(table, columns);
      const indexes =
        indexesSql.map((s: any) => s.sql[0].replace(/^CREATE INDEX (.*?) ON (.*?) \((.*)$/, 'INDEX $1 ($3')).join(' ');
      // eslint-disable-next-line no-unused-vars
      const createTableSqlWithLocation = `${createTableSql} ${indexes} LOCATION ${files.map(() => '?').join(', ')}`;
      await this.query(createTableSqlWithLocation, files).catch(e => {
        e.message = `Error during create table: ${createTableSqlWithLocation}: ${e.message}`;
        throw e;
      });
      return;
    }
    if (!tableData.rows) {
      throw new Error(`${this.constructor} driver supports only rows and CSV upload`);
    }
    await this.createTable(table, columns);
    try {
      for (let i = 0; i < indexesSql.length; i++) {
        const [query, params] = indexesSql[i].sql;
        await this.query(query, params);
      }
      const batchSize = 2000; // TODO make dynamic?
      for (let j = 0; j < Math.ceil(tableData.rows.length / batchSize); j++) {
        const currentBatchSize = Math.min(tableData.rows.length - j * batchSize, batchSize);
        const indexArray = Array.from({ length: currentBatchSize }, (v, i) => i);
        const valueParamPlaceholders =
          indexArray.map(i => `(${columns.map((c, paramIndex) => this.param(paramIndex + i * columns.length)).join(', ')})`).join(', ');
        const params = indexArray.map(i => columns
          .map(c => this.toColumnValue(tableData.rows[i + j * batchSize][c.name], c.type)))
          .reduce((a, b) => a.concat(b), []);

        await this.query(
          `INSERT INTO ${table}
        (${columns.map(c => this.quoteIdentifier(c.name)).join(', ')})
        VALUES ${valueParamPlaceholders}`,
          params
        );
      }
    } catch (e) {
      await this.dropTable(table);
      throw e;
    }
  }

  public static dialectClass() {
    return CubeStoreQuery;
  }

  public capabilities() {
    return {
      csvImport: true
    };
  }
}
