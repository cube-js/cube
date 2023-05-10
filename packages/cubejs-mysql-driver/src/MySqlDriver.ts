import mysql, { Connection, ConnectionConfig, FieldInfo, QueryOptions } from 'mysql';
import genericPool from 'generic-pool';
import { promisify } from 'util';
import {
  BaseDriver,
  GenericDataBaseType,
  DriverInterface,
  StreamOptions,
  DownloadQueryResultsOptions, TableStructure, DownloadTableData, IndexesSQL, DownloadTableMemoryData,
} from '@cubejs-backend/base-driver';

const GenericTypeToMySql: Record<GenericDataBaseType, string> = {
  string: 'varchar(255) CHARACTER SET utf8mb4',
  text: 'varchar(255) CHARACTER SET utf8mb4',
  decimal: 'decimal(38,10)',
};

/**
 * MySQL Native types -> SQL type
 * @link https://github.com/mysqljs/mysql/blob/master/lib/protocol/constants/types.js#L9
 */
const MySqlNativeToMySqlType = {
  [mysql.Types.DECIMAL]: 'decimal',
  [mysql.Types.NEWDECIMAL]: 'decimal',
  [mysql.Types.TINY]: 'tinyint',
  [mysql.Types.SHORT]: 'smallint',
  [mysql.Types.LONG]: 'int',
  [mysql.Types.INT24]: 'mediumint',
  [mysql.Types.LONGLONG]: 'bigint',
  [mysql.Types.NEWDATE]: 'datetime',
  [mysql.Types.TIMESTAMP2]: 'timestamp',
  [mysql.Types.DATETIME2]: 'datetime',
  [mysql.Types.TIME2]: 'time',
  [mysql.Types.TINY_BLOB]: 'tinytext',
  [mysql.Types.MEDIUM_BLOB]: 'mediumtext',
  [mysql.Types.LONG_BLOB]: 'longtext',
  [mysql.Types.BLOB]: 'text',
  [mysql.Types.VAR_STRING]: 'varchar',
  [mysql.Types.STRING]: 'varchar',
};

const MySqlToGenericType: Record<string, GenericDataBaseType> = {
  mediumtext: 'text',
  longtext: 'text',
  mediumint: 'int',
  smallint: 'int',
  bigint: 'int',
  tinyint: 'int',
  'mediumint unsigned': 'int',
  'smallint unsigned': 'int',
  'bigint unsigned': 'int',
  'tinyint unsigned': 'int',
};

export interface MySqlDriverConfiguration extends ConnectionConfig {
  readOnly?: boolean,
  loadPreAggregationWithoutMetaLock?: boolean,
  storeTimezone?: string,
  maxPoolSize?: number,
  pool?: any,
}

interface MySQLConnection extends Connection {
  execute: (options: string | QueryOptions, values?: any) => Promise<any>
}

export class MySqlDriver extends BaseDriver implements DriverInterface {
  /**
   * Returns default concurrency value.
   */
  public static getDefaultConcurrency(): number {
    return 2;
  }

  protected readonly config: MySqlDriverConfiguration;

  protected readonly pool: genericPool.Pool<MySQLConnection>;

  public constructor(config: MySqlDriverConfiguration = {}) {
    super();

    const { pool, ...restConfig } = config;

    this.config = {
      host: process.env.CUBEJS_DB_HOST,
      database: process.env.CUBEJS_DB_NAME,
      port: <any>process.env.CUBEJS_DB_PORT,
      user: process.env.CUBEJS_DB_USER,
      password: process.env.CUBEJS_DB_PASS,
      socketPath: process.env.CUBEJS_DB_SOCKET_PATH,
      timezone: 'Z',
      ssl: this.getSslOptions(),
      dateStrings: true,
      readOnly: true,
      ...restConfig,
    };

    this.pool = genericPool.createPool({
      create: async () => {
        const conn: any = mysql.createConnection(this.config);
        const connect = promisify(conn.connect.bind(conn));

        if (conn.on) {
          conn.on('error', () => {
            conn.destroy();
          });
        }
        conn.execute = promisify(conn.query.bind(conn));

        await connect();

        return conn;
      },
      validate: async (connection) => {
        try {
          await connection.execute('SELECT 1');
        } catch (e) {
          this.databasePoolError(e);
          return false;
        }
        return true;
      },
      destroy: (connection) => promisify(connection.end.bind(connection))(),
    }, {
      min: 0,
      max:
        process.env.CUBEJS_DB_MAX_POOL && parseInt(process.env.CUBEJS_DB_MAX_POOL, 10) ||
        config.maxPoolSize || 8,
      evictionRunIntervalMillis: 10000,
      softIdleTimeoutMillis: 30000,
      idleTimeoutMillis: 30000,
      testOnBorrow: true,
      acquireTimeoutMillis: 20000,
      ...pool
    });
  }

  public readOnly() {
    return !!this.config.readOnly;
  }

  protected withConnection(fn: (conn: MySQLConnection) => Promise<any>) {
    const self = this;
    const connectionPromise = this.pool.acquire();

    let cancelled = false;
    const cancelObj: any = {};

    const promise: any = connectionPromise.then(async conn => {
      const [{ connectionId }] = await conn.execute('select connection_id() as connectionId');
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
    promise.cancel = () => cancelObj.cancel();
    return promise;
  }

  public async testConnection() {
    // eslint-disable-next-line no-underscore-dangle
    const conn: MySQLConnection = await (<any> this.pool)._factory.create();

    try {
      return await conn.execute('SELECT 1');
    } finally {
      // eslint-disable-next-line no-underscore-dangle
      await (<any> this.pool)._factory.destroy(conn);
    }
  }

  public async query(query: string, values: unknown[]) {
    return this.withConnection(async (conn) => {
      await this.setTimeZone(conn);

      return conn.execute(query, values);
    });
  }

  protected setTimeZone(conn: MySQLConnection) {
    return conn.execute(`SET time_zone = '${this.config.storeTimezone || '+00:00'}'`, []);
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

  public fromGenericType(columnType: GenericDataBaseType) {
    return GenericTypeToMySql[columnType] || super.fromGenericType(columnType);
  }

  public loadPreAggregationIntoTable(preAggregationTableName: string, loadSql: any, params: any, tx: any) {
    if (this.config.loadPreAggregationWithoutMetaLock) {
      return this.cancelCombinator(async (saveCancelFn: any) => {
        await saveCancelFn(this.query(`${loadSql} LIMIT 0`, params));
        await saveCancelFn(this.query(loadSql.replace(/^CREATE TABLE (\S+) AS/i, 'INSERT INTO $1'), params));
      });
    }

    return super.loadPreAggregationIntoTable(preAggregationTableName, loadSql, params, tx);
  }

  public async stream(query: string, values: unknown[], { highWaterMark }: StreamOptions) {
    // eslint-disable-next-line no-underscore-dangle
    const conn: MySQLConnection = await (<any> this.pool)._factory.create();

    try {
      await this.setTimeZone(conn);

      const [rowStream, fields] = await (
        new Promise<[any, mysql.FieldInfo[]]>((resolve, reject) => {
          const stream = conn.query(query, values).stream({ highWaterMark });

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
        types: this.mapFieldsToGenericTypes(fields),
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

  protected mapFieldsToGenericTypes(fields: mysql.FieldInfo[]) {
    return fields.map((field) => {
      // @ts-ignore
      let dbType = mysql.Types[field.type];

      if (field.type in MySqlNativeToMySqlType) {
        // @ts-ignore
        dbType = MySqlNativeToMySqlType[field.type];
      }

      return {
        name: field.name,
        type: this.toGenericType(dbType)
      };
    });
  }

  public async downloadQueryResults(query: string, values: unknown[], options: DownloadQueryResultsOptions) {
    if ((options || {}).streamImport) {
      return this.stream(query, values, options);
    }

    return this.withConnection(async (conn) => {
      await this.setTimeZone(conn);

      return new Promise((resolve, reject) => {
        conn.query(query, values, (err, rows, fields) => {
          if (err) {
            reject(err);
          } else {
            resolve({
              rows,
              types: this.mapFieldsToGenericTypes(<FieldInfo[]>fields),
            });
          }
        });
      });
    });
  }

  public toColumnValue(value: any, genericType: GenericDataBaseType) {
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

  protected isDownloadTableDataRow(tableData: DownloadTableData): tableData is DownloadTableMemoryData {
    return (<DownloadTableMemoryData> tableData).rows !== undefined;
  }

  public async uploadTableWithIndexes(
    table: string,
    columns: TableStructure,
    tableData: DownloadTableData,
    indexesSql: IndexesSQL
  ) {
    if (!this.isDownloadTableDataRow(tableData)) {
      throw new Error(`${this.constructor} driver supports only rows upload`);
    }

    await this.createTable(table, columns);

    try {
      const batchSize = 1000; // TODO make dynamic?
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

      for (let i = 0; i < indexesSql.length; i++) {
        const [query, p] = indexesSql[i].sql;
        await this.query(query, p);
      }
    } catch (e) {
      await this.dropTable(table);
      throw e;
    }
  }

  public toGenericType(columnType: string) {
    return MySqlToGenericType[columnType.toLowerCase()] ||
      MySqlToGenericType[columnType.toLowerCase().split('(')[0]] ||
      super.toGenericType(columnType);
  }
}
