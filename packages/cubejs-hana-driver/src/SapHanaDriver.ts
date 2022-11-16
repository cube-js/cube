/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `SapHanaDriver` and related types declaration.
 */

import {
  getEnv,
  assertDataSource,
} from '@cubejs-backend/shared';
// import hana, { Connection, ConnectionConfig, FieldInfo, QueryOptions } from '@sap/hana-client';

import genericPool from 'generic-pool';
import { promisify } from 'util';
import {
  BaseDriver,
  GenericDataBaseType,
  DriverInterface,
  StreamOptions,
  DownloadQueryResultsOptions,
  TableStructure,
  DownloadTableData,
  IndexesSQL,
  DownloadTableMemoryData,
  StreamTableDataWithTypes,
} from '@cubejs-backend/base-driver';

import { ConnectionOptions, Connection } from 'types-hana-client'

const hdb = require('@sap/hana-client')

const GenericTypeToSapHana: Record<GenericDataBaseType, string> = {
  string: 'varchar(255)',
  text: 'varchar(255)',
  decimal: 'decimal(38,10)',
};

/**
 * HANA Native types -> SQL type
 */
const SapHanaNativeToSapHanaType: Record<GenericDataBaseType, string> = {
  string: 'text',
  double: 'decimal'
};

const SapHanaToGenericType: Record<string, GenericDataBaseType> = {
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

export interface SapHanaDriverConfiguration extends ConnectionOptions{
  readOnly?: boolean,
  loadPreAggregationWithoutMetaLock?: boolean,
  storeTimezone?: string,
  pool?: any,
}

interface SapHanaConnection extends Connection {
  execute: (options: string, values?: any) => Promise<any>
}

/**
 * SAP HANA driver class.
 */
export class SapHanaDriver extends BaseDriver implements DriverInterface {
  /**
   * Returns default concurrency value.
   */
  public static getDefaultConcurrency(): number {
    return 2;
  }

  protected readonly config: SapHanaDriverConfiguration;

  protected pool: genericPool.Pool<SapHanaConnection>;

  protected hdb: any;

  /**
   * Class constructor.
   */
  public constructor(
    config: SapHanaDriverConfiguration & {
      dataSource?: string,
      maxPoolSize?: number,
    } = {}
  ) {
    super();

    const dataSource =
      config.dataSource ||
      assertDataSource('default');

    this.hdb = hdb;

    const { pool, ...restConfig } = config;
    this.config = {
      host: getEnv('dbHost', { dataSource }),
      databaseName: getEnv('dbName', { dataSource }),
      port: getEnv('dbPort', { dataSource }),
      uid: getEnv('dbUser', { dataSource }),
      pwd: getEnv('dbPass', { dataSource }),
      // ssl: this.getSslOptions(dataSource),
      // dateStrings: true,
      readOnly: true,
      ...restConfig,
    };
    this.pool = genericPool.createPool({
      create: async () => {
        const conn: any = hdb.createConnection(this.config);
        const connect = promisify(conn.connect.bind(conn));

        if (conn.on) {
          // there is no `on` method on HANA connection
          conn.on('error', () => {
            conn.destroy();
          });
        }
        conn.execute = promisify(conn.exec.bind(conn));

        await connect();

        return conn;
      },
      validate: async (connection) => {
        try {
          await connection.execute('SELECT 1 FROM DUMMY');
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
        config.maxPoolSize ||
        getEnv('dbMaxPoolSize', { dataSource }) ||
        8,
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

  protected withConnection(fn: (conn: SapHanaConnection) => Promise<any>) {
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

  async getConnectionFromPool() {
    return await (<any> this.pool)._factory.create();
  }

  public async testConnection() {
    // eslint-disable-next-line no-underscore-dangle
    const conn: SapHanaConnection = await this.getConnectionFromPool();

    try {
      return await conn.execute('SELECT 1 FROM DUMMY');
    } finally {
      // eslint-disable-next-line no-underscore-dangle
      await (<any> this.pool)._factory.destroy(conn);
    }
  }

  public async query(query: string, values: unknown[]) {
    const conn = await this.getConnectionFromPool();
    return new Promise(
      (resolve, reject) => conn.exec(
        query,
        values || [],
        (err, result) => (err ? reject(err) : resolve(result))
      )
    );
  }

  protected setTimeZone(conn: SapHanaConnection) {
    return conn.execute(`SET time_zone = '${this.config.storeTimezone || '+00:00'}'`, []);
  }

  public async release() {
    await this.pool.drain();
    await this.pool.clear();
  }

  public informationSchemaQuery() {
    // return `${super.informationSchemaQuery()} AND columns.table_schema = '${this.config.database}'`;
    return `
      SELECT columns.COLUMN_NAME as ${this.quoteIdentifier('COLUMN_NAME')},
             columns.TABLE_NAME as ${this.quoteIdentifier('TABLE_NAME')},
             columns.SCHEMA_NAME as ${this.quoteIdentifier('SCHEMA_NAME')},
             columns.DATA_TYPE_NAME as ${this.quoteIdentifier('DATA_TYPE_NAME')}
      FROM SYS.TABLE_COLUMNS columns
      WHERE columns.table_schema IN '${this.config.databaseName}'
   `;
  }

  public quoteIdentifier(identifier: string) {
    return `\"${identifier}\"`;
  }

  public fromGenericType(columnType: GenericDataBaseType) {
    return GenericTypeToSapHana[columnType] || super.fromGenericType(columnType);
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

  public async stream(
    query: string,
    values: unknown[],
    { highWaterMark }: StreamOptions
  ): Promise<StreamTableDataWithTypes> {
    // eslint-disable-next-line no-underscore-dangle
    const conn: SapHanaConnection = await (<any> this.pool)._factory.create();

    try {
      // await this.setTimeZone(conn);

      const [rowStream, fields] = await (
        new Promise<[any, FieldInfo[]]>((resolve, reject) => {
          const stream = conn.exec(query, values).stream({ highWaterMark });

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

  protected mapFieldsToGenericTypes(fields: FieldInfo[]) {
    return fields.map((field) => {
      // @ts-ignore
      let dbType = hana.Types[field.type];

      if (field.type in SapHanaNativeToSapHanaType) {
        // @ts-ignore
        dbType = SapHanaNativeToSapHanaType[field.type];
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
        conn.exec(query, values, (err, result) => {
          if (err) {
            reject(err);
          } else {
            resolve({
              result,
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
    return SapHanaToGenericType[columnType.toLowerCase()] ||
      SapHanaToGenericType[columnType.toLowerCase().split('(')[0]] ||
      super.toGenericType(columnType);
  }
}
