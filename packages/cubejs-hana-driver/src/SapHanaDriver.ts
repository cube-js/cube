/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `SapHanaDriver` and related types declaration.
 */

import {
  getEnv,
  assertDataSource,
} from '@cubejs-backend/shared';
import genericPool from 'generic-pool';
import { promisify } from 'util';
import {
  BaseDriver,
  GenericDataBaseType,
  DriverInterface,
  DownloadQueryResultsOptions,
} from '@cubejs-backend/base-driver';
import { ConnectionOptions, Connection, FieldInfo } from 'types-hana-client';

const hdb = require('@sap/hana-client');

const TypeCode = require('@sap/hana-client/extension/TypeCode');

// convert HANA build-in types with type:type object
const HanaBuildInTypes: Record<string, string> = {};
Object.entries(TypeCode).forEach(([key, _]) => {
  HanaBuildInTypes[key] = key;
});

const SapHanaToGenericType: Record<string, GenericDataBaseType> = {
  smalldecimal: 'decimal',
  seconddate: 'timestamp',
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
      serverNode: getEnv('dbHost', { dataSource }),
      uid: getEnv('dbUser', { dataSource }),
      pwd: getEnv('dbPass', { dataSource }),
      encrypt: true,
      sslValidateCertificate: false,
      readOnly: true,
      ...restConfig,
    };
    this.pool = genericPool.createPool({
      create: async () => {
        const conn: any = hdb.createConnection();
        const connect = promisify(conn.connect.bind(conn));

        if (conn.on) {
          // there is no `on` method on HANA connection
          conn.on('error', () => {
            conn.destroy();
          });
        }
        conn.execute = promisify(conn.exec.bind(conn));

        await connect(this.config);

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

  protected async getConnectionFromPool() {
    return (<any> this.pool)._factory.create();
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
    const res = await conn.execute(query, values || {});
    return res;
  }

  public async queryResultSet(query: string, values: unknown[]) {
    const conn = await this.getConnectionFromPool();

    return conn.prepare(query).execQuery(values);
  }

  public async release() {
    await this.pool.drain();
    await this.pool.clear();
  }

  public informationSchemaQuery() {
    return `
      SELECT columns.COLUMN_NAME as ${this.quoteIdentifier('column_name')},
             columns.TABLE_NAME as ${this.quoteIdentifier('table_name')},
             columns.SCHEMA_NAME as ${this.quoteIdentifier('table_schema')},
             columns.DATA_TYPE_NAME as ${this.quoteIdentifier('data_type')}
      FROM SYS.TABLE_COLUMNS columns
      WHERE columns.SCHEMA_NAME = '${this.config.uid}'
   `;
  }

  public quoteIdentifier(identifier: string) {
    return `"${identifier}"`; 
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

  public async downloadQueryResults(
    query: string,
    values: unknown[],
    options: DownloadQueryResultsOptions
  ) {
    if (options.streamImport) {
      throw new Error('No support on the HANA stream yet');
    }

    const resultSet = await this.queryResultSet(query, values);
    const rows = [];
    while (resultSet.next()) {
      rows.push(resultSet.getValues());
    }

    return {
      rows,
      types: this.mapFieldsToGenericTypes(resultSet.getColumnInfo())
    };
  }

  protected mapFieldsToGenericTypes(fields: FieldInfo[]) {
    return fields.map((f) => {
      let hanaType = HanaBuildInTypes[f.nativeTypeName].toLowerCase();

      if (f.nativeTypeName.toLowerCase() in SapHanaToGenericType) {
        hanaType = SapHanaToGenericType[f.nativeTypeName.toLowerCase()];
      }

      if (!hanaType) {
        throw new Error(
          `Unable to detect type for field "${f.columnName}" with dataTypeID: ${f.nativeTypeName}`
        );
      }

      return ({
        name: f.columnName,
        type: this.toGenericType(hanaType)
      });
    });
  }

  public toGenericType(columnType: string) {
    return SapHanaToGenericType[columnType.toLowerCase()] ||
      SapHanaToGenericType[columnType.toLowerCase().split('(')[0]] ||
      super.toGenericType(columnType);
  }
}
