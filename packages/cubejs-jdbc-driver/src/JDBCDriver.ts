/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `JDBCDriver` and related types declaration.
 */

import {
  getEnv,
  assertDataSource,
  CancelablePromise,
  format,
  Pool,
} from '@cubejs-backend/shared';
import {
  BaseDriver,
  createPoolName,
  DownloadQueryResultsOptions,
  DownloadQueryResultsResult,
  StreamOptions,
} from '@cubejs-backend/base-driver';
import { promisify } from 'util';
import path from 'path';

import { SupportedDrivers } from './supported-drivers';
import type { DriverOptionsInterface, EscapeDialect } from './supported-drivers';
import type { JDBCDriverConfiguration } from './types';
import { QueryStream } from './QueryStream';
import type { NextBatchFn, Row } from './QueryStream';

/* eslint-disable no-restricted-syntax,import/no-extraneous-dependencies */
const DriverManager = require('@cubejs-backend/jdbc/lib/drivermanager');
const Connection = require('@cubejs-backend/jdbc/lib/connection');
const DatabaseMetaData = require('@cubejs-backend/jdbc/lib/databasemetadata');
const jinst = require('@cubejs-backend/jdbc/lib/jinst');
const mvn = require('@cubejs-backend/node-java-maven');

let mvnPromise: Promise<void> | null = null;

const initMvn = (customClassPath: any) => {
  if (!mvnPromise) {
    mvnPromise = new Promise((resolve, reject) => {
      const options = {
        packageJsonPath: `${path.join(__dirname, '../..')}/package.json`,
      };
      mvn(options, (err: any, mvnResults: any) => {
        if (err && !err.message.includes('Could not find java property')) {
          reject(err);
        } else {
          if (!jinst.isJvmCreated()) {
            jinst.addOption('-Xrs');
            jinst.addOption('-Dfile.encoding=UTF8');

            // Workaround for Databricks JDBC driver
            // Issue when deserializing Apache Arrow data with Java JVMs version 11 or higher, due to compatibility issues.
            jinst.addOption('--add-opens=java.base/java.nio=ALL-UNNAMED');

            const classPath = (mvnResults && mvnResults.classpath || []).concat(customClassPath || []);
            jinst.setupClasspath(classPath);
          }
          resolve();
        }
      });
    });
  }
  return mvnPromise;
};

// promisify Connection methods
Connection.prototype.getMetaDataAsync = promisify(Connection.prototype.getMetaData);
// promisify DatabaseMetaData methods
DatabaseMetaData.prototype.getSchemasAsync = promisify(DatabaseMetaData.prototype.getSchemas);
DatabaseMetaData.prototype.getTablesAsync = promisify(DatabaseMetaData.prototype.getTables);

// java.sql.Types
const JDBC_TYPE_NAMES: Record<number, string> = {
  [-5]: 'bigint',
  2: 'bigdecimal',
  3: 'bigdecimal',
};

type RowBatchIter = {
  labels: string[],
  types: number[],
  next: NextBatchFn,
};

export class JDBCDriver extends BaseDriver {
  protected readonly config: JDBCDriverConfiguration;

  protected pool: Pool<any>;

  protected jdbcProps: any;

  public constructor(
    config: Partial<JDBCDriverConfiguration> & {
      /**
       * Data source name.
       */
      dataSource?: string,

      /**
       * Whether this driver is used for pre-aggregations.
       */
      preAggregations?: boolean,

      /**
       * Max pool size value for the [cube]<-->[db] pool.
       */
      maxPoolSize?: number,

      /**
       * Time to wait for a response from a connection after validation
       * request before determining it as not valid. Default - 60000 ms.
       */
      testConnectionTimeout?: number,
    } = {}
  ) {
    super({
      testConnectionTimeout: config.testConnectionTimeout || 60000,
    });

    const dataSource =
      config.dataSource ||
      assertDataSource('default');
    const preAggregations = config.preAggregations || false;

    const { poolOptions, ...dbOptions } = config;

    const dbTypeDescription = JDBCDriver.dbTypeDescription(
      <string>(config.dbType || getEnv('dbType', { dataSource, preAggregations })),
    );

    this.config = {
      dbType: getEnv('dbType', { dataSource, preAggregations }),
      url:
        getEnv('jdbcUrl', { dataSource, preAggregations }) ||
        dbTypeDescription && dbTypeDescription.jdbcUrl(),
      drivername:
        getEnv('jdbcDriver', { dataSource, preAggregations }) ||
        dbTypeDescription && dbTypeDescription.driverClass,
      properties: dbTypeDescription && dbTypeDescription.properties,
      ...dbOptions,
      batchSize: config.batchSize || getEnv('jdbcBatchSize', { dataSource, preAggregations }),
    } as JDBCDriverConfiguration;

    if (!this.config.drivername) {
      throw new Error('drivername is required property');
    }

    if (!this.config.url) {
      throw new Error('url is required property');
    }

    const poolName = createPoolName('jdbc', dataSource, preAggregations);
    this.pool = new Pool(poolName, {
      create: async () => {
        await initMvn(await this.getCustomClassPath());

        if (!this.jdbcProps) {
          /** @protected */
          this.jdbcProps = this.getJdbcProperties();
        }

        const getConnection = promisify(DriverManager.getConnection.bind(DriverManager));
        return new Connection(await getConnection(this.config.url, this.jdbcProps));
      },
      destroy: async (connection) => promisify(connection.close.bind(connection))(),
      validate: async (connection) => (
        new Promise((resolve) => {
          const isValid = promisify(connection.isValid.bind(connection));
          const timeout = setTimeout(() => {
            if (this.logger) {
              this.logger('Connection validation failed by timeout', {
                testConnectionTimeout: this.testConnectionTimeout(),
              });
            }
            resolve(false);
          }, this.testConnectionTimeout());
          isValid(0).then((valid: boolean) => {
            clearTimeout(timeout);
            if (!valid && this.logger) {
              this.logger('Connection validation failed', {});
            }
            resolve(valid);
          }).catch((e: { stack?: string }) => {
            clearTimeout(timeout);
            this.databasePoolError(e);
            resolve(false);
          });
        })
      )
    }, {
      min: 0,
      max: config.maxPoolSize || getEnv('dbMaxPoolSize', { dataSource, preAggregations }) || 8,
      evictionRunIntervalMillis: 10000,
      softIdleTimeoutMillis: 30000,
      idleTimeoutMillis: 30000,
      testOnBorrow: true,
      acquireTimeoutMillis: 120000,
      ...(poolOptions || {})
    });

    // https://github.com/coopernurse/node-pool/blob/ee5db9ddb54ce3a142fde3500116b393d4f2f755/README.md#L220-L226
    this.pool.on('factoryCreateError', (err) => {
      this.databasePoolError(err);
    });
    this.pool.on('factoryDestroyError', (err) => {
      this.databasePoolError(err);
    });
  }

  protected async getCustomClassPath() {
    return this.config.customClassPath;
  }

  protected getJdbcProperties() {
    const java = jinst.getInstance();
    const Properties = java.import('java.util.Properties');
    const properties = new Properties();

    for (const [name, value] of Object.entries(this.config.properties)) {
      properties.putSync(name, value);
    }

    return properties;
  }

  public async testConnection() {
    let err;
    let connection;

    try {
      connection = await this.pool._factory.create();
    } catch (e: any) {
      err = e.message || e;
    }

    if (err) {
      throw new Error(err.toString());
    } else {
      await this.pool._factory.destroy(connection);
    }
  }

  protected prepareConnectionQueries() {
    const dbTypeDescription = JDBCDriver.dbTypeDescription(this.config.dbType);
    return this.config.prepareConnectionQueries ||
      dbTypeDescription && dbTypeDescription.prepareConnectionQueries ||
      [];
  }

  protected escapeDialect(): EscapeDialect {
    const dbTypeDescription = JDBCDriver.dbTypeDescription(this.config.dbType);
    if (dbTypeDescription?.escapeDialect) {
      return dbTypeDescription.escapeDialect;
    }

    throw new Error(
      `Unable to detect SQL escaping rules for db type "${this.config.dbType}"`
    );
  }

  protected prepareQueryWithParams(query: string, values: unknown[]): string {
    return format(this.escapeDialect(), query, values || []);
  }

  public async query<R = unknown>(query: string, values: unknown[]): Promise<R[]> {
    const queryWithParams = this.prepareQueryWithParams(query, values);
    const cancelObj: { cancel?: Function } = {};
    const promise = this.queryPromised(queryWithParams, cancelObj, this.prepareConnectionQueries());
    (promise as CancelablePromise<any>).cancel =
      () => cancelObj.cancel && cancelObj.cancel() ||
      Promise.reject(new Error('Statement is not ready'));
    return promise;
  }

  protected async withConnection<T extends Function>(fn: T) {
    const conn = await this.pool.acquire();

    try {
      return await fn(conn);
    } finally {
      await this.pool.release(conn);
    }
  }

  protected async queryPromised(query: string, cancelObj: any, options: any) {
    options = options || {};

    try {
      const conn = await this.pool.acquire();

      try {
        const prepareConnectionQueries = options.prepareConnectionQueries || [];

        for (let i = 0; i < prepareConnectionQueries.length; i++) {
          await this.executeStatement(conn, prepareConnectionQueries[i]);
        }
        return await this.executeStatement(conn, query, cancelObj);
      } finally {
        await this.pool.release(conn);
      }
    } catch (ex: any) {
      if (ex.cause) {
        throw new Error(ex.cause.getMessageSync());
      } else {
        throw ex;
      }
    }
  }

  public async stream(sql: string, values: unknown[], { highWaterMark }: StreamOptions): Promise<DownloadQueryResultsResult> {
    const conn = await this.pool.acquire();

    try {
      const query = this.prepareQueryWithParams(sql, values);
      const cancelObj: { cancel?: Function } = {};

      const createStatement = promisify(conn.createStatement.bind(conn));
      const statement = await createStatement();

      if (cancelObj) {
        cancelObj.cancel = promisify(statement.cancel.bind(statement));
      }

      const executeQuery = promisify(statement.execute.bind(statement));
      const resultSet = await executeQuery(query);
      const iter = await this.rowBatchIter(resultSet);

      const rowStream = new QueryStream(iter.next, highWaterMark);

      return {
        rowStream,
        // Called when either side of the pipe closes, possibly while a batch is still being
        // fetched; handing the connection to another query then would use it concurrently.
        release: async () => {
          try {
            try {
              await rowStream.close();
            } finally {
              await this.closeStatement(statement);
            }
          } finally {
            await this.pool.release(conn);
          }
        },
        types: iter.types.map(
          (t, i) => ({
            name: iter.labels[i],
            type: this.toGenericType(this.jdbcTypeName(t, resultSet._types))
          })
        )
      };
    } catch (ex: any) {
      await this.pool.release(conn);

      if (ex.cause) {
        throw new Error(ex.cause.getMessageSync());
      } else {
        throw ex;
      }
    }
  }

  public async downloadQueryResults(query: string, values: unknown[], options: DownloadQueryResultsOptions): Promise<DownloadQueryResultsResult> {
    if (options.streamImport) {
      return this.stream(query, values, options);
    }

    return super.downloadQueryResults(query, values, options);
  }

  protected async executeStatement(conn: any, query: any, cancelObj?: any) {
    const createStatementAsync = promisify(conn.createStatement.bind(conn));
    const statement = await createStatementAsync();
    if (cancelObj) {
      cancelObj.cancel = promisify(statement.cancel.bind(statement));
    }
    const setQueryTimeout = promisify(statement.setQueryTimeout.bind(statement));
    await setQueryTimeout(600);
    const executeQueryAsync = promisify(statement.execute.bind(statement));
    const resultSet = await executeQueryAsync(query);

    if (resultSet.toObjectBatchIter) {
      const iter = await this.rowBatchIter(resultSet);
      const result: Row[] = [];

      for (let rows = await iter.next(); rows; rows = await iter.next()) {
        for (let i = 0; i < rows.length; i++) {
          result.push(rows[i]);
        }
      }

      return result;
    }

    return resultSet;
  }

  /**
   * Never throws: callers await `release` in a `finally`, where a failed close would replace
   * the error of the query or fail one that has already succeeded.
   */
  protected async closeStatement(statement: any): Promise<void> {
    try {
      await promisify(statement.close.bind(statement))();
    } catch (e: any) {
      if (this.logger) {
        this.logger('Closing statement failed', { error: (e.stack || e).toString() });
      }
    }
  }

  /**
   * `getterNames` is the table node-jdbc uses to pick a ResultSet getter. It reads BIGINT, NUMERIC
   * and DECIMAL as strings to keep their precision, so their column types can't be taken from it.
   */
  protected jdbcTypeName(jdbcType: number, getterNames: (string | undefined)[]): string {
    return (JDBC_TYPE_NAMES[jdbcType] || getterNames[jdbcType] || 'string').toLowerCase();
  }

  /**
   * Rows are read and serialized inside the JVM, one batch per call. Reading them cell by cell
   * from Node.js costs a synchronous JNI round trip per cell and blocks the event loop.
   */
  protected async rowBatchIter(resultSet: any): Promise<RowBatchIter> {
    return resultSet.toObjectBatchIter({ batchSize: this.config.batchSize });
  }

  public async release() {
    await this.pool.drain();
    await this.pool.clear();
  }

  public static getSupportedDrivers(): string[] {
    return Object.keys(SupportedDrivers);
  }

  public static dbTypeDescription(dbType: string): DriverOptionsInterface {
    return SupportedDrivers[dbType];
  }
}
