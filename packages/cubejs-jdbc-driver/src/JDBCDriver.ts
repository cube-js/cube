/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `JDBCDriver` and related types declaration.
 */

import {
  getEnv,
  assertDataSource,
  CancelablePromise,
} from '@cubejs-backend/shared';
import {
  BaseDriver,
  DownloadQueryResultsOptions,
  DownloadQueryResultsResult,
  StreamOptions,
} from '@cubejs-backend/base-driver';
import * as SqlString from 'sqlstring';
import { promisify } from 'util';
import genericPool, { Factory, Pool } from 'generic-pool';
import path from 'path';

import { SupportedDrivers } from './supported-drivers';
import type { DriverOptionsInterface } from './supported-drivers';
import type { JDBCDriverConfiguration } from './types';
import { QueryStream, transformRow } from './QueryStream';
import type { nextFn } from './QueryStream';

/* eslint-disable no-restricted-syntax,import/no-extraneous-dependencies */
const DriverManager = require('@cubejs-backend/jdbc/lib/drivermanager');
const Connection = require('@cubejs-backend/jdbc/lib/connection');
const DatabaseMetaData = require('@cubejs-backend/jdbc/lib/databasemetadata');
const jinst = require('@cubejs-backend/jdbc/lib/jinst');
const mvn = require('node-java-maven');

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

const applyParams = (query: string, params: object | any[]) => SqlString.format(query, params);

// promisify Connection methods
Connection.prototype.getMetaDataAsync = promisify(Connection.prototype.getMetaData);
// promisify DatabaseMetaData methods
DatabaseMetaData.prototype.getSchemasAsync = promisify(DatabaseMetaData.prototype.getSchemas);
DatabaseMetaData.prototype.getTablesAsync = promisify(DatabaseMetaData.prototype.getTables);

interface ExtendedPool extends Pool<any> {
  _factory: Factory<any>;
}

export class JDBCDriver extends BaseDriver {
  protected readonly config: JDBCDriverConfiguration;

  protected pool: ExtendedPool;

  protected jdbcProps: any;

  public constructor(
    config: Partial<JDBCDriverConfiguration> & {
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

    const { poolOptions, ...dbOptions } = config;

    const dbTypeDescription = JDBCDriver.dbTypeDescription(
      <string>(config.dbType || getEnv('dbType', { dataSource })),
    );

    this.config = {
      dbType: getEnv('dbType', { dataSource }),
      url:
        getEnv('jdbcUrl', { dataSource }) ||
        dbTypeDescription && dbTypeDescription.jdbcUrl(),
      drivername:
        getEnv('jdbcDriver', { dataSource }) ||
        dbTypeDescription && dbTypeDescription.driverClass,
      properties: dbTypeDescription && dbTypeDescription.properties,
      ...dbOptions
    } as JDBCDriverConfiguration;

    if (!this.config.drivername) {
      throw new Error('drivername is required property');
    }

    if (!this.config.url) {
      throw new Error('url is required property');
    }

    this.pool = genericPool.createPool({
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
      max: config.maxPoolSize || getEnv('dbMaxPoolSize', { dataSource }) || 8,
      evictionRunIntervalMillis: 10000,
      softIdleTimeoutMillis: 30000,
      idleTimeoutMillis: 30000,
      testOnBorrow: true,
      acquireTimeoutMillis: 120000,
      ...(poolOptions || {})
    }) as ExtendedPool;

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

  public async query<R = unknown>(query: string, values: unknown[]): Promise<R[]> {
    const queryWithParams = applyParams(query, values);
    const cancelObj: {cancel?: Function} = {};
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
    const query = applyParams(sql, values);
    const cancelObj: {cancel?: Function} = {};
    try {
      const createStatement = promisify(conn.createStatement.bind(conn));
      const statement = await createStatement();

      if (cancelObj) {
        cancelObj.cancel = promisify(statement.cancel.bind(statement));
      }

      const executeQuery = promisify(statement.execute.bind(statement));
      const resultSet = await executeQuery(query);
      return (await new Promise((resolve, reject) => {
        resultSet.toObjectIter(
          (
            err: unknown,
            res: {
                labels: string[],
                types: number[],
                rows: { next: nextFn },
              },
          ) => {
            if (err) {
              reject(err);
              return;
            }

            const rowStream = new QueryStream(res.rows.next, highWaterMark);
            resolve({
              rowStream,
              release: () => this.pool.release(conn),
              types: res.types.map(
                (t, i) => ({
                  name: res.labels[i],
                  type: this.toGenericType(((t === -5 ? 'bigint' : resultSet._types[t]) || 'string').toLowerCase())
                })
              )
            });
          }
        );
      }));
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

    if (resultSet.toObjArray) {
      const result: any = await (promisify(resultSet.toObjArray.bind(resultSet)))();

      for (const [key, row] of Object.entries(result)) {
        result[key] = transformRow(row);
      }

      return result;
    }

    return resultSet;
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
