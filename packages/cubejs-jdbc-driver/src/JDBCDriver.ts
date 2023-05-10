/* eslint-disable no-restricted-syntax,import/no-extraneous-dependencies */
import { BaseDriver } from '@cubejs-backend/base-driver';
import { CancelablePromise } from '@cubejs-backend/shared';
import * as SqlString from 'sqlstring';
import { promisify } from 'util';
import genericPool, { Factory, Pool } from 'generic-pool';

import { DriverOptionsInterface, SupportedDrivers } from './supported-drivers';
// eslint-disable-next-line @typescript-eslint/no-unused-vars
import { JDBCDriverConfiguration } from './types';

const DriverManager = require('jdbc/lib/drivermanager');
const Connection = require('jdbc/lib/connection');
const DatabaseMetaData = require('jdbc/lib/databasemetadata');
const jinst = require('jdbc/lib/jinst');
const mvn = require('node-java-maven');

let mvnPromise: Promise<void> | null = null;

const initMvn = (customClassPath: any) => {
  if (!mvnPromise) {
    mvnPromise = new Promise((resolve, reject) => {
      mvn((err: any, mvnResults: any) => {
        if (err && !err.message.includes('Could not find java property')) {
          reject(err);
        } else {
          if (!jinst.isJvmCreated()) {
            jinst.addOption('-Xrs');
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

  public constructor(config: Partial<JDBCDriverConfiguration> = {}) {
    super();

    const { poolOptions, ...dbOptions } = config || {};

    const dbTypeDescription = JDBCDriver.dbTypeDescription((config.dbType || process.env.CUBEJS_DB_TYPE) as string);

    this.config = {
      dbType: process.env.CUBEJS_DB_TYPE,
      url: process.env.CUBEJS_JDBC_URL || dbTypeDescription && dbTypeDescription.jdbcUrl(),
      drivername: process.env.CUBEJS_JDBC_DRIVER || dbTypeDescription && dbTypeDescription.driverClass,
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
      // @ts-expect-error Promise<Function> vs Promise<void>
      destroy: async (connection) => promisify(connection.close.bind(connection)),
      validate: (connection) => {
        const isValid = promisify(connection.isValid.bind(connection));
        try {
          return isValid(this.testConnectionTimeout() / 1000);
        } catch (e) {
          return false;
        }
      }
    }, {
      min: 0,
      max:
        process.env.CUBEJS_DB_MAX_POOL && parseInt(process.env.CUBEJS_DB_MAX_POOL, 10) ||
        this.config.maxPoolSize || 8,
      evictionRunIntervalMillis: 10000,
      softIdleTimeoutMillis: 30000,
      idleTimeoutMillis: 30000,
      testOnBorrow: true,
      acquireTimeoutMillis: 20000,
      ...(poolOptions || {})
    }) as ExtendedPool;
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
      err = e.message;
    }
    if (err) {
      throw new Error(err);
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
      () => cancelObj.cancel && cancelObj.cancel() || Promise.reject(new Error('Statement is not ready'));
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
    const toObjArrayAsync =
      resultSet.toObjArray && promisify(resultSet.toObjArray.bind(resultSet)) ||
      (() => Promise.resolve(resultSet));

    return toObjArrayAsync();
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
