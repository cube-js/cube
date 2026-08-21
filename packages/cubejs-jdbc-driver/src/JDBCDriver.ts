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
  normalizeSqlPreamble,
  splitSqlPreamble,
  applySqlPreambleStatements,
} from '@cubejs-backend/base-driver';
import { promisify } from 'util';
import path from 'path';

import { SupportedDrivers } from './supported-drivers';
import type { DriverOptionsInterface, EscapeDialect } from './supported-drivers';
import type { JDBCDriverConfiguration } from './types';
import { QueryStream, transformRow } from './QueryStream';
import type { nextFn } from './QueryStream';

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

export class JDBCDriver extends BaseDriver {
  protected readonly config: JDBCDriverConfiguration;

  protected pool: Pool<any>;

  // prepareConnectionQueries() runs per query, so the deprecation notice is
  // latched to one line per driver instead of one per query.
  private deprecationWarned = false;

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
      ...dbOptions
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

  /**
   * This driver applies `sql_preamble`.
   *
   * Inherited by every JDBC-based driver — Databricks among them — which reuse
   * the replay this class performs on each acquired connection.
   */
  public override supportsSqlPreamble(): boolean {
    return true;
  }

  /**
   * Resolved here rather than in BaseDriver, so the base accessor would
   * otherwise report no preamble to the pre-aggregation version key. Only the
   * user-configured value, not the per-dbType built-ins — those are fixed for a
   * driver and would add a constant to every key.
   */
  public override effectiveSqlPreamble(): string | undefined {
    return normalizeSqlPreamble(this.config.sqlPreamble) ?? getEnv('dbSqlPreamble', {
      dataSource: this.config.dataSource ?? 'default',
      preAggregations: this.config.preAggregationsSqlPreamble ?? this.config.preAggregations,
    });
  }

  /**
   * Statements replayed on a connection before the primary query.
   *
   * A `sqlPreamble` is appended to the per-dbType built-ins, built-ins first:
   * `supported-drivers.ts` ships `SET time_zone` for MySQL, and dropping it
   * would silently change how timestamps are read.
   *
   * The deprecated `prepareConnectionQueries` keeps *replacing* those built-ins,
   * which is what it has always done — someone who set it to override the
   * timezone still gets that, and only that, until the option is removed.
   */
  protected prepareConnectionQueries(): string[] {
    const dbTypeDescription = JDBCDriver.dbTypeDescription(this.config.dbType);
    const builtIn = dbTypeDescription && dbTypeDescription.prepareConnectionQueries || [];

    const preamble = normalizeSqlPreamble(this.config.sqlPreamble) ?? getEnv('dbSqlPreamble', {
      dataSource: this.config.dataSource ?? 'default',
      // Not `this.config.preAggregations`: a build resolves the preamble from
      // the pre-aggregation namespace even when its credentials do not.
      preAggregations: this.config.preAggregationsSqlPreamble ?? this.config.preAggregations,
    });

    // Only fall back to the deprecated option when no preamble is configured by
    // either the new option or the env var, so migrating to `sqlPreamble` is
    // never silently overridden by a value left behind in the old one.
    if (!preamble && this.config.prepareConnectionQueries?.length) {
      if (!this.deprecationWarned) {
        this.deprecationWarned = true;
        this.logger?.('Deprecated driver option', {
          warning: 'The prepareConnectionQueries driver option is deprecated and will be removed in a future release. Use sqlPreamble instead — note it appends to the built-in connection queries rather than replacing them.',
        });
      }

      return this.config.prepareConnectionQueries;
    }

    return [...builtIn, ...splitSqlPreamble(preamble)];
  }

  /**
   * The same statements as `prepareConnectionQueries`, split by origin.
   *
   * The two halves need different failure postures. This driver pools
   * connections, so the preamble is replayed on every acquire and a `CREATE …` —
   * the feature's headline use case — raises "already exists" the second time a
   * connection is reused; that has to be tolerated. The per-dbType built-ins are
   * only `SET`s, are idempotent, and stay raw so a genuine failure in them still
   * surfaces. Same for the deprecated `prepareConnectionQueries`, whose contents
   * are the user's own and whose semantics must not change.
   */
  protected splitConnectionQueries(): { builtIn: string[], preamble: string[] } {
    const all = this.prepareConnectionQueries();
    const preamble = splitSqlPreamble(this.effectiveSqlPreamble());
    // A preamble is only appended, never mixed in, so the last N entries are it —
    // but only when the merged list actually ends with them: the deprecated
    // option replaces the built-ins instead of appending, and then no entry is a
    // `sqlPreamble` statement.
    const appended = preamble.length > 0
      && all.length >= preamble.length
      && all.slice(all.length - preamble.length).every((statement, i) => statement === preamble[i]);

    return appended
      ? { builtIn: all.slice(0, all.length - preamble.length), preamble }
      : { builtIn: all, preamble: [] };
  }

  /**
   * Replays the connection queries on a freshly acquired connection.
   */
  protected async applyConnectionQueries(conn: any): Promise<void> {
    const { builtIn, preamble } = this.splitConnectionQueries();

    for (const statementSql of builtIn) {
      await this.executeStatement(conn, statementSql);
    }

    await applySqlPreambleStatements(
      preamble,
      statement => this.executeStatement(conn, statement),
    );
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
    const cancelObj: {cancel?: Function} = {};
    // No `prepareConnectionQueries` override: `queryPromised` replays them
    // itself, splitting the user preamble from the built-ins so a pooled
    // connection tolerates an already-applied `CREATE …`.
    const promise = this.queryPromised(queryWithParams, cancelObj, {});
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
        // `options.prepareConnectionQueries` stays honoured for callers that pass
        // their own list, but it is a flat merged list with no way to tell a
        // built-in from user preamble, so only the default path gets the
        // already-applied tolerance the pool requires.
        if (options.prepareConnectionQueries) {
          for (const statementSql of options.prepareConnectionQueries) {
            await this.executeStatement(conn, statementSql);
          }
        } else {
          await this.applyConnectionQueries(conn);
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
      const cancelObj: {cancel?: Function} = {};

      // A streamed query has to run in the preamble's context too; this path
      // used to skip the connection queries the query path replays.
      await this.applyConnectionQueries(conn);

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
