/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `MaterializeDriver` and related types declaration.
 */

import { PostgresDriver, PostgresDriverConfiguration } from '@cubejs-backend/postgres-driver';
import {
  BaseDriver,
  DatabaseStructure,
  DownloadTableMemoryData,
  IndexesSQL,
  InformationSchemaColumn,
  StreamOptions,
  StreamTableDataWithTypes,
  TableStructure
} from '@cubejs-backend/base-driver';
import { PoolClient, QueryResult } from 'pg';
import { Readable } from 'stream';
import semver from 'semver';

export type ReadableStreamTableDataWithTypes = StreamTableDataWithTypes & {
  /**
   * Compatibility with streamToArray method from '@cubejs-backend/shared'
   */
  rowStream: Readable;
};

/**
 * Materialize driver class.
 */
export class MaterializeDriver extends PostgresDriver {
  /**
   * Returns default concurrency value.
   */
  public static getDefaultConcurrency(): number {
    return 2;
  }

  /**
   * Class constructor.
   */
  public constructor(
    options: PostgresDriverConfiguration & {
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
       * request before determining it as not valid. Default - 10000 ms.
       */
      testConnectionTimeout?: number,

      /**
       * Optional cluster name to set for the connection.
       */
      cluster?: string,

      /**
       * SSL is enabled by default. Set to false to disable.
       */
      ssl?: boolean | { rejectUnauthorized: boolean },

      /**
       * Application name to set for the connection.
       */
      // eslint-disable-next-line camelcase
      application_name?: string,
    } = {},
  ) {
    // Enable SSL by default if not set explicitly to false
    const sslEnv = process.env.CUBEJS_DB_SSL;
    if (sslEnv === 'false') {
      options.ssl = false;
    } else if (sslEnv === 'true') {
      options.ssl = { rejectUnauthorized: true };
    } else if (options.ssl === undefined) {
      options.ssl = true;
    }
    // Set application name to 'cubejs-materialize-driver' by default
    options.application_name = options.application_name || 'cubejs-materialize-driver';

    super(options);
  }

  protected async prepareConnection(
    conn: PoolClient
  ) {
    await conn.query(`SET TIME ZONE '${this.config.storeTimezone || 'UTC'}'`);
    // Support for statement_timeout is still pending. https://github.com/MaterializeInc/materialize/issues/10390

    // Set cluster to the CUBEJS_DB_MATERIALIZE_CLUSTER env variable if it exists
    if (process.env.CUBEJS_DB_MATERIALIZE_CLUSTER) {
      await conn.query(`SET CLUSTER TO ${process.env.CUBEJS_DB_MATERIALIZE_CLUSTER}`);
    }
  }

  protected async loadUserDefinedTypes(): Promise<void> {
    // Support for typcategory field still pending: https://github.com/MaterializeInc/materialize/issues/2157
  }

  /**
   * @param {string} schemaName
   * @return {Promise<Array<unknown>>}
   */
  public async createSchemaIfNotExists(schemaName: string): Promise<void> {
    const schemas = await this.query(
      `SHOW SCHEMAS WHERE name = '${schemaName}'`, []
    );
    if (schemas.length === 0) {
      await this.query(`CREATE SCHEMA IF NOT EXISTS ${schemaName}`, []);
    }
  }

  public async uploadTableWithIndexes(
    table: string,
    columns: TableStructure,
    tableData: DownloadTableMemoryData,
    indexesSql: IndexesSQL
  ) {
    return BaseDriver.prototype.uploadTableWithIndexes.bind(this)(table, columns, tableData, indexesSql, [], null, {});
  }

  /**
   * Materialize queryable schema
   * Returns materialized sources, materialized views, and tables
   * @returns {string} schemaQuery
   */
  public informationSchemaQueryWithFilter(version: string): string {
    console.log(version);
    const materializationFilter = semver.lt(version, 'v0.27.0-alpha') ? `
        table_name IN (
          SELECT name
          FROM mz_catalog.mz_sources
          WHERE mz_internal.mz_is_materialized(id)
          UNION
          SELECT name
          FROM mz_catalog.mz_views
          WHERE mz_internal.mz_is_materialized(id)
          UNION
          SELECT t.name
          FROM mz_catalog.mz_tables t
        )` : `
        table_name IN (
          SELECT name
          FROM mz_catalog.mz_sources
          UNION
          SELECT name
          FROM mz_catalog.mz_tables t
          UNION
          SELECT name
          FROM mz_catalog.mz_materialized_views t
        )
        `;

    return `${super.informationSchemaQuery()} AND ${materializationFilter}`;
  }

  /**
   * Materialize instance version
   * @returns {Promise<string>} version
   */
  public async getMaterializeVersion(): Promise<string> {
    const [{ version }] = await this.query<{version: string}>('SELECT mz_version() as version;', []);

    // Materialize returns the version as follows: 'v0.24.3-alpha.5 (65778f520)'
    return version.split(' ')[0];
  }

  public override async tablesSchema(): Promise<DatabaseStructure> {
    const version = await this.getMaterializeVersion();
    const query = this.informationSchemaQueryWithFilter(version);
    const data: InformationSchemaColumn[] = await this.query(query, []);
    const sortedData = this.informationColumnsSchemaSorter(data);

    return sortedData.reduce<DatabaseStructure>(this.informationColumnsSchemaReducer, {});
  }

  protected async* asyncFetcher<R extends unknown>(conn: PoolClient, cursorId: string): AsyncGenerator<R> {
    const timeout = `${this.config.executionTimeout ? <number>(this.config.executionTimeout) * 1000 : 600000} milliseconds`;
    const queryParams = {
      text: `FETCH 1000 ${cursorId} WITH (TIMEOUT='${timeout}');`,
      values: [],
      types: { getTypeParser: this.getTypeParser }
    };
    let finish = false;

    while (!finish) {
      const results: QueryResult<any> | undefined = await conn.query(queryParams);
      const { rows, rowCount } = results;

      if (rowCount === 0) {
        finish = true;
      }

      for (const row of rows) {
        yield row;
      }
    }
  }

  private async releaseStream(conn: PoolClient): Promise<void> {
    try {
      await conn.query('COMMIT;', []);
    } finally {
      await conn.release();
    }
  }

  public async stream(
    query: string,
    values: unknown[],
    { highWaterMark }: StreamOptions
  ): Promise<ReadableStreamTableDataWithTypes> {
    const conn = await this.pool.connect();
    try {
      const cursorId = 'mz_cursor';
      await this.prepareConnection(conn);
      await conn.query('BEGIN;', []);
      await conn.query(`DECLARE ${cursorId} CURSOR FOR ${query}`, values);
      const { fields } = await conn.query({
        text: `FETCH 0 FROM ${cursorId};`,
        values: [],
        types: {
          getTypeParser: this.getTypeParser,
        },
      });
      const rowStream = Readable.from(this.asyncFetcher(conn, cursorId), { highWaterMark });

      return {
        rowStream,
        types: this.mapFields(fields),
        release: () => this.releaseStream(conn)
      };
    } catch (e) {
      this.releaseStream(conn);
      throw e;
    }
  }
}
