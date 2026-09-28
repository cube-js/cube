/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `CrateDriver` and related types declaration.
 */

import { PostgresDriver, PgClient } from '@cubejs-backend/postgres-driver';

export class CrateDriver extends PostgresDriver {
  protected async prepareConnection(conn: PgClient, _options: any) {
    // Not supported by Crate yet... https://github.com/crate/crate/issues/12356
    // await conn.query(`SET TIME ZONE '${this.config.storeTimezone || 'UTC'}'`);
    // await conn.query(`SET statement_timeout TO ${options.executionTimeout}`);

    await this.loadUserDefinedTypes(conn);
  }

  /**
   * CrateDB's Postgres-wire protocol does not accept bound parameters inside a
   * `CREATE TABLE AS` statement -- it fails with "Requested parameter index
   * exceeds the number of parameters: 0".
   *
   * The ParameterDescription message works for the most common use cases except for DDL statements
   * https://cratedb.com/docs/crate/reference/en/latest/interfaces/postgres.html#extended-query
   */
  public async loadPreAggregationIntoTable(
    preAggregationTableName: string,
    loadSql: string,
    params: unknown[],
    options: any
  ): Promise<any> {
    const result = await super.loadPreAggregationIntoTable(
      preAggregationTableName,
      CrateDriver.inlineParams(loadSql, params),
      [],
      options
    );

    // CrateDB is eventually consistent: rows written by the `CREATE TABLE AS`
    // above are not visible to subsequent reads until the table is refreshed.
    // Refresh so the freshly materialized pre-aggregation is immediately readable.
    // See https://cratedb.com/docs/crate/reference/en/latest/general/dql/refresh.html
    await this.query(`REFRESH TABLE ${preAggregationTableName}`, []);

    return result;
  }

  protected static inlineParams(sql: string, params: unknown[]): string {
    if (!params || params.length === 0) {
      return sql;
    }

    return sql.replace(/\$(\d+)/g, (match, index) => {
      const param = params[parseInt(index, 10) - 1];
      return param === undefined ? match : CrateDriver.formatParam(param);
    });
  }

  protected static formatParam(value: unknown): string {
    if (value === null) {
      return 'NULL';
    }

    if (typeof value === 'number') {
      return `${value}`;
    }

    if (typeof value === 'boolean') {
      return value ? 'TRUE' : 'FALSE';
    }

    if (value instanceof Date) {
      return `'${value.toISOString()}'`;
    }

    return `'${String(value).replace(/'/g, '\'\'')}'`;
  }
}
