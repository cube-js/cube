import { PostgresDriver, PostgresDriverConfiguration } from '@cubejs-backend/postgres-driver';
import { PoolClient } from 'pg';

export class MaterializeDriver extends PostgresDriver {
  public constructor(options: PostgresDriverConfiguration) {
    super(options);
  }

  protected async prepareConnection(
    conn: PoolClient,
    options: { executionTimeout: number } = {
      executionTimeout: this.config.executionTimeout ? <number>this.config.executionTimeout * 1000 : 600000,
    }
  ) {
    await conn.query(`SET TIME ZONE '${this.config.storeTimezone || 'UTC'}'`);
    // Support for statement_timeout is still pending. https://github.com/MaterializeInc/materialize/issues/10390
    // await conn.query(`SET statement_timeout TO ${options.executionTimeout}`);

    await this.loadUserDefinedTypes(conn);
  }

  protected async loadUserDefinedTypes(conn: PoolClient): Promise<void> {
    // Support for typcategory field still pending: https://github.com/MaterializeInc/materialize/issues/2157
  }

  /**
   * @param {string} schemaName
   * @return {Promise<Array<unknown>>}
   */
  public async createSchemaIfNotExists(schemaName: string): Promise<unknown[]> {
    return this.query(
      `SHOW SCHEMAS WHERE name = '${schemaName}'`, []
    ).then((schemas) => {
      if (schemas.length === 0) {
        return this.query(`CREATE SCHEMA IF NOT EXISTS ${schemaName}`, []);
      } else return [];
    });
  }

  public loadPreAggregationIntoTable(preAggregationTableName: string, loadSql: string, params: any[], options: any): Promise<any> {
    // const materializedLoadSql = loadSql.replace(/^CREATE TABLE (\S+) AS/i, `CREATE MATERIALIZED VIEW ${preAggregationTableName} AS`);
    // return this.query(materializedLoadSql, params, options);
    return this.query(loadSql, params, options);
  }
}
