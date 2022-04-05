import { PostgresDriver, PostgresDriverConfiguration } from '@cubejs-backend/postgres-driver';
import { DownloadTableMemoryData, IndexesSQL, StreamOptions, StreamTableDataWithTypes, TableStructure } from '@cubejs-backend/query-orchestrator';
import { PoolClient } from 'pg';
import Stream from './Stream';

export class MaterializeDriver extends PostgresDriver {
  public constructor(options: PostgresDriverConfiguration) {
    super(options);
  }

  protected async prepareConnection(
    conn: PoolClient
  ) {
    await conn.query(`SET TIME ZONE '${this.config.storeTimezone || 'UTC'}'`);
    // Support for statement_timeout is still pending. https://github.com/MaterializeInc/materialize/issues/10390
  }

  protected async loadUserDefinedTypes(): Promise<void> {
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

  public async uploadTableWithIndexes(
    table: string,
    columns: TableStructure,
    tableData: DownloadTableMemoryData,
    indexesSql: IndexesSQL
  ) {
    if (!tableData.rows) {
      throw new Error(`${this.constructor} driver supports only rows upload`);
    }

    await this.createTable(table, columns);

    try {
      for (let i = 0; i < tableData.rows.length; i++) {
        await this.query(
          `INSERT INTO ${table}
        (${columns.map(c => this.quoteIdentifier(c.name)).join(', ')})
        VALUES (${columns.map((c, paramIndex) => this.param(paramIndex)).join(', ')})`,
          columns.map(c => this.toColumnValue(tableData.rows[i][c.name], c.type))
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

  public async stream(
    query: string,
    values: unknown[],
    { highWaterMark }: StreamOptions
  ): Promise<StreamTableDataWithTypes> {
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
      const rowStream = new Stream(conn, cursorId, highWaterMark, this.getTypeParser);

      return {
        rowStream,
        types: this.mapFields(fields),
        release: async () => {
          await conn.release();
        }
      };
    } catch (e) {
      await conn.release();

      throw e;
    }
  }
}
