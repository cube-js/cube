import { types, Pool, PoolConfig, FieldDef } from 'pg';
// eslint-disable-next-line import/no-extraneous-dependencies
import { TypeId, TypeFormat } from 'pg-types';
import * as moment from 'moment';
import R from 'ramda';
import {
  BaseDriver, DownloadQueryResultsOptions,
  DownloadTableMemoryData, DriverInterface,
  IndexesSQL, TableStructure, QueryOptions,
} from '@cubejs-backend/query-orchestrator';
import { QuestQuery } from './QuestQuery';

const NativeTypeToQuestType: Record<string, string> = R.invertObj(types.builtins);

const timestampDataTypes = [
  types.builtins.DATE,
  types.builtins.TIMESTAMP,
  types.builtins.TIMESTAMPTZ,
];
const timestampTypeParser = (val: string) => moment.utc(val).format(moment.HTML5_FMT.DATETIME_LOCAL_MS);

export type QuestDriverConfiguration = Partial<PoolConfig> & {
  readOnly?: boolean,
};

export class QuestDriver<Config extends QuestDriverConfiguration = QuestDriverConfiguration>
  extends BaseDriver implements DriverInterface {
  private readonly pool: Pool;

  private readonly config: Partial<Config>;

  public static dialectClass() {
    return QuestQuery;
  }

  public constructor(
    config: Partial<Config> = {}
  ) {
    super();

    this.pool = new Pool({
      max: process.env.CUBEJS_DB_MAX_POOL && parseInt(process.env.CUBEJS_DB_MAX_POOL, 10) || 4,
      idleTimeoutMillis: 30_000,
      host: process.env.CUBEJS_DB_HOST,
      database: process.env.CUBEJS_DB_NAME,
      port: <any>process.env.CUBEJS_DB_PORT,
      user: process.env.CUBEJS_DB_USER,
      password: process.env.CUBEJS_DB_PASS,
      ...config
    });
    this.pool.on('error', (err) => {
      console.log(`Unexpected error on idle client: ${err.stack || err}`);
    });

    this.config = {
      ...this.getInitialConfiguration(),
      ...config,
    };
  }

  private getInitialConfiguration(): Partial<QuestDriverConfiguration> {
    return {
      readOnly: true,
    };
  }

  public async testConnection(): Promise<void> {
    await this.pool.query('SELECT $1 AS number', ['1']);
  }

  private mapFields(fields: FieldDef[]) {
    return fields.map((f) => {
      let questType;
      if (f.dataTypeID in NativeTypeToQuestType) {
        questType = NativeTypeToQuestType[f.dataTypeID].toLowerCase();
      }
      if (!questType) {
        throw new Error(
          `Unable to detect type for field "${f.name}" with dataTypeID: ${f.dataTypeID}`
        );
      }

      return ({
        name: f.name,
        type: this.toGenericType(questType)
      });
    });
  }

  public async query<R = unknown>(query: string, values: unknown[], _options?: QueryOptions): Promise<R[]> {
    const result = await this.queryResponse(query, values);
    return result.rows;
  }

  private async queryResponse(query: string, values: unknown[]) {
    const conn = await this.pool.connect();

    try {
      const res = await conn.query({
        text: query,
        values: values || [],
        types: {
          getTypeParser: this.getTypeParser,
        },
      });
      return res;
    } finally {
      conn.release();
    }
  }

  private getTypeParser(dataTypeID: TypeId, format: TypeFormat | undefined) {
    const isTimestamp = timestampDataTypes.includes(dataTypeID);
    if (isTimestamp) {
      return timestampTypeParser;
    }

    const parser = types.getTypeParser(dataTypeID, format);
    return (val: any) => parser(val);
  }

  public async downloadQueryResults(query: string, values: unknown[], _options: DownloadQueryResultsOptions) {
    const res = await this.queryResponse(query, values);
    return {
      rows: res.rows,
      types: this.mapFields(res.fields),
    };
  }

  public async createSchemaIfNotExists(_schemaName: string): Promise<any> {
    // no-op as there are no schemas in QuestDB
  }

  public async tablesSchema() {
    const tables = await this.getTablesQuery('');

    // QuestDB doesn't have a notion of schema/logical database while the driver
    // has to return a `{ 'schema_name': { 'table1': {...} } }` object. So, we use
    // empty schema name ('') as a workaround to avoid the schema prefix
    // ('schema_name.') being used for table names in the generated queries.
    const metadata: Record<string, Record<string, object>> = { '': {} };

    // eslint-disable-next-line camelcase
    await Promise.all(tables.map(async ({ table_name: tableName }) => {
      if (tableName === undefined) {
        return;
      }
      const columns = await this.tableColumnTypes(tableName);
      metadata[''][tableName] = columns;
    }));

    return metadata;
  }

  // eslint-disable-next-line camelcase
  public async getTablesQuery(_schemaName: string): Promise<({ table_name?: string, TABLE_NAME?: string })[]> {
    const response = await this.query('SHOW TABLES', []);

    return response.map((row: any) => ({
      table_name: row.table,
    }));
  }

  public async tableColumnTypes(table: string) {
    const response: any[] = await this.query(`SHOW COLUMNS FROM ${table}`, []);

    return response.map((row) => ({ name: row.column, type: this.toGenericType(row.type) }));
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
      // Make sure to commit the data to make it visible for later queries.
      await this.query('COMMIT', []);

      for (let i = 0; i < indexesSql.length; i++) {
        const [query, params] = indexesSql[i].sql;
        await this.query(query, params);
      }
    } catch (e) {
      await this.dropTable(table);
      throw e;
    }
  }

  public readOnly() {
    return !!this.config.readOnly;
  }

  public release() {
    return this.pool.end();
  }

  public param(paramIndex: number) {
    return `$${paramIndex + 1}`;
  }
}
