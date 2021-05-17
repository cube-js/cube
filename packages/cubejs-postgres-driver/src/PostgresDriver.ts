import { types, Pool, PoolConfig } from 'pg';
import moment from 'moment';
import {
  BaseDriver,
  DownloadQueryResults, DownloadTableMemoryData, DriverInterface,
  GenericDataBaseType, IndexesSQL, TableStructure,
} from '@cubejs-backend/query-orchestrator';

const GenericTypeToPostgres: Record<GenericDataBaseType, string> = {
  string: 'text',
  double: 'decimal'
};

const DataTypeMapping: Record<string, any> = {};

Object.entries(types.builtins).forEach(pair => {
  const [key, value] = pair;
  DataTypeMapping[value] = key;
});

const timestampDataTypes = [1114, 1184];
const timestampTypeParser = (val: any) => moment.utc(val).format(moment.HTML5_FMT.DATETIME_LOCAL_MS);

export type PostgresDriverConfiguration = Partial<PoolConfig> & {
  storeTimezone?: string,
  executionTimeout?: number,
  readOnly?: number,
};

export class PostgresDriver extends BaseDriver implements DriverInterface {
  protected readonly pool: Pool;

  public constructor(
    protected readonly config: Partial<PostgresDriverConfiguration> = {}
  ) {
    super();

    this.pool = new Pool({
      max: process.env.CUBEJS_DB_MAX_POOL && parseInt(process.env.CUBEJS_DB_MAX_POOL, 10) || 8,
      idleTimeoutMillis: 30000,
      host: process.env.CUBEJS_DB_HOST,
      database: process.env.CUBEJS_DB_NAME,
      port: <any>process.env.CUBEJS_DB_PORT,
      user: process.env.CUBEJS_DB_USER,
      password: process.env.CUBEJS_DB_PASS,
      ssl: this.getSslOptions(),
      ...config
    });
    this.pool.on('error', (err) => {
      console.log(`Unexpected error on idle client: ${err.stack || err}`); // TODO
    });
  }

  public async testConnection(): Promise<void> {
    try {
      await this.pool.query('SELECT $1::int AS number', ['1']);
    } catch (e) {
      if (e.toString().indexOf('no pg_hba.conf entry for host') !== -1) {
        throw new Error(`Please use CUBEJS_DB_SSL=true to connect: ${e.toString()}`);
      }

      throw e;
    }
  }

  protected async queryResponse(query: string, values: unknown[]) {
    const client = await this.pool.connect();
    try {
      await client.query(`SET TIME ZONE '${this.config.storeTimezone || 'UTC'}'`);

      const statementTimeout: number = this.config.executionTimeout ? this.config.executionTimeout * 1000 : 600000;
      await client.query(`set statement_timeout to ${statementTimeout}`);

      const res = await client.query({
        text: query,
        values: values || [],
        types: {
          getTypeParser: (dataType, format) => {
            const isTimestamp = timestampDataTypes.indexOf(dataType) > -1;
            let parser = types.getTypeParser(dataType, format);

            if (isTimestamp) {
              parser = timestampTypeParser;
            }

            return (val: any) => parser(val);
          },
        },
      });
      return res;
    } finally {
      await client.release();
    }
  }

  public async query(query: string, values: unknown[]) {
    const result = await this.queryResponse(query, values);
    return result.rows;
  }

  public async downloadQueryResults(query: string, values: unknown[], options: DownloadQueryResults) {
    const res = await this.queryResponse(query, values);
    return {
      rows: res.rows,
      types: res.fields.map(f => ({
        name: f.name,
        type: this.toGenericType(DataTypeMapping[f.dataTypeID].toLowerCase())
      })),
    };
  }

  public readOnly() {
    return !!this.config.readOnly;
  }

  public async uploadTableWithIndexes(table: string, columns: TableStructure, tableData: DownloadTableMemoryData, indexesSql: IndexesSQL) {
    if (!tableData.rows) {
      throw new Error(`${this.constructor} driver supports only rows upload`);
    }

    await this.createTable(table, columns);

    try {
      await this.query(
        `INSERT INTO ${table}
      (${columns.map(c => this.quoteIdentifier(c.name)).join(', ')})
      SELECT * FROM UNNEST (${columns.map((c, columnIndex) => `${this.param(columnIndex)}::${this.fromGenericType(c.type)}[]`).join(', ')})`,
        columns.map(c => tableData.rows.map(r => r[c.name]))
      );

      for (let i = 0; i < indexesSql.length; i++) {
        const [query, p] = indexesSql[i].sql;
        await this.query(query, p);
      }
    } catch (e) {
      await this.dropTable(table);
      throw e;
    }
  }

  public release() {
    return this.pool.end();
  }

  public param(paramIndex: number) {
    return `$${paramIndex + 1}`;
  }

  public fromGenericType(columnType: string) {
    return GenericTypeToPostgres[columnType] || super.fromGenericType(columnType);
  }
}
