import { types, Pool, PoolConfig, PoolClient } from 'pg';
// eslint-disable-next-line import/no-extraneous-dependencies
import { TypeId, TypeFormat } from 'pg-types';
import * as moment from 'moment';
import {
  BaseDriver,
  DownloadQueryResultsOptions, DownloadTableMemoryData, DriverInterface,
  GenericDataBaseType, IndexesSQL, TableStructure, StreamOptions, StreamTableDataWithTypes, QueryOptions,
} from '@cubejs-backend/query-orchestrator';
import { QueryStream } from './QueryStream';

const GenericTypeToPostgres: Record<GenericDataBaseType, string> = {
  string: 'text',
  double: 'decimal',
};

const NativeTypeToPostgresType: Record<string, string> = {};

Object.entries(types.builtins).forEach(([key, value]) => {
  NativeTypeToPostgresType[value] = key;
});

const PostgresToGenericType: Record<string, GenericDataBaseType> = {
  // bpchar (“blank-padded char”, the internal name of the character data type)
  bpchar: 'varchar',
};

const timestampDataTypes = [
  // @link TypeId.DATE
  1082,
  // @link TypeId.TIMESTAMP
  1114,
  // @link TypeId.TIMESTAMPTZ
  1184
];
const timestampTypeParser = (val: any) => moment.utc(val).format(moment.HTML5_FMT.DATETIME_LOCAL_MS);

export type PostgresDriverConfiguration = Partial<PoolConfig> & {
  storeTimezone?: string,
  executionTimeout?: number,
  readOnly?: boolean,
};

function getTypeParser(dataType: TypeId, format: TypeFormat|undefined) {
  const isTimestamp = timestampDataTypes.includes(dataType);
  if (isTimestamp) {
    return timestampTypeParser;
  }

  const parser = types.getTypeParser(dataType, format);
  return (val: any) => parser(val);
}

export class PostgresDriver<Config extends PostgresDriverConfiguration = PostgresDriverConfiguration>
  extends BaseDriver implements DriverInterface {
  protected readonly pool: Pool;

  protected readonly config: Partial<Config>;

  public constructor(
    config: Partial<Config> = {}
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

    this.config = {
      ...this.getInitialConfiguration(),
      ...config,
    };
  }

  /**
   * The easiest way how to add additional configuration from env variables, because
   * you cannot call method in RedshiftDriver.constructor before super.
   */
  protected getInitialConfiguration(): Partial<PostgresDriverConfiguration> {
    return {
      readOnly: true,
    };
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

  protected async prepareConnection(
    conn: PoolClient,
    options: { executionTimeout: number } = {
      executionTimeout: this.config.executionTimeout ? <number>(this.config.executionTimeout) * 1000 : 600000
    }
  ) {
    await conn.query(`SET TIME ZONE '${this.config.storeTimezone || 'UTC'}'`);
    await conn.query(`SET statement_timeout TO ${options.executionTimeout}`);
  }

  public async stream(
    query: string,
    values: unknown[],
    { highWaterMark }: StreamOptions
  ): Promise<StreamTableDataWithTypes> {
    const conn = await this.pool.connect();

    try {
      await this.prepareConnection(conn);

      const queryStream = new QueryStream(query, values, {
        types: {
          getTypeParser,
        },
        highWaterMark
      });
      const rowStream: QueryStream = await conn.query(queryStream);
      const meta = await rowStream.fields();

      return {
        rowStream,
        types: meta.map((f: any) => ({
          name: f.name,
          type: this.toGenericType(NativeTypeToPostgresType[f.dataTypeID].toLowerCase())
        })),
        release: async () => {
          await conn.release();
        }
      };
    } catch (e) {
      await conn.release();

      throw e;
    }
  }

  protected async queryResponse(query: string, values: unknown[]) {
    const conn = await this.pool.connect();

    try {
      await this.prepareConnection(conn);

      const res = await conn.query({
        text: query,
        values: values || [],
        types: {
          getTypeParser,
        },
      });
      return res;
    } finally {
      await conn.release();
    }
  }

  public async query<R = unknown>(query: string, values: unknown[], options?: QueryOptions): Promise<R[]> {
    const result = await this.queryResponse(query, values);
    return result.rows;
  }

  public async downloadQueryResults(query: string, values: unknown[], options: DownloadQueryResultsOptions) {
    if (options.streamImport) {
      return this.stream(query, values, options);
    }

    const res = await this.queryResponse(query, values);
    return {
      rows: res.rows,
      types: res.fields.map(f => ({
        name: f.name,
        type: this.toGenericType(NativeTypeToPostgresType[f.dataTypeID].toLowerCase())
      })),
    };
  }

  public toGenericType(columnType: string): GenericDataBaseType {
    if (columnType in PostgresToGenericType) {
      return PostgresToGenericType[columnType];
    }

    return super.toGenericType(columnType);
  }

  public readOnly() {
    return !!this.config.readOnly;
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
