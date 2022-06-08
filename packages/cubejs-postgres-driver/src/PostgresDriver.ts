import { types, Pool, PoolConfig, PoolClient, FieldDef } from 'pg';
// eslint-disable-next-line import/no-extraneous-dependencies
import { TypeId, TypeFormat } from 'pg-types';
import { getEnv } from '@cubejs-backend/shared';
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
  // Revert mapping for internal pre-aggregations
  HLL_POSTGRES: 'hll',
};

const NativeTypeToPostgresType: Record<string, string> = {};

Object.entries(types.builtins).forEach(([key, value]) => {
  NativeTypeToPostgresType[value] = key;
});

const PostgresToGenericType: Record<string, GenericDataBaseType> = {
  // bpchar (“blank-padded char”, the internal name of the character data type)
  bpchar: 'varchar',
  // Numeric is an alias
  numeric: 'decimal',
  // External mapping
  hll: 'HLL_POSTGRES',
};

const timestampDataTypes = [
  // @link TypeId.DATE
  1082,
  // @link TypeId.TIMESTAMP
  1114,
  // @link TypeId.TIMESTAMPTZ
  1184
];
const timestampTypeParser = (val: string) => moment.utc(val).format(moment.HTML5_FMT.DATETIME_LOCAL_MS);
const hllTypeParser = (val: string) => Buffer.from(
  // Postgres uses prefix as \x for encoding
  val.slice(2),
  'hex'
).toString('base64');

export type PostgresDriverConfiguration = Partial<PoolConfig> & {
  storeTimezone?: string,
  executionTimeout?: number,
  readOnly?: boolean,
} & {
  poolSize: number
};

export class PostgresDriver<Config extends PostgresDriverConfiguration = PostgresDriverConfiguration>
  extends BaseDriver implements DriverInterface {
  /**
   * Returns default concurrency value.
   */
  public static getDefaultConcurrency(): number {
    return 40;
  }

  protected readonly pool: Pool;

  protected readonly config: Partial<Config>;

  public constructor(
    config: Partial<Config> = {}
  ) {
    super();

    this.pool = new Pool({
      max:
        process.env.CUBEJS_DB_MAX_POOL && parseInt(process.env.CUBEJS_DB_MAX_POOL, 10) ||
        config.poolSize ||
        8,
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
      executionTimeout: getEnv('dbQueryTimeout'),
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

  protected getTypeParser = (dataTypeID: TypeId, format: TypeFormat | undefined) => {
    const isTimestamp = timestampDataTypes.includes(dataTypeID);
    if (isTimestamp) {
      return timestampTypeParser;
    }

    const typeName = this.getPostgresTypeForField(dataTypeID);
    if (typeName === 'hll') {
      // We are using base64 encoding as main format for all HLL sketches, but in pg driver it uses binary encoding
      return hllTypeParser;
    }

    const parser = types.getTypeParser(dataTypeID, format);
    return (val: any) => parser(val);
  };

  /**
   * It's not possible to detect user defined types via constant oids
   * For example HLL extensions is using CREATE TYPE HLL which will generate a new pg_type with different oids
   */
  protected userDefinedTypes: Record<string, string> | null = null;

  protected getPostgresTypeForField(dataTypeID: number): string | null {
    if (dataTypeID in NativeTypeToPostgresType) {
      return NativeTypeToPostgresType[dataTypeID].toLowerCase();
    }

    if (this.userDefinedTypes && dataTypeID in this.userDefinedTypes) {
      return this.userDefinedTypes[dataTypeID].toLowerCase();
    }

    return null;
  }

  public async testConnection(): Promise<void> {
    try {
      await this.pool.query('SELECT $1::int AS number', ['1']);
    } catch (e) {
      if ((e as Error).toString().indexOf('no pg_hba.conf entry for host') !== -1) {
        throw new Error(`Please use CUBEJS_DB_SSL=true to connect: ${(e as Error).toString()}`);
      }

      throw e;
    }
  }

  protected async loadUserDefinedTypes(conn: PoolClient): Promise<void> {
    if (!this.userDefinedTypes) {
      // Postgres enum types defined as typcategory = 'E' these can be assumed
      // to be of type varchar for the drivers purposes.
      // TODO: if full implmentation the constraints can be looked up via pg_enum
      // https://www.postgresql.org/docs/9.1/catalog-pg-enum.html
      const customTypes = await conn.query(
        `SELECT
            oid,
            CASE
                WHEN typcategory = 'E' THEN 'varchar'
                ELSE typname
            END
        FROM
            pg_type
        WHERE
            typcategory in ('U', 'E')`,
        []
      );

      this.userDefinedTypes = customTypes.rows.reduce(
        (prev, current) => ({ [current.oid]: current.typname, ...prev }),
        {}
      );
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

    await this.loadUserDefinedTypes(conn);
  }

  protected mapFields(fields: FieldDef[]) {
    return fields.map((f) => {
      const postgresType = this.getPostgresTypeForField(f.dataTypeID);
      if (!postgresType) {
        throw new Error(
          `Unable to detect type for field "${f.name}" with dataTypeID: ${f.dataTypeID}`
        );
      }

      return ({
        name: f.name,
        type: this.toGenericType(postgresType)
      });
    });
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
          getTypeParser: this.getTypeParser,
        },
        highWaterMark
      });
      const rowStream: QueryStream = await conn.query(queryStream);
      const meta = await rowStream.fields();

      return {
        rowStream,
        types: this.mapFields(meta),
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
          getTypeParser: this.getTypeParser,
        },
      });
      return res;
    } finally {
      await conn.release();
    }
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
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
      types: this.mapFields(res.fields),
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
