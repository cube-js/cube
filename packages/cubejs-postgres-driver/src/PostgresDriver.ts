/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `PostgresDriver` and related types declaration.
 */

import {
  getEnv,
  assertDataSource,
} from '@cubejs-backend/shared';
import { types, Pool, PoolConfig, PoolClient, FieldDef } from 'pg';
// eslint-disable-next-line import/no-extraneous-dependencies
import { TypeId, TypeFormat } from 'pg-types';
import * as moment from 'moment';
import {
  BaseDriver,
  DownloadQueryResultsOptions, DownloadTableMemoryData, DriverInterface,
  GenericDataBaseType, IndexesSQL, TableStructure, StreamOptions,
  StreamTableDataWithTypes, QueryOptions, DownloadQueryResultsResult, DriverCapabilities, TableColumn,
} from '@cubejs-backend/base-driver';
import { QueryStream } from './QueryStream';

const GenericTypeToPostgres: Record<GenericDataBaseType, string> = {
  string: 'text',
  double: 'decimal',
  int: 'int8',
  // Revert mapping for internal pre-aggregations
  HLL_POSTGRES: 'hll',
};

const NativeTypeToPostgresType: Record<string, string> = {};

Object.entries(types.builtins).forEach(([key, value]) => {
  NativeTypeToPostgresType[value] = key;
});
// pg-types lacks the default `unknown` type since it's a pseudo-type
NativeTypeToPostgresType['705'] = 'UNKNOWN';

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

  /**
   * The export bucket CSV file escape symbol.
   */
  exportBucketCsvEscapeSymbol?: string,
};

/**
 * Postgres driver class.
 */
export class PostgresDriver<Config extends PostgresDriverConfiguration = PostgresDriverConfiguration>
  extends BaseDriver implements DriverInterface {
  /**
   * Returns default concurrency value.
   */
  public static getDefaultConcurrency(): number {
    return 2;
  }

  private enabled: boolean = false;

  protected readonly pool: Pool;

  protected readonly config: Partial<Config>;

  /**
   * Class constructor.
   */
  public constructor(
    config: Partial<Config> & {
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
    } = {}
  ) {
    super({
      testConnectionTimeout: config.testConnectionTimeout,
    });

    const dataSource =
      config.dataSource ||
      assertDataSource('default');

    this.pool = new Pool({
      idleTimeoutMillis: 30000,
      max:
        config.maxPoolSize ||
        getEnv('dbMaxPoolSize', { dataSource }) ||
        8,
      host: getEnv('dbHost', { dataSource }),
      database: getEnv('dbName', { dataSource }),
      port: getEnv('dbPort', { dataSource }),
      user: getEnv('dbUser', { dataSource }),
      password: getEnv('dbPass', { dataSource }),
      ssl: this.getSslOptions(dataSource),
      ...config
    });
    this.pool.on('error', (err) => {
      console.log(`Unexpected error on idle client: ${err.stack || err}`); // TODO
    });
    this.config = <Partial<Config>>{
      ...this.getInitialConfiguration(dataSource),
      executionTimeout: getEnv('dbQueryTimeout', { dataSource }),
      exportBucketCsvEscapeSymbol: getEnv('dbExportBucketCsvEscapeSymbol', { dataSource }),
      ...config,
    };
    this.enabled = true;
  }

  protected primaryKeysQuery(conditionString?: string): string | null {
    return `SELECT
      columns.table_schema as ${this.quoteIdentifier('table_schema')},
      columns.table_name as ${this.quoteIdentifier('table_name')},
      columns.column_name as ${this.quoteIdentifier('column_name')}
    FROM information_schema.table_constraints tc
    JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)
    JOIN information_schema.columns AS columns ON columns.table_schema = tc.constraint_schema
      AND tc.table_name = columns.table_name AND ccu.column_name = columns.column_name
    WHERE constraint_type = 'PRIMARY KEY' AND columns.table_schema NOT IN ('pg_catalog', 'information_schema', 'mysql', 'performance_schema', 'sys', 'INFORMATION_SCHEMA')${conditionString ? ` AND (${conditionString})` : ''}`;
  }

  protected foreignKeysQuery(conditionString?: string): string | null {
    return `SELECT
        tc.table_schema as ${this.quoteIdentifier('table_schema')},
        tc.table_name as ${this.quoteIdentifier('table_name')},
        kcu.column_name as ${this.quoteIdentifier('column_name')},
        columns.table_name as ${this.quoteIdentifier('target_table')},
        columns.column_name as ${this.quoteIdentifier('target_column')}
      FROM
        information_schema.table_constraints AS tc
      JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
      JOIN information_schema.constraint_column_usage AS columns
        ON columns.constraint_name = tc.constraint_name
      WHERE
         constraint_type = 'FOREIGN KEY'
         AND ${this.getColumnNameForSchemaName()} NOT IN ('pg_catalog', 'information_schema', 'mysql', 'performance_schema', 'sys', 'INFORMATION_SCHEMA')
         ${conditionString ? ` AND (${conditionString})` : ''}
    `;
  }

  /**
   * The easiest way how to add additional configuration from env variables, because
   * you cannot call method in RedshiftDriver.constructor before super.
   */
  protected getInitialConfiguration(
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    dataSource: string,
  ): Partial<PostgresDriverConfiguration> {
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
    PostgresDriver.checkValuesLimit(values);

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
      const fields = await await rowStream.fields();

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

  protected static checkValuesLimit(values?: unknown[]) {
    // PostgreSQL protocol allows sending up to 65535 params in a single bind message
    // See https://github.com/postgres/postgres/blob/REL_16_0/src/backend/tcop/postgres.c#L1698-L1708
    // See https://github.com/postgres/postgres/blob/REL_16_0/src/backend/libpq/pqformat.c#L428-L431
    // But 'pg' module does not check for params count, and ends up sending incorrect bind message
    // See https://github.com/brianc/node-postgres/blob/92cb640fd316972e323ced6256b2acd89b1b58e0/packages/pg-protocol/src/serializer.ts#L155
    // See https://github.com/brianc/node-postgres/blob/92cb640fd316972e323ced6256b2acd89b1b58e0/packages/pg-protocol/src/buffer-writer.ts#L32-L37
    const length = (values?.length ?? 0);
    if (length >= 65536) {
      throw new Error(`PostgreSQL protocol does not support more than 65535 parameters, but ${length} passed`);
    }
  }

  protected async queryResponse(query: string, values: unknown[]) {
    PostgresDriver.checkValuesLimit(values);

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

  public async createTable(quotedTableName: string, columns: TableColumn[]): Promise<void> {
    if (quotedTableName.length > 63) {
      throw new Error('PostgreSQL can not work with table names longer than 63 symbols. ' +
        `Consider using the 'sqlAlias' attribute in your cube definition for ${quotedTableName}.`);
    }
    return super.createTable(quotedTableName, columns);
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  public async query<R = unknown>(query: string, values: unknown[], options?: QueryOptions): Promise<R[]> {
    const result = await this.queryResponse(query, values);
    return result.rows;
  }

  public async downloadQueryResults(query: string, values: unknown[], options: DownloadQueryResultsOptions): Promise<DownloadQueryResultsResult> {
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

  public async release() {
    if (this.enabled) {
      this.pool.end();
      this.enabled = false;
    }
  }

  public param(paramIndex: number) {
    return `$${paramIndex + 1}`;
  }

  public fromGenericType(columnType: string) {
    return GenericTypeToPostgres[columnType] || super.fromGenericType(columnType);
  }

  public capabilities(): DriverCapabilities {
    return {
      incrementalSchemaLoading: true,
    };
  }
}
