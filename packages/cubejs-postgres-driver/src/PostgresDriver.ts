/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `PostgresDriver` and related types declaration.
 */

import { getEnv, assertDataSource, Pool, type PoolUserOptions } from '@cubejs-backend/shared';
import { types, FieldDef, DatabaseError } from 'pg';
// eslint-disable-next-line import/no-extraneous-dependencies
import { TypeId, TypeFormat } from 'pg-types';
import {
  BaseDriver,
  DownloadQueryResultsOptions, DownloadTableMemoryData, DriverInterface,
  GenericDataBaseType, IndexesSQL, TableStructure, StreamOptions,
  StreamTableDataWithTypes, QueryOptions, DownloadQueryResultsResult, DriverCapabilities, TableColumn, createPoolName,
  TableColumnQueryResult, DatabaseStructure, QuerySchemasResult, QueryTablesResult, QueryColumnsResult,
} from '@cubejs-backend/base-driver';
import { QueryStream } from './QueryStream';
import { PgClient, PgClientConfig } from './PgClient';
import { ConnectionError, PostgresError } from './errors';
import { dateTypeParser, timestampTypeParser, timestampTzTypeParser } from './type-parsers';

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
  // External mapping
  hll: 'HLL_POSTGRES',
};

// Same list as BaseDriver's information_schema queries
const IGNORED_SCHEMAS = "'pg_catalog', 'information_schema', 'mysql', 'performance_schema', 'sys', 'INFORMATION_SCHEMA'";

// SQLSTATE classes of errors that say nothing about whether the server supports a query: connection exception,
// transaction rollback, insufficient resources, object not in prerequisite state, operator intervention, system error
const TRANSIENT_SQLSTATE_CLASSES = ['08', '40', '53', '55', '57', '58'];

// The server rejected the query itself, as opposed to a connection, pool or transient failure
const isQueryRejectedError = (e: unknown): boolean => e instanceof DatabaseError &&
  !TRANSIENT_SQLSTATE_CLASSES.includes((e.code || '').slice(0, 2));

const hllTypeParser = (val: string) => Buffer.from(
  // Postgres uses prefix as \x for encoding
  val.slice(2),
  'hex'
).toString('base64');

export type PostgresDriverConfiguration = PgClientConfig & PoolUserOptions & {
  // @deprecated Please use maxPoolSize
  max?: number | undefined;
  // @deprecated Please use minPoolSize
  min?: number | undefined;

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

  protected readonly pool: Pool<PgClient>;

  protected readonly config: Partial<Config>;

  public constructor(
    config: Partial<Config> & {
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
       * Min pool size value for the [cube]<-->[db] pool.
       */
      minPoolSize?: number,

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
    const preAggregations = config.preAggregations || false;

    const poolConfig: PgClientConfig = {
      host: getEnv('dbHost', { dataSource, preAggregations }),
      database: getEnv('dbName', { dataSource, preAggregations }),
      port: getEnv('dbPort', { dataSource, preAggregations }),
      user: getEnv('dbUser', { dataSource, preAggregations }),
      password: getEnv('dbPass', { dataSource, preAggregations }),
      ssl: this.getSslOptions(dataSource, preAggregations),
      ...config
    };

    const poolName = createPoolName('postgres', dataSource, preAggregations);
    this.pool = new Pool<PgClient>(poolName, {
      create: async () => this.createConnection(poolConfig, poolName),
      validate: async (client) => {
        if (client.isEnding() || client.isEnded()) {
          return false;
        }

        return client.isQueryable();
      },
      destroy: async (client) => {
        await client.end();
      },
    }, {
      min: config.minPoolSize ||
        config.min ||
        getEnv('dbMinPoolSize', { dataSource, preAggregations }) ||
        0,
      max:
        config.maxPoolSize ||
        config.max ||
        getEnv('dbMaxPoolSize', { dataSource, preAggregations }) ||
        8,
      evictionRunIntervalMillis: config.evictionRunIntervalMillis || 10000,
      softIdleTimeoutMillis: config.softIdleTimeoutMillis || 30000,
      idleTimeoutMillis: config.idleTimeoutMillis || 30000,
      acquireTimeoutMillis: config.acquireTimeoutMillis || 20000,
      testOnBorrow: true,
    });

    // https://github.com/coopernurse/node-pool/blob/ee5db9ddb54ce3a142fde3500116b393d4f2f755/README.md#L220-L226
    this.pool.on('factoryCreateError', (err) => this.databasePoolError(err));
    this.pool.on('factoryDestroyError', (err) => this.databasePoolError(err));

    this.config = <Partial<Config>>{
      ...this.getInitialConfiguration(dataSource, preAggregations),
      executionTimeout: getEnv('dbQueryTimeout', { dataSource, preAggregations }),
      exportBucketCsvEscapeSymbol: getEnv('dbExportBucketCsvEscapeSymbol', { dataSource, preAggregations }),
      ...config,
    };
    this.enabled = true;
  }

  protected async createConnection(poolConfig: PgClientConfig, poolName: string): Promise<PgClient> {
    const client = new PgClient(poolConfig);
    client.on('error', (err) => this.databasePoolError(err));

    try {
      await client.connect();
    } catch (e: unknown) {
      throw new ConnectionError(e as Error, poolName);
    }

    return client;
  }

  /**
   * Whether the pg_catalog query for materialized views works on this server: undefined until a schema
   * query has run, false once it failed where the information_schema-only query works.
   */
  protected materializedViewsSupported: boolean | undefined = undefined;

  /**
   * information_schema doesn't list materialized views, so the schema listing adds them from pg_catalog.
   * Postgres-compatible servers that connect through this driver and lack that catalog fall back to
   * information_schema alone, see withMaterializedViews(). Subclasses that already know the answer override this.
   */
  protected listMaterializedViews(): boolean {
    return this.materializedViewsSupported !== false;
  }

  /**
   * Runs a schema query that includes materialized views. If the server rejects it, which Postgres-compatible
   * databases such as RisingWave do, retries without them and keeps doing so for this driver. Connection, pool
   * and transient errors are rethrown as they are, and so is any error once the query has worked.
   */
  protected async withMaterializedViews<T>(fn: () => Promise<T>): Promise<T> {
    if (!this.listMaterializedViews()) {
      return fn();
    }

    let rejection: unknown;

    try {
      const result = await fn();
      this.materializedViewsSupported = true;
      return result;
    } catch (e) {
      if (this.materializedViewsSupported || !isQueryRejectedError(e)) {
        throw e;
      }
      rejection = e;
    }

    this.materializedViewsSupported = false;

    let result: T;

    try {
      result = await fn();
    } catch (e) {
      // The query without materialized views fails too, so the first failure wasn't about them
      this.materializedViewsSupported = undefined;
      throw e;
    }

    if (this.logger) {
      this.logger('Materialized views query failed. Listing information_schema only', {
        error: ((rejection as Error)?.stack || String(rejection)).toString(),
      });
    }

    return result;
  }

  // Mirrors the information_schema.tables row filter: tables the role owns or holds any privilege on.
  // Skips relations information_schema already lists, as Postgres-compatible databases like CockroachDB do.
  protected materializedViewsQuery(): string {
    return `SELECT n.nspname AS table_schema, c.relname AS table_name
      FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relkind = 'm'
        AND (pg_catalog.pg_has_role(c.relowner, 'USAGE')
          OR pg_catalog.has_table_privilege(c.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
          OR pg_catalog.has_any_column_privilege(c.oid, 'SELECT, INSERT, UPDATE, REFERENCES'))
        AND ${this.notInInformationSchema()}`;
  }

  // Mirrors how information_schema.columns derives data_type, numeric_precision and numeric_scale,
  // and its row filter, so a materialized view's columns match those of a view with the same definition.
  protected materializedViewColumnsQuery(): string {
    return `SELECT n.nspname AS table_schema, c.relname AS table_name, a.attname AS column_name,
        a.attnum AS ordinal_position,
        CASE
          WHEN t.typtype = 'd' THEN CASE
            WHEN bt.typelem <> 0 AND bt.typlen = -1 THEN 'ARRAY'
            WHEN bn.nspname = 'pg_catalog' THEN pg_catalog.format_type(t.typbasetype, NULL)
            ELSE 'USER-DEFINED'
          END
          ELSE CASE
            WHEN t.typelem <> 0 AND t.typlen = -1 THEN 'ARRAY'
            WHEN tn.nspname = 'pg_catalog' THEN pg_catalog.format_type(a.atttypid, NULL)
            ELSE 'USER-DEFINED'
          END
        END AS data_type,
        information_schema._pg_numeric_precision(
          information_schema._pg_truetypid(a, t), information_schema._pg_truetypmod(a, t)
        ) AS numeric_precision,
        information_schema._pg_numeric_scale(
          information_schema._pg_truetypid(a, t), information_schema._pg_truetypmod(a, t)
        ) AS numeric_scale
      FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
      JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
      JOIN pg_catalog.pg_namespace tn ON tn.oid = t.typnamespace
      LEFT JOIN (pg_catalog.pg_type bt JOIN pg_catalog.pg_namespace bn ON bn.oid = bt.typnamespace)
        ON t.typtype = 'd' AND bt.oid = t.typbasetype
      WHERE c.relkind = 'm' AND a.attnum > 0 AND NOT a.attisdropped
        AND (pg_catalog.pg_has_role(c.relowner, 'USAGE')
          OR pg_catalog.has_column_privilege(c.oid, a.attnum, 'SELECT, INSERT, UPDATE, REFERENCES'))
        AND ${this.notInInformationSchema()}`;
  }

  // Condition on the pg_class row `c` and its pg_namespace row `n`
  protected notInInformationSchema(): string {
    return `NOT EXISTS (
          SELECT 1 FROM information_schema.tables it
          WHERE it.table_schema = n.nspname AND it.table_name = c.relname
        )`;
  }

  protected tablesSource(): string {
    return `(
      SELECT table_schema, table_name FROM information_schema.tables
      UNION ALL
      ${this.materializedViewsQuery()}
    )`;
  }

  protected columnsSource(): string {
    return `(
      SELECT table_schema, table_name, column_name, ordinal_position, data_type, numeric_precision, numeric_scale
      FROM information_schema.columns
      UNION ALL
      ${this.materializedViewColumnsQuery()}
    )`;
  }

  protected override informationSchemaQuery() {
    if (!this.listMaterializedViews()) {
      return super.informationSchemaQuery();
    }

    return `
      SELECT columns.column_name as ${this.quoteIdentifier('column_name')},
             columns.table_name as ${this.quoteIdentifier('table_name')},
             columns.table_schema as ${this.quoteIdentifier('table_schema')},
             columns.data_type as ${this.quoteIdentifier('data_type')}
      FROM ${this.columnsSource()} AS columns
      WHERE columns.table_schema NOT IN (${IGNORED_SCHEMAS})
   `;
  }

  protected override getSchemasQuery() {
    if (!this.listMaterializedViews()) {
      return super.getSchemasQuery();
    }

    return `
      SELECT tables.table_schema as ${this.quoteIdentifier('schema_name')}
      FROM ${this.tablesSource()} AS tables
      WHERE tables.table_schema NOT IN (${IGNORED_SCHEMAS})
      GROUP BY tables.table_schema
    `;
  }

  protected override getTablesForSpecificSchemasQuery(schemasPlaceholders: string) {
    if (!this.listMaterializedViews()) {
      return super.getTablesForSpecificSchemasQuery(schemasPlaceholders);
    }

    return `
      SELECT tables.table_schema as ${this.quoteIdentifier('schema_name')},
             tables.table_name as ${this.quoteIdentifier('table_name')}
      FROM ${this.tablesSource()} AS tables
      WHERE tables.table_schema IN (${schemasPlaceholders})
    `;
  }

  protected override getColumnsForSpecificTablesQuery(conditionString: string) {
    if (!this.listMaterializedViews()) {
      return super.getColumnsForSpecificTablesQuery(conditionString);
    }

    return `
      SELECT columns.column_name as ${this.quoteIdentifier('column_name')},
             columns.table_name as ${this.quoteIdentifier('table_name')},
             columns.table_schema as ${this.quoteIdentifier('schema_name')},
             columns.data_type as ${this.quoteIdentifier('data_type')}
      FROM ${this.columnsSource()} AS columns
      WHERE ${conditionString}
    `;
  }

  public override async tablesSchema(): Promise<DatabaseStructure> {
    return this.withMaterializedViews(() => super.tablesSchema());
  }

  public override async getSchemas(): Promise<QuerySchemasResult[]> {
    return this.withMaterializedViews(() => super.getSchemas());
  }

  public override async getTablesForSpecificSchemas(schemas: QuerySchemasResult[]): Promise<QueryTablesResult[]> {
    return this.withMaterializedViews(() => super.getTablesForSpecificSchemas(schemas));
  }

  public override async getColumnsForSpecificTables(tables: QueryTablesResult[]): Promise<QueryColumnsResult[]> {
    return this.withMaterializedViews(() => super.getColumnsForSpecificTables(tables));
  }

  public override async tableColumnTypesWithPrecision(table: string): Promise<TableStructure> {
    return this.withMaterializedViews(() => (
      this.listMaterializedViews()
        ? this.tableColumnTypesWithMaterializedViews(table)
        : super.tableColumnTypesWithPrecision(table)
    ));
  }

  protected async tableColumnTypesWithMaterializedViews(table: string): Promise<TableStructure> {
    const [schema, name] = table.split('.');

    const columns = await this.query<TableColumnQueryResult>(
      `SELECT columns.column_name as ${this.quoteIdentifier('column_name')},
             columns.table_name as ${this.quoteIdentifier('table_name')},
             columns.table_schema as ${this.quoteIdentifier('table_schema')},
             columns.data_type as ${this.quoteIdentifier('data_type')},
             columns.numeric_precision as ${this.quoteIdentifier('numeric_precision')},
             columns.numeric_scale as ${this.quoteIdentifier('numeric_scale')}
      FROM ${this.columnsSource()} AS columns
      WHERE columns.table_name = ${this.param(0)} AND columns.table_schema = ${this.param(1)}
      ${getEnv('fetchColumnsByOrdinalPosition') ? 'ORDER BY columns.ordinal_position' : ''}`,
      [name, schema]
    );

    return columns.map(c => ({ name: c.column_name, type: this.toGenericType(c.data_type, c.numeric_precision, c.numeric_scale) }));
  }

  // pg_catalog, not information_schema: constraint_column_usage hides tables the role doesn't own, and
  // constraint names are only unique per table. `columns` is the alias getColumnsForSpecificTables() filters on.
  protected primaryKeysQuery(conditionString?: string): string | null {
    return `SELECT
        columns.table_schema as ${this.quoteIdentifier('table_schema')},
        columns.table_name as ${this.quoteIdentifier('table_name')},
        columns.column_name as ${this.quoteIdentifier('column_name')}
      FROM (
        SELECT n.nspname AS table_schema, c.relname AS table_name, a.attname AS column_name
        FROM pg_catalog.pg_constraint con
        CROSS JOIN LATERAL unnest(con.conkey) AS k(attnum)
        JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_catalog.pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
        WHERE con.contype = 'p'
      ) AS columns
      WHERE columns.table_schema NOT IN ('pg_catalog', 'information_schema')${conditionString ? ` AND (${conditionString})` : ''}`;
  }

  protected foreignKeysQuery(conditionString?: string): string | null {
    // Postgres 12+ clones an FK per partition of a referenced table on the same conrelid; joining the parent drops them.
    // conparentid only exists since Postgres 11 (Greenplum 6 is 9.4), and row_to_json yields NULL where it's missing.
    // TODO: use `con.conparentid` once Postgres 11 is the minimum.
    const parentConstraintOid = "(row_to_json(con) ->> 'conparentid')::oid";

    return `SELECT
        columns.table_schema as ${this.quoteIdentifier('table_schema')},
        columns.table_name as ${this.quoteIdentifier('table_name')},
        columns.column_name as ${this.quoteIdentifier('column_name')},
        columns.target_table as ${this.quoteIdentifier('target_table')},
        columns.target_column as ${this.quoteIdentifier('target_column')}
      FROM (
        SELECT n.nspname AS table_schema, c.relname AS table_name, a.attname AS column_name,
          fc.relname AS target_table, fa.attname AS target_column
        FROM pg_catalog.pg_constraint con
        CROSS JOIN LATERAL unnest(con.conkey, con.confkey) AS k(attnum, fattnum)
        JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_catalog.pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
        JOIN pg_catalog.pg_class fc ON fc.oid = con.confrelid
        JOIN pg_catalog.pg_attribute fa ON fa.attrelid = con.confrelid AND fa.attnum = k.fattnum
        LEFT JOIN pg_catalog.pg_constraint parent ON parent.oid = ${parentConstraintOid}
        WHERE con.contype = 'f' AND parent.conrelid IS DISTINCT FROM con.conrelid
      ) AS columns
      WHERE columns.table_schema NOT IN ('pg_catalog', 'information_schema')${conditionString ? ` AND (${conditionString})` : ''}`;
  }

  /**
   * The easiest way how to add additional configuration from env variables, because
   * you cannot call method in RedshiftDriver.constructor before super.
   */
  protected getInitialConfiguration(
    _dataSource: string,
    _preAggregations?: boolean,
  ): Partial<PostgresDriverConfiguration> {
    return {
      readOnly: true,
    };
  }

  protected getTypeParser = (dataTypeID: TypeId, format: TypeFormat | undefined) => {
    // @link TypeId.DATE
    if (dataTypeID === 1082) {
      return dateTypeParser;
    }

    // @link TypeId.TIMESTAMP
    if (dataTypeID === 1114) {
      return timestampTypeParser;
    }

    // @link TypeId.TIMESTAMPTZ
    if (dataTypeID === 1184) {
      return timestampTzTypeParser;
    }

    const typeName = this.getPostgresTypeForField(dataTypeID);
    if (typeName === 'hll') {
      // We are using base64 encoding as main format for all HLL sketches, but in pg driver it uses binary encoding
      return hllTypeParser;
    }

    return types.getTypeParser(dataTypeID, format);
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
    // eslint-disable-next-line no-underscore-dangle
    const conn: PgClient = await this.pool._factory.create();

    try {
      await conn.query('SELECT $1::int AS number', ['1']);
    } catch (e) {
      if ((e as Error).toString().indexOf('no pg_hba.conf entry for host') !== -1) {
        throw new PostgresError(`Please use CUBEJS_DB_SSL=true to connect: ${(e as Error).toString()}`, { cause: e as Error });
      }

      throw e;
    } finally {
      // eslint-disable-next-line no-underscore-dangle
      await this.pool._factory.destroy(conn);
    }
  }

  protected async loadUserDefinedTypes(conn: PgClient): Promise<void> {
    if (!this.userDefinedTypes) {
      // Postgres enum types defined as typcategory = 'E' these can be assumed
      // to be of type varchar for the drivers purposes.
      // Postgres array types defined as typcategory = 'A' these can be assumed
      // to be of type text for the drivers purposes.
      // TODO: if full implmentation the constraints can be looked up via pg_enum
      // https://www.postgresql.org/docs/9.1/catalog-pg-enum.html
      //
      // Postgres creates a row type for every relation and an array type over it, so
      // selecting all of typcategory = 'A' returns one row per relation in the database.
      // That's why there is a NOT EXIST filter.
      const customTypes = await conn.query(
        `SELECT
            t.oid,
            CASE
                WHEN t.typcategory = 'E' THEN 'varchar'
                WHEN t.typcategory = 'A' THEN 'text'
                ELSE t.typname
            END AS typname
        FROM
            pg_type t
        WHERE
            t.typcategory in ('U', 'E')
            OR (
                t.typcategory = 'A'
                AND NOT EXISTS (
                    SELECT 1
                    FROM pg_type e
                    JOIN pg_class c ON c.oid = e.typrelid
                    WHERE e.oid = t.typelem AND e.typtype = 'c' AND c.relkind <> 'c'
                )
            )`,
        []
      );

      const userDefinedTypes: Record<string, string> = {};

      for (const row of customTypes.rows) {
        userDefinedTypes[row.oid] = row.typname;
      }

      this.userDefinedTypes = userDefinedTypes;
    }
  }

  protected async prepareConnection(
    conn: PgClient,
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
        throw new PostgresError(
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

    const conn = await this.pool.acquire();

    try {
      await this.prepareConnection(conn);

      const queryStream = new QueryStream(query, values, {
        types: {
          getTypeParser: this.getTypeParser,
        },
        highWaterMark
      });
      const rowStream: QueryStream = await conn.query(queryStream);
      const fields = await rowStream.fields();

      return {
        rowStream,
        types: this.mapFields(fields),
        release: async () => {
          await this.pool.release(conn);
        }
      };
    } catch (e) {
      await this.pool.release(conn);

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
      throw new PostgresError(`PostgreSQL protocol does not support more than 65535 parameters, but ${length} passed`);
    }
  }

  protected async withConnection<T>(fn: (conn: PgClient) => Promise<T>): Promise<T> {
    const conn = await this.pool.acquire();

    try {
      return await fn(conn);
    } finally {
      await this.pool.release(conn);
    }
  }

  protected async queryResponse(query: string, values: unknown[]) {
    PostgresDriver.checkValuesLimit(values);

    return this.withConnection(async (conn) => {
      await this.prepareConnection(conn);

      const res = await conn.query({
        text: query,
        values: values || [],
        types: {
          getTypeParser: this.getTypeParser,
        },
      });
      return res;
    });
  }

  public async createTable(quotedTableName: string, columns: TableColumn[]): Promise<void> {
    if (quotedTableName.length > 63) {
      throw new PostgresError('PostgreSQL can not work with table names longer than 63 symbols. ' +
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

  public override async tableColumnTypes(table: string): Promise<TableStructure> {
    return this.tableColumnTypesWithPrecision(table);
  }

  protected override toGenericType(columnType: string, precision?: number | null, scale?: number | null): GenericDataBaseType {
    return PostgresToGenericType[columnType.toLowerCase()] || super.toGenericType(columnType, precision, scale);
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
      throw new PostgresError(`${this.constructor} driver supports only rows upload`);
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
      await this.pool.drain();
      await this.pool.clear();
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
