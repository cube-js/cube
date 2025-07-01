/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `MSSqlDriver` and related types declaration.
 */

import sql, { ConnectionPool, config as MsSQLConfig } from 'mssql';
import {
  getEnv,
  assertDataSource,
} from '@cubejs-backend/shared';
import {
  BaseDriver,
  DriverInterface,
  StreamOptions,
  DownloadQueryResultsOptions,
  TableStructure,
  DriverCapabilities,
  DownloadQueryResultsResult, TableColumnQueryResult,
} from '@cubejs-backend/base-driver';
import { QueryStream } from './QueryStream';

// ********* Value converters ***************** //
const numericTypes = [
  sql.TYPES.Int,
  sql.TYPES.BigInt,
  sql.TYPES.SmallInt,
  sql.TYPES.TinyInt,
  sql.TYPES.Decimal,
  sql.TYPES.Numeric,
  sql.TYPES.Float,
  sql.TYPES.Real,
  sql.TYPES.Money,
  sql.TYPES.SmallMoney
];

for (const type of numericTypes) {
  sql.valueHandler.set(type, (value) => (value != null ? String(value) : value));
}

export type MSSqlDriverConfiguration = Omit<MsSQLConfig, 'server'> & {
  readOnly?: boolean;
  server?: string;
};

const GenericTypeToMSSql: Record<string, string> = {
  boolean: 'bit',
  string: 'nvarchar(max)',
  text: 'nvarchar(max)',
  timestamp: 'datetime2',
  uuid: 'uniqueidentifier'
};

const MSSqlToGenericType: Record<string, string> = {
  bit: 'boolean',
  uniqueidentifier: 'uuid',
  datetime2: 'timestamp'
};

/**
 * MS SQL driver class.
 */
export class MSSqlDriver extends BaseDriver implements DriverInterface {
  private readonly connectionPool: ConnectionPool;

  private readonly initialConnectPromise: Promise<ConnectionPool>;

  private readonly config: MSSqlDriverConfiguration;

  /**
   * Returns default concurrency value.
   */
  public static getDefaultConcurrency() {
    return 2;
  }

  /**
   * Class constructor.
   */
  public constructor(config: MSSqlDriverConfiguration & {
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
      server?: string,
    } = {}) {
    super({
      testConnectionTimeout: config.testConnectionTimeout,
    });

    const dataSource =
      config.dataSource ||
      assertDataSource('default');

    /**
     * @type {import('mssql').config}
     */
    this.config = {
      readOnly: true,
      server: getEnv('dbHost', { dataSource }),
      database: getEnv('dbName', { dataSource }),
      port: getEnv('dbPort', { dataSource }),
      user: getEnv('dbUser', { dataSource }),
      password: getEnv('dbPass', { dataSource }),
      domain: getEnv('dbDomain', { dataSource }),
      requestTimeout: getEnv('dbQueryTimeout') * 1000,
      options: {
        encrypt: getEnv('dbSsl', { dataSource }),
        useUTC: false
      },
      pool: {
        max:
          config.maxPoolSize ||
          getEnv('dbMaxPoolSize', { dataSource }) ||
          8,
        min: 0,
        idleTimeoutMillis: 30 * 1000,
        acquireTimeoutMillis: 20 * 1000
      },
      ...config
    };
    const { readOnly, ...poolConfig } = this.config;
    this.connectionPool = new ConnectionPool(poolConfig as MsSQLConfig);
    this.initialConnectPromise = this.connectionPool.connect();
  }

  /**
   * Returns the configurable driver options
   * Note: It returns the unprefixed option names.
   * In case of using multi sources options need to be prefixed manually.
   */
  public static driverEnvVariables() {
    return [
      'CUBEJS_DB_HOST',
      'CUBEJS_DB_NAME',
      'CUBEJS_DB_PORT',
      'CUBEJS_DB_USER',
      'CUBEJS_DB_PASS',
      'CUBEJS_DB_DOMAIN',
    ];
  }

  public async testConnection() {
    const conn = await this.initialConnectPromise.then((pool: ConnectionPool) => pool.request());
    await conn.query('SELECT 1 as number');
  }

  /**
   * Executes query in streaming mode.
   *
   * @param {string} query
   * @param {Array} values
   * @param {{ highWaterMark: number? }} options
   * @return {Promise<StreamTableDataWithTypes>}
   */
  public async stream(query: string, values: unknown[], { highWaterMark }: StreamOptions) {
    const pool = await this.initialConnectPromise;
    const request = pool.request();

    request.stream = true;
    (values || []).forEach((v, i) => {
      request.input(`_${i + 1}`, v);
    });
    request.query(query);

    const stream = new QueryStream(request, highWaterMark);
    const fields: TableStructure = await new Promise((resolve, reject) => {
      request.on('recordset', (columns) => {
        resolve(this.mapFields(columns));
      });
      request.on('error', (err: Error) => {
        reject(err);
      });
      stream.on('error', (err: Error) => {
        reject(err);
      });
    });
    return {
      rowStream: stream,
      types: fields,
      release: async () => {
        request.cancel();
      },
    };
  }

  /**
   * @param {{
   *   [name: string]: {
   *     index: number,
   *     name: string,
   *     type: *,
   *     nullable: boolean,
   *     caseSensitive: boolean,
   *     identity: boolean,
   *     readOnly: boolean,
   *     length: number?,
   *     scale: number?,
   *     precision: number?
   *   }
   * }} fields
   */
  private mapFields(fields: Record<string, any>) {
    return Object.keys(fields).map((field) => {
      let type;
      switch (fields[field].type) {
        case sql.Bit:
          type = 'boolean';
          break;
        // integers
        case sql.Int:
        case sql.SmallInt:
        case sql.TinyInt:
        case sql.BigInt:
          type = 'int';
          break;
        // float
        case sql.Money:
        case sql.SmallMoney:
        case sql.Numeric:
        case sql.Decimal:
          type = 'decimal';
          break;
        // double
        case sql.Real:
        case sql.Float:
          type = 'double';
          break;
        // strings
        case sql.Char:
        case sql.NChar:
        case sql.Text:
        case sql.NText:
        case sql.VarChar:
        case sql.NVarChar:
        case sql.Xml:
          type = 'text';
          break;
        // date and time
        case sql.Time:
          type = 'time';
          break;
        case sql.Date:
          type = 'timestamp';
          break;
        case sql.DateTime:
        case sql.DateTime2:
        case sql.SmallDateTime:
        case sql.DateTimeOffset:
          type = 'timestamp';
          break;
        // others
        case sql.UniqueIdentifier:
        case sql.Variant:
        case sql.Binary:
        case sql.VarBinary:
        case sql.Image:
        case sql.UDT:
        case sql.Geography:
        case sql.Geometry:
        case sql.TVP:
          type = 'string';
          break;
        // unknown
        default:
          type = 'string';
          break;
      }
      return { name: fields[field].name, type: this.toGenericType(type) };
    });
  }

  public async query(query: string, values: unknown[]) {
    let cancelFn: (() => void) | null = null;
    const promise: any = this.initialConnectPromise.then((pool) => {
      const request = pool.request();
      (values || []).forEach((v, i) => request.input(`_${i + 1}`, v));

      // TODO time zone UTC set in driver ?

      cancelFn = () => request.cancel();
      return request.query(query).then(res => res.recordset);
    });
    promise.cancel = () => cancelFn && cancelFn();
    return promise;
  }

  public param(paramIndex: number): string {
    return `@_${paramIndex + 1}`;
  }

  public async tableColumnTypes(table: string): Promise<TableStructure> {
    const [schema, name] = table.split('.');

    const columns: TableColumnQueryResult[] = await this.query(
      `SELECT column_name as ${this.quoteIdentifier('column_name')},
             table_name as ${this.quoteIdentifier('table_name')},
             table_schema as ${this.quoteIdentifier('table_schema')},
             data_type  as ${this.quoteIdentifier('data_type')}
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE table_name = ${this.param(0)} AND table_schema = ${this.param(1)}`,
      [name, schema]
    );

    return columns.map(c => ({ name: c.column_name, type: this.toGenericType(c.data_type) }));
  }

  public getTablesQuery(schemaName: string) {
    return this.query(
      `SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = ${this.param(0)}`,
      [schemaName]
    );
  }

  public async createSchemaIfNotExists(schemaName: string): Promise<void> {
    return this.query(
      `SELECT schema_name FROM INFORMATION_SCHEMA.SCHEMATA WHERE schema_name = ${this.param(0)}`,
      [schemaName]
    ).then((schemas: string[]) => {
      if (schemas.length === 0) {
        return this.query(`CREATE SCHEMA ${schemaName}`, []);
      }
      return null;
    });
  }

  public informationSchemaQuery(): string {
    // fix The multipart identifier "columns.data_type" could not be bound
    return `
      SELECT column_name as ${this.quoteIdentifier('column_name')},
        table_name as ${this.quoteIdentifier('table_name')},
        table_schema as ${this.quoteIdentifier('table_schema')},
        data_type as ${this.quoteIdentifier('data_type')}
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE table_schema NOT IN ('information_schema', 'sys')
    `;
  }

  public async downloadQueryResults(query: string, values: unknown[], options: DownloadQueryResultsOptions): Promise<DownloadQueryResultsResult> {
    if (options?.streamImport) {
      return this.stream(query, values, options);
    }

    const result = await this.query(query, values);
    const types = Object.keys(result.columns).map((key) => ({
      name: result.columns[key].name,
      type: this.toGenericType(result.columns[key].type.declaration),
    }));

    return {
      rows: result,
      types,
    };
  }

  protected fromGenericType(columnType: string): string {
    return GenericTypeToMSSql[columnType] || super.fromGenericType(columnType);
  }

  protected toGenericType(columnType: string): string {
    return MSSqlToGenericType[columnType] || super.toGenericType(columnType);
  }

  public readOnly(): boolean {
    return !!this.config.readOnly;
  }

  public wrapQueryWithLimit(query: { query: string, limit: number}) {
    query.query = `SELECT TOP ${query.limit} * FROM (${query.query}) AS t`;
  }

  public capabilities(): DriverCapabilities {
    return {
      incrementalSchemaLoading: true,
    };
  }
}
