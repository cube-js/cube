/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `BaseDriver` and related types declaration.
 */

import * as stream from 'stream';
import type { ConnectionOptions as TLSConnectionOptions } from 'tls';

import {
  getEnv,
  keyByDataSource,
  isFilePath,
  isSslKey,
  isSslCert,
} from '@cubejs-backend/shared';
import { reduce } from 'ramda';
import fs from 'fs';
import { cancelCombinator } from './utils';
import {
  ExternalCreateTableOptions,
  DownloadQueryResultsOptions,
  DownloadQueryResultsResult,
  DownloadTableCSVData,
  DownloadTableData,
  DownloadTableMemoryData,
  DriverInterface,
  ExternalDriverCompatibilities,
  IndexesSQL,
  isDownloadTableMemoryData,
  QueryOptions,
  Row,
  TableColumn,
  TableColumnQueryResult,
  TableQueryResult,
  TableStructure,
  DriverCapabilities
} from './driver.interface';

const sortByKeys = (unordered) => {
  const ordered = {};

  Object.keys(unordered).sort().forEach((key) => {
    ordered[key] = unordered[key];
  });

  return ordered;
};

const DbTypeToGenericType = {
  'timestamp without time zone': 'timestamp',
  'character varying': 'text',
  varchar: 'text',
  integer: 'int',
  nvarchar: 'text',
  text: 'text',
  string: 'text',
  boolean: 'boolean',
  bigint: 'bigint',
  time: 'string',
  datetime: 'timestamp',
  date: 'date',
  enum: 'text',
  'double precision': 'double',
  // PostgreSQL aliases, but maybe another databases support it
  int8: 'bigint',
  int4: 'int',
  int2: 'int',
  bool: 'boolean',
  float4: 'float',
  float8: 'double',
};

const DB_BIG_INT_MAX = BigInt('9223372036854775807');
const DB_BIG_INT_MIN = BigInt('-9223372036854775808');

const DB_INT_MAX = 2147483647;
const DB_INT_MIN = -2147483648;

// Order of keys is important here: from more specific to less specific
const DbTypeValueMatcher = {
  timestamp: (v) => v instanceof Date || v.toString().match(/^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d/),
  date: (v) => v instanceof Date || v.toString().match(/^\d\d\d\d-\d\d-\d\d$/),
  int: (v) => {
    if (Number.isInteger(v)) {
      return (v <= DB_INT_MAX && v >= DB_INT_MIN);
    }

    if (v.toString().match(/^[-]?\d+$/)) {
      const value = BigInt(v.toString());

      return value <= DB_INT_MAX && value >= DB_INT_MIN;
    }

    return false;
  },
  bigint: (v) => {
    if (Number.isInteger(v)) {
      return (v <= DB_BIG_INT_MAX && v >= DB_BIG_INT_MIN);
    }

    if (v.toString().match(/^[-]?\d+$/)) {
      const value = BigInt(v.toString());

      return value <= DB_BIG_INT_MAX && value >= DB_BIG_INT_MIN;
    }

    return false;
  },
  decimal: (v) => {
    if (v instanceof Number) {
      return true;
    }

    return v.toString().match(/^[-]?\d+(\.\d+)?$/);
  },
  boolean: (v) => v === false || v === true || v.toString().toLowerCase() === 'true' || v.toString().toLowerCase() === 'false',
  string: (v) => v.length < 256,
  text: () => true
};

/**
 * Base driver class.
 */
export abstract class BaseDriver implements DriverInterface {
  private testConnectionTimeoutValue = 10000;

  protected logger: any;

  /**
   * Class constructor.
   */
  public constructor(_options: {
    /**
     * Time to wait for a response from a connection after validation
     * request before determining it as not valid. Default - 10000 ms.
     */
    testConnectionTimeout?: number,
  } = {}) {
    this.testConnectionTimeoutValue = _options.testConnectionTimeout || 10000;
  }

  protected informationSchemaQuery() {
    return `
      SELECT columns.column_name as ${this.quoteIdentifier('column_name')},
             columns.table_name as ${this.quoteIdentifier('table_name')},
             columns.table_schema as ${this.quoteIdentifier('table_schema')},
             columns.data_type as ${this.quoteIdentifier('data_type')}
      FROM information_schema.columns
      WHERE columns.table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys', 'INFORMATION_SCHEMA')
   `;
  }

  protected getSslOptions(dataSource: string): TLSConnectionOptions | undefined {
    if (
      getEnv('dbSsl', { dataSource }) ||
      getEnv('dbSslRejectUnauthorized', { dataSource })
    ) {
      const sslOptions = [{
        name: 'ca',
        canBeFile: true,
        envKey: keyByDataSource('CUBEJS_DB_SSL_CA', dataSource),
        validate: isSslCert,
      }, {
        name: 'cert',
        canBeFile: true,
        envKey: keyByDataSource('CUBEJS_DB_SSL_CERT', dataSource),
        validate: isSslCert,
      }, {
        name: 'key',
        canBeFile: true,
        envKey: keyByDataSource('CUBEJS_DB_SSL_KEY', dataSource),
        validate: isSslKey,
      }, {
        name: 'ciphers',
        envKey: keyByDataSource('CUBEJS_DB_SSL_CIPHERS', dataSource),
      }, {
        name: 'passphrase',
        envKey: keyByDataSource('CUBEJS_DB_SSL_PASSPHRASE', dataSource),
      }, {
        name: 'servername',
        envKey: keyByDataSource('CUBEJS_DB_SSL_SERVERNAME', dataSource),
      }];

      const ssl: TLSConnectionOptions = sslOptions.reduce(
        (agg, { name, envKey, canBeFile, validate }) => {
          const value = process.env[envKey];
          if (value) {
            if (validate && validate(value)) {
              return {
                ...agg,
                ...{ [name]: value }
              };
            }

            if (canBeFile && isFilePath(value)) {
              if (!fs.existsSync(value)) {
                throw new Error(
                  `Unable to find ${name} from path: "${value}"`,
                );
              }

              const file = fs.readFileSync(value, 'utf8');
              if (validate(file)) {
                return {
                  ...agg,
                  ...{ [name]: file }
                };
              }

              throw new Error(
                `Content of the file from ${envKey} is not a valid SSL ${name}.`,
              );
            }

            throw new Error(
              `${envKey} is not a valid SSL ${name}. If it's a path, please specify it correctly`,
            );
          }

          return agg;
        },
        {}
      );

      ssl.rejectUnauthorized = getEnv('dbSslRejectUnauthorized', { dataSource });

      return ssl;
    }

    return undefined;
  }

  abstract testConnection(): Promise<void>;

  abstract query<R = unknown>(_query: string, _values?: unknown[], _options?: QueryOptions): Promise<R[]>;

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  public async streamQuery(sql: string, values: string[]): Promise<stream.Readable> {
    throw new TypeError('Driver\'s .streamQuery() method is not implemented yet.');
  }

  public async downloadQueryResults(query: string, values: unknown[], _options: DownloadQueryResultsOptions): Promise<DownloadQueryResultsResult> {
    const rows = await this.query<Row>(query, values);
    if (rows.length === 0) {
      throw new Error(
        'Unable to detect column types for pre-aggregation on empty values in readOnly mode.'
      );
    }

    const fields = Object.keys(rows[0]);

    const types = fields.map(field => ({
      name: field,
      type: Object.keys(DbTypeValueMatcher).find(
        type => !rows.filter(row => field in row).find(row => !DbTypeValueMatcher[type](row[field])) &&
          rows.find(row => field in row)
      ) || 'text'
    }));

    return {
      rows,
      types,
    };
  }

  public readOnly() {
    return false;
  }

  protected informationColumnsSchemaReducer(result, i) {
    let schema = (result[i.table_schema] || {});
    const tables = (schema[i.table_name] || []);

    tables.push({ name: i.column_name, type: i.data_type, attributes: i.key_type ? ['primaryKey'] : [] });

    tables.sort();
    schema[i.table_name] = tables;
    schema = sortByKeys(schema);
    result[i.table_schema] = schema;

    return sortByKeys(result);
  }

  public tablesSchema() {
    const query = this.informationSchemaQuery();

    return this.query(query).then(data => reduce(this.informationColumnsSchemaReducer, {}, data));
  }

  public async createSchemaIfNotExists(schemaName: string): Promise<void> {
    const schemas = await this.query(
      `SELECT schema_name FROM information_schema.schemata WHERE schema_name = ${this.param(0)}`,
      [schemaName]
    );

    if (schemas.length === 0) {
      await this.query(`CREATE SCHEMA IF NOT EXISTS ${schemaName}`);
    }
  }

  public getTablesQuery(schemaName: string) {
    return this.query<TableQueryResult>(
      `SELECT table_name FROM information_schema.tables WHERE table_schema = ${this.param(0)}`,
      [schemaName]
    );
  }

  public loadPreAggregationIntoTable(_preAggregationTableName: string, loadSql: string, params, options) {
    return this.query(loadSql, params, options);
  }

  public dropTable(tableName: string, options?: QueryOptions): Promise<unknown> {
    return this.query(`DROP TABLE ${tableName}`, [], options);
  }

  public param(_paramIndex: number): string {
    return '?';
  }

  public testConnectionTimeout() {
    return this.testConnectionTimeoutValue;
  }

  public async downloadTable(table: string, _options: ExternalDriverCompatibilities): Promise<DownloadTableMemoryData | DownloadTableCSVData> {
    return { rows: await this.query(`SELECT * FROM ${table}`) };
  }

  public async uploadTable(table: string, columns: TableStructure, tableData: DownloadTableData) {
    return this.uploadTableWithIndexes(table, columns, tableData, [], null, [], {});
  }

  public async uploadTableWithIndexes(table: string, columns: TableStructure, tableData: DownloadTableData, indexesSql: IndexesSQL, _uniqueKeyColumns: string[] | null, _queryTracingObj: any, _externalOptions: ExternalCreateTableOptions) {
    if (!isDownloadTableMemoryData(tableData)) {
      throw new Error(`${this.constructor} driver supports only rows upload`);
    }

    await this.createTable(table, columns);
    try {
      if (isDownloadTableMemoryData(tableData)) {
        for (let i = 0; i < tableData.rows.length; i++) {
          await this.query(
            `INSERT INTO ${table}
          (${columns.map(c => this.quoteIdentifier(c.name)).join(', ')})
          VALUES (${columns.map((c, paramIndex) => this.param(paramIndex)).join(', ')})`,
            columns.map(c => this.toColumnValue(tableData.rows[i][c.name] as string, c.type))
          );
        }
        for (let i = 0; i < indexesSql.length; i++) {
          const [query, params] = indexesSql[i].sql;
          await this.query(query, params);
        }
      }
    } catch (e) {
      await this.dropTable(table);
      throw e;
    }
  }

  protected toColumnValue(value: string, _genericType: string): string | boolean {
    return value;
  }

  public async tableColumnTypes(table: string): Promise<TableStructure> {
    const [schema, name] = table.split('.');

    const columns = await this.query<TableColumnQueryResult>(
      `SELECT columns.column_name as ${this.quoteIdentifier('column_name')},
             columns.table_name as ${this.quoteIdentifier('table_name')},
             columns.table_schema as ${this.quoteIdentifier('table_schema')},
             columns.data_type  as ${this.quoteIdentifier('data_type')}
      FROM information_schema.columns
      WHERE table_name = ${this.param(0)} AND table_schema = ${this.param(1)}`,
      [name, schema]
    );

    return columns.map(c => ({ name: c.column_name, type: this.toGenericType(c.data_type) }));
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  public async queryColumnTypes(sql: string, params?: unknown[]): Promise<{ name: any; type: string; }[]> {
    return [];
  }

  public createTable(quotedTableName: string, columns: TableColumn[]) {
    const createTableSql = this.createTableSql(quotedTableName, columns);
    return this.query(createTableSql, []).catch(e => {
      e.message = `Error during create table: ${createTableSql}: ${e.message}`;
      throw e;
    });
  }

  protected createTableSql(quotedTableName: string, columns: TableColumn[]) {
    const columnNames = columns.map(c => `${this.quoteIdentifier(c.name)} ${this.fromGenericType(c.type)}`);
    return `CREATE TABLE ${quotedTableName} (${columnNames.join(', ')})`;
  }

  protected toGenericType(columnType: string): string {
    return DbTypeToGenericType[columnType.toLowerCase()] || columnType;
  }

  protected fromGenericType(columnType: string): string {
    return columnType;
  }

  protected quoteIdentifier(identifier: string): string {
    return `"${identifier}"`;
  }

  protected cancelCombinator(fn) {
    return cancelCombinator(fn);
  }

  public setLogger(logger) {
    this.logger = logger;
  }

  protected reportQueryUsage(usage, queryOptions) {
    if (this.logger) {
      this.logger('SQL Query Usage', {
        ...usage,
        ...queryOptions
      });
    }
  }

  protected databasePoolError(error) {
    if (this.logger) {
      this.logger('Database Pool Error', {
        error: (error.stack || error).toString()
      });
    }
  }

  public async release() {
    // override, if it's needed
  }

  public capabilities(): DriverCapabilities {
    return {};
  }

  public nowTimestamp() {
    return Date.now();
  }

  public wrapQueryWithLimit(query: { query: string, limit: number}) {
    query.query = `SELECT * FROM (${query.query}) AS t LIMIT ${query.limit}`;
  }
}
