import { reduce } from 'ramda';
import fs from 'fs';
import { getEnv, isFilePath, isSslKey, isSslCert } from '@cubejs-backend/shared';

import { cancelCombinator } from './utils';

const sortByKeys = (unordered) => {
  const ordered = {};

  Object.keys(unordered).sort().forEach((key) => {
    ordered[key] = unordered[key];
  });

  return ordered;
};

const DbTypeToGenericType = {
  'timestamp without time zone': 'timestamp',
  integer: 'int',
  int8: 'int',
  int4: 'int',
  int2: 'int',
  'character varying': 'text',
  varchar: 'text',
  nvarchar: 'text',
  text: 'text',
  string: 'text',
  boolean: 'boolean',
  bigint: 'bigint',
  time: 'string',
  datetime: 'timestamp',
  date: 'date',
  'double precision': 'decimal'
};

const DB_INT_MAX = 2147483647;
const DB_INT_MIN = -2147483648;

// Order of keys is important here: from more specific to less specific
const DbTypeValueMatcher = {
  timestamp: (v) => v instanceof Date || v.toString().match(/^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d/),
  date: (v) => v instanceof Date || v.toString().match(/^\d\d\d\d-\d\d-\d\d$/),
  bigint: (v) => Number.isInteger(v) && (v > DB_INT_MAX || v < DB_INT_MIN),
  int: (v) => {
    if (Number.isInteger(v)) {
      return true;
    }

    return v.toString().match(/^[-]?\d+$/);
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

export class BaseDriver {
  informationSchemaQuery() {
    return `
      SELECT columns.column_name as ${this.quoteIdentifier('column_name')},
             columns.table_name as ${this.quoteIdentifier('table_name')},
             columns.table_schema as ${this.quoteIdentifier('table_schema')},
             columns.data_type as ${this.quoteIdentifier('data_type')}
      FROM information_schema.columns
      WHERE columns.table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
   `;
  }

  getSslOptions() {
    let ssl;

    const sslOptions = [
      { name: 'ca', canBeFile: true, envKey: 'CUBEJS_DB_SSL_CA', validate: isSslCert },
      { name: 'cert', canBeFile: true, envKey: 'CUBEJS_DB_SSL_CERT', validate: isSslCert },
      { name: 'key', canBeFile: true, envKey: 'CUBEJS_DB_SSL_KEY', validate: isSslKey },
      { name: 'ciphers', envKey: 'CUBEJS_DB_SSL_CIPHERS' },
      { name: 'passphrase', envKey: 'CUBEJS_DB_SSL_PASSPHRASE' },
      { name: 'servername', envKey: 'CUBEJS_DB_SSL_SERVERNAME' },
    ];

    if (
      getEnv('dbSsl') ||
      getEnv('dbSslRejectUnauthorized') ||
      sslOptions.find(o => !!process.env[o.value])
    ) {
      ssl = sslOptions.reduce(
        (agg, { name, envKey, canBeFile, validate }) => {
          if (process.env[envKey]) {
            const value = process.env[envKey];

            if (canBeFile) {
              if (isFilePath(value)) {
                if (!fs.existsSync(value)) {
                  throw new Error(
                    `Unable to find ${name} from path: "${value}"`,
                  );
                }

                return {
                  ...agg,
                  ...{ [name]: fs.readFileSync(value) }
                };
              } else if (!validate(value)) {
                throw new Error(
                  `${envKey} is not a valid ssl key. If it's a path, please specify it correctly`,
                );
              }
            }

            return {
              ...agg,
              ...{ [name]: value }
            };
          }

          return agg;
        },
        {}
      );

      ssl.rejectUnauthorized = getEnv('dbSslRejectUnauthorized');
    }

    return ssl;
  }

  /**
   * @abstract
   */
  async testConnection() {
    throw new Error('Not implemented');
  }

  /**
   * @abstract
   * @param {string} query
   * @param {Array<unknown>} values
   * @return {Promise<Array<unknown>>}
   */
  async query(query, values) {
    throw new Error('Not implemented');
  }

  async downloadQueryResults(query, values) {
    const rows = await this.query(query, values);
    if (rows.length === 0) {
      throw new Error('Unable to detect column types for pre-aggregation on empty values in readOnly mode');
    }

    const fields = Object.keys(rows[0]);

    const types = fields.map(field => ({
      name: field,
      type: Object.keys(DbTypeValueMatcher).find(
        type => !rows.filter(row => !!row[field]).find(row => !DbTypeValueMatcher[type](row[field])) &&
          rows.find(row => !!row[field])
      ) || 'text'
    }));

    return {
      rows,
      types,
    };
  }

  readOnly() {
    return false;
  }

  tablesSchema() {
    const query = this.informationSchemaQuery();

    const reduceCb = (result, i) => {
      let schema = (result[i.table_schema] || {});
      const tables = (schema[i.table_name] || []);

      tables.push({ name: i.column_name, type: i.data_type, attributes: i.key_type ? ['primaryKey'] : [] });

      tables.sort();
      schema[i.table_name] = tables;
      schema = sortByKeys(schema);
      result[i.table_schema] = schema;

      return sortByKeys(result);
    };

    return this.query(query).then(data => reduce(reduceCb, {}, data));
  }

  /**
   * @param {string} schemaName
   * @return {Promise<Array<unknown>>}
   */
  async createSchemaIfNotExists(schemaName) {
    return this.query(
      `SELECT schema_name FROM information_schema.schemata WHERE schema_name = ${this.param(0)}`,
      [schemaName]
    ).then((schemas) => {
      if (schemas.length === 0) {
        return this.query(`CREATE SCHEMA IF NOT EXISTS ${schemaName}`);
      }
      return null;
    });
  }

  getTablesQuery(schemaName) {
    return this.query(
      `SELECT table_name FROM information_schema.tables WHERE table_schema = ${this.param(0)}`,
      [schemaName]
    );
  }

  loadPreAggregationIntoTable(preAggregationTableName, loadSql, params, options) {
    return this.query(loadSql, params, options);
  }

  dropTable(tableName, options) {
    return this.query(`DROP TABLE ${tableName}`, [], options);
  }

  param(/* paramIndex */) {
    return '?';
  }

  testConnectionTimeout() {
    return 10000;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  async downloadTable(table, options) {
    return { rows: await this.query(`SELECT * FROM ${table}`) };
  }

  async uploadTable(table, columns, tableData) {
    return this.uploadTableWithIndexes(table, columns, tableData, []);
  }

  async uploadTableWithIndexes(table, columns, tableData, indexesSql) {
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
        const [query, params] = indexesSql[i].sql;
        await this.query(query, params);
      }
    } catch (e) {
      await this.dropTable(table);
      throw e;
    }
  }

  // eslint-disable-next-line no-unused-vars
  toColumnValue(value, genericType) {
    return value;
  }

  async tableColumnTypes(table) {
    const [schema, name] = table.split('.');
    const columns = await this.query(
      `SELECT columns.column_name,
             columns.table_name,
             columns.table_schema,
             columns.data_type
      FROM information_schema.columns
      WHERE table_name = ${this.param(0)} AND table_schema = ${this.param(1)}`,
      [name, schema]
    );
    return columns.map(c => ({ name: c.column_name, type: this.toGenericType(c.data_type) }));
  }

  createTable(quotedTableName, columns) {
    const createTableSql = this.createTableSql(quotedTableName, columns);
    return this.query(createTableSql, []).catch(e => {
      e.message = `Error during create table: ${createTableSql}: ${e.message}`;
      throw e;
    });
  }

  createTableSql(quotedTableName, columns) {
    columns = columns.map(c => `${this.quoteIdentifier(c.name)} ${this.fromGenericType(c.type)}`);
    return `CREATE TABLE ${quotedTableName} (${columns.join(', ')})`;
  }

  toGenericType(columnType) {
    return DbTypeToGenericType[columnType.toLowerCase()] || columnType;
  }

  fromGenericType(columnType) {
    return columnType;
  }

  quoteIdentifier(identifier) {
    return `"${identifier}"`;
  }

  cancelCombinator(fn) {
    return cancelCombinator(fn);
  }

  setLogger(logger) {
    this.logger = logger;
  }

  reportQueryUsage(usage, queryOptions) {
    if (this.logger) {
      this.logger('SQL Query Usage', {
        ...usage,
        ...queryOptions
      });
    }
  }

  databasePoolError(error) {
    if (this.logger) {
      this.logger('Database Pool Error', {
        error: (error.stack || error).toString()
      });
    }
  }

  capabilities() {
    return {};
  }
}
