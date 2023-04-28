/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `MSSqlDriver` and related types declaration.
 */

const {
  getEnv,
  assertDataSource,
} = require('@cubejs-backend/shared');
const sql = require('mssql');
const { BaseDriver } = require('@cubejs-backend/base-driver');
const QueryStream = require('./QueryStream');


const GenericTypeToMSSql = {
  boolean: 'bit',
  string: 'nvarchar(max)',
  text: 'nvarchar(max)',
  timestamp: 'datetime2',
  uuid: 'uniqueidentifier'
};

const MSSqlToGenericType = {
  bit: 'boolean',
  uniqueidentifier: 'uuid',
  datetime2: 'timestamp'
}

/**
 * MS SQL driver class.
 */
class MSSqlDriver extends BaseDriver {
  /**
   * Returns default concurrency value.
   */
  static getDefaultConcurrency() {
    return 2;
  }

  /**
   * Class constructor.
   */
  constructor(config = {}) {
    super({
      testConnectionTimeout: config.testConnectionTimeout,
    });

    const dataSource =
      config.dataSource ||
      assertDataSource('default');

    this.config = {
      readOnly: true,
      server: getEnv('dbHost', { dataSource }),
      database: getEnv('dbName', { dataSource }),
      port: getEnv('dbPort', { dataSource }),
      user: getEnv('dbUser', { dataSource }),
      password: getEnv('dbPass', { dataSource }),
      domain: getEnv('dbDomain', { dataSource }),
      requestTimeout: 10 * 60 * 1000, // 10 minutes
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
        evictionRunIntervalMillis: 10000,
        softIdleTimeoutMillis: 30000,
        idleTimeoutMillis: 30000,
        testOnBorrow: true,
        acquireTimeoutMillis: 20000
      },
      ...config
    };
    const { readOnly, ...poolConfig } = this.config;
    this.connectionPool = new sql.ConnectionPool(poolConfig);
    this.initialConnectPromise = this.connectionPool.connect();
  }

  static driverEnvVariables() {
    // TODO (buntarb): check how this method can/must be used with split
    // names by the data source.
    return [
      'CUBEJS_DB_HOST', 'CUBEJS_DB_NAME', 'CUBEJS_DB_PORT', 'CUBEJS_DB_USER', 'CUBEJS_DB_PASS', 'CUBEJS_DB_DOMAIN'
    ];
  }

  testConnection() {
    return this.initialConnectPromise.then((pool) => pool.request().query('SELECT 1 as number'));
  }

  /**
   * Executes query in streaming mode.
   *
   * @param {string} query 
   * @param {Array} values 
   * @param {{ highWaterMark: number? }} options
   * @return {Promise<StreamTableDataWithTypes>}
   */
  async stream(
    query,
    values,
    options,
  ) {
    const pool = await this.initialConnectPromise;
    const request = pool.request();

    request.stream = true;
    (values || []).forEach((v, i) => {
      request.input(`_${i + 1}`, v);
    });
    request.query(query);

    const stream = new QueryStream(request, options?.highWaterMark);
    const fields = await new Promise((resolve, reject) => {
      request.on('recordset', (columns) => {
        resolve(this.mapFields(columns));
      });
      request.on('error', (err) => {
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
  mapFields(fields) {
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
          type = 'int';
          break;
        // float
        case sql.Real:
        case sql.Money:
        case sql.SmallMoney:
        case sql.Numeric:
          type = 'float';
          break;
        // double
        case sql.Float:
        case sql.Decimal:
          type = 'double';
          break;
        // strings
        case sql.Char:
        case sql.NChar:
        case sql.Text:
        case sql.VarChar:
        case sql.NVarChar:
        case sql.Xml:
          type = 'text';
          break;
        // date and time
        case sql.Time:
          type = 'date';
          break;
        case sql.Date:
          type = 'date';
          break;
        case sql.DateTime:
        case sql.DateTime2:
        case sql.SmallDateTime:
        case sql.DateTimeOffset:
          type = 'date';
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
          type = 'string';
          break;
        // unknown
        default:
          type = 'string';
          break;
      }
      return { name: fields[field].name, type };
    });
  }

  query(query, values) {
    let cancelFn = null;
    const promise = this.initialConnectPromise.then((pool) => {
      const request = pool.request();
      (values || []).forEach((v, i) => request.input(`_${i + 1}`, v));

      // TODO time zone UTC set in driver ?

      cancelFn = () => request.cancel();
      return request.query(query).then(res => res.recordset);
    });
    promise.cancel = () => cancelFn && cancelFn();
    return promise;
  }

  param(paramIndex) {
    return `@_${paramIndex + 1}`;
  }

  async tableColumnTypes(table) {
    const [schema, name] = table.split('.');

    const columns = await this.query(
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

  getTablesQuery(schemaName) {
    return this.query(
      `SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = ${this.param(0)}`,
      [schemaName]
    );
  }

  createSchemaIfNotExists(schemaName) {
    return this.query(
      `SELECT schema_name FROM INFORMATION_SCHEMA.SCHEMATA WHERE schema_name = ${this.param(0)}`,
      [schemaName]
    ).then((schemas) => {
      if (schemas.length === 0) {
        return this.query(`CREATE SCHEMA ${schemaName}`);
      }
      return null;
    });
  }

  informationSchemaQuery() {
    // fix The multi-part identifier "columns.data_type" could not be bound
    return `
      SELECT column_name as ${this.quoteIdentifier('column_name')},
        table_name as ${this.quoteIdentifier('table_name')},
        table_schema as ${this.quoteIdentifier('table_schema')},
        data_type as ${this.quoteIdentifier('data_type')}
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE table_schema NOT IN ('information_schema', 'sys')
    `;
  }

  async downloadQueryResults(query, values, options) {
    if ((options || {}).streamImport) {
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

  fromGenericType(columnType) {
    return GenericTypeToMSSql[columnType] || super.fromGenericType(columnType);
  }

  toGenericType(columnType){
    return MSSqlToGenericType[columnType] || super.toGenericType(columnType);
  }

  readOnly() {
    return !!this.config.readOnly;
  }

  wrapQueryWithLimit(query) {
    query.query = `SELECT TOP ${query.limit} * FROM (${query.query}) AS t`;
  }
}

module.exports = MSSqlDriver;
