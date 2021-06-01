const sql = require('mssql');
const { BaseDriver } = require('@cubejs-backend/query-orchestrator');

const GenericTypeToMSSql = {
  string: 'nvarchar(max)',
  text: 'nvarchar(max)',
  timestamp: 'datetime2',
};

class MSSqlDriver extends BaseDriver {
  constructor(config) {
    super();
    this.config = {
      server: process.env.CUBEJS_DB_HOST,
      database: process.env.CUBEJS_DB_NAME,
      port: process.env.CUBEJS_DB_PORT && parseInt(process.env.CUBEJS_DB_PORT, 10),
      user: process.env.CUBEJS_DB_USER,
      password: process.env.CUBEJS_DB_PASS,
      domain: process.env.CUBEJS_DB_DOMAIN && process.env.CUBEJS_DB_DOMAIN.trim().length > 0 ?
        process.env.CUBEJS_DB_DOMAIN : undefined,
      requestTimeout: 10 * 60 * 1000, // 10 minutes
      options: {
        encrypt: process.env.CUBEJS_DB_SSL === 'true',
        useUTC: false
      },
      pool: {
        max: 8,
        min: 0,
        evictionRunIntervalMillis: 10000,
        softIdleTimeoutMillis: 30000,
        idleTimeoutMillis: 30000,
        testOnBorrow: true,
        acquireTimeoutMillis: 20000
      },
      ...config
    };
    this.connectionPool = new sql.ConnectionPool(this.config);
    this.initialConnectPromise = this.connectionPool.connect();
  }

  static driverEnvVariables() {
    return [
      'CUBEJS_DB_HOST', 'CUBEJS_DB_NAME', 'CUBEJS_DB_PORT', 'CUBEJS_DB_USER', 'CUBEJS_DB_PASS', 'CUBEJS_DB_DOMAIN'
    ];
  }

  testConnection() {
    return this.initialConnectPromise.then((pool) => pool.request().query('SELECT 1 as number'));
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

  createSchemaIfNotExists(schemaName) {
    return this.query(
      `SELECT schema_name FROM information_schema.schemata WHERE schema_name = ${this.param(0)}`,
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
      SELECT columns.column_name as ${this.quoteIdentifier('column_name')},
        columns.table_name as ${this.quoteIdentifier('table_name')},
        columns.table_schema as ${this.quoteIdentifier('table_schema')},
        columns.data_type as ${this.quoteIdentifier('data_type')}
      FROM information_schema.columns columns
      WHERE columns.table_schema NOT IN ('information_schema', 'sys')
    `;
  }

  async downloadQueryResults(query, values) {
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

  readOnly() {
    return !!this.config.readOnly;
  }
}

module.exports = MSSqlDriver;
