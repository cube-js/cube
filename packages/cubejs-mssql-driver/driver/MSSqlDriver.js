const sql = require('mssql');
const { BaseDriver } = require('@cubejs-backend/query-orchestrator');

const GenericTypeToMSSql = {
  string: 'nvarchar(max)',
  text: 'nvarchar(max)',
  timestamp: 'datetime2',
  uuid: 'uniqueidentifier'
};

const MSSqlToGenericType = {
  uniqueidentifier: 'uuid',
  datetime2: 'timestamp'
}

class MSSqlDriver extends BaseDriver {
  /**
   * Returns default concurrency value.
   */
  static getDefaultConcurrency() {
    return 2;
  }

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
        max: config.maxPoolSize || 8,
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

  toGenericType(columnType){
    return MSSqlToGenericType[columnType] || super.toGenericType(columnType);
  }

  readOnly() {
    return !!this.config.readOnly;
  }
}

module.exports = MSSqlDriver;
