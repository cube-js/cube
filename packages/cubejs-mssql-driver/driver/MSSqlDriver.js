const sql = require('mssql');
const BaseDriver = require('@cubejs-backend/query-orchestrator/driver/BaseDriver');

class MSSqlDriver extends BaseDriver {
  constructor(config) {
    super();
    this.config = {
      server: process.env.CUBEJS_DB_HOST,
      database: process.env.CUBEJS_DB_NAME,
      port: process.env.CUBEJS_DB_PORT && parseInt(process.env.CUBEJS_DB_PORT, 10),
      user: process.env.CUBEJS_DB_USER,
      password: process.env.CUBEJS_DB_PASS,
      domain: process.env.CUBEJS_DB_DOMAIN,
      options: {
        encrypt: !!process.env.CUBEJS_DB_SSL || false
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
    this.config = config;
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
    return this.initialConnectPromise.then((pool) => {
      const request = pool.request();
      (values || []).forEach((v, i) => request.input(`_${i + 1}`, v));

      // TODO time zone UTC set in driver ?

      return request.query(query).then(res => res.recordset);
    });
  }

  param(paramIndex) {
    return `@_${paramIndex + 1}`;
  }
}

module.exports = MSSqlDriver;
