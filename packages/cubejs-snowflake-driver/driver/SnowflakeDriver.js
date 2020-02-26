const snowflake = require('snowflake-sdk');
const BaseDriver = require('@cubejs-backend/query-orchestrator/driver/BaseDriver');

class SnowflakeDriver extends BaseDriver {
  constructor(config) {
    super();
    this.config = {
      account: process.env.CUBEJS_DB_SNOWFLAKE_ACCOUNT,
      region: process.env.CUBEJS_DB_SNOWFLAKE_REGION,
      warehouse: process.env.CUBEJS_DB_SNOWFLAKE_WAREHOUSE,
      role: process.env.CUBEJS_DB_SNOWFLAKE_ROLE,
      database: process.env.CUBEJS_DB_NAME,
      username: process.env.CUBEJS_DB_USER,
      password: process.env.CUBEJS_DB_PASS,
      ...config
    };
    const connection = snowflake.createConnection(this.config);
    this.initialConnectPromise = new Promise(
      (resolve, reject) => connection.connect((err, conn) => (err ? reject(err) : resolve(conn)))
    );
  }

  static driverEnvVariables() {
    return [
      'CUBEJS_DB_NAME',
      'CUBEJS_DB_USER',
      'CUBEJS_DB_PASS',
      'CUBEJS_DB_SNOWFLAKE_ACCOUNT',
      'CUBEJS_DB_SNOWFLAKE_REGION',
      'CUBEJS_DB_SNOWFLAKE_WAREHOUSE',
      'CUBEJS_DB_SNOWFLAKE_ROLE'
    ];
  }

  testConnection() {
    return this.query('SELECT 1 as number');
  }

  query(query, values) {
    return this.initialConnectPromise.then((connection) => this.execute(connection, "ALTER SESSION SET TIMEZONE = 'UTC'")
      .then(() => this.execute(connection, "ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 600"))
      .then(() => this.execute(connection, query, values)));
  }

  execute(connection, query, values) {
    return new Promise((resolve, reject) => connection.execute({
      sqlText: query,
      binds: values,
      fetchAsString: ['Number', 'Date'],
      complete: (err, stmt, rows) => {
        if (err) {
          reject(err);
        } else {
          resolve(rows);
        }
      }
    }));
  }

  informationSchemaQuery() {
    return `
        SELECT columns.column_name as "column_name",
               columns.table_name as "table_name",
               columns.table_schema as "table_schema",
               columns.data_type as "data_type"
        FROM information_schema.columns
        WHERE columns.table_schema NOT IN ('INFORMATION_SCHEMA')
     `;
  }
}

module.exports = SnowflakeDriver;
