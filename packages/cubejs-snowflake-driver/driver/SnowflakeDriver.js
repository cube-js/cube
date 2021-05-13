/* eslint-disable no-restricted-syntax */
const snowflake = require('snowflake-sdk');
const { formatToTimeZone } = require('date-fns-timezone');
const { BaseDriver } = require('@cubejs-backend/query-orchestrator');

// It's not possible to declare own map converters by passing config to snowflake-sdk
const hydrators = [
  {
    types: ['fixed', 'real'],
    toValue: (column) => {
      if (column.isNullable()) {
        return (value) => {
          // We use numbers as strings by fetchAsString
          if (value === 'NULL') {
            return null;
          }

          return value;
        };
      }

      // Nothing to fix, let's skip this field
      return null;
    },
  },
  {
    // The TIMESTAMP_* variation associated with TIMESTAMP, default to TIMESTAMP_NTZ
    types: [
      'date',
      // TIMESTAMP_LTZ internally stores UTC time with a specified precision.
      'timestamp_ltz',
      // TIMESTAMP_NTZ internally stores “wallclock” time with a specified precision.
      // All operations are performed without taking any time zone into account.
      'timestamp_ntz',
      // TIMESTAMP_TZ internally stores UTC time together with an associated time zone offset.
      // When a time zone is not provided, the session time zone offset is used.
      'timestamp_tz'
    ],
    toValue: () => (value) => {
      if (!value) {
        return null;
      }

      return formatToTimeZone(
        value,
        'YYYY-MM-DDTHH:mm:ss.SSS',
        {
          timeZone: 'UTC'
        }
      );
    },
  }
];

const SnowflakeToGenericType = {
  number: 'decimal',
  timestamp_ntz: 'timestamp'
};

/**
 * Attention:
 * Snowflake is using UPPER_CASE for table, schema and column names
 * Similar to data in response, column_name will be COLUMN_NAME
 */
class SnowflakeDriver extends BaseDriver {
  constructor(config) {
    super();
    this.config = {
      account: process.env.CUBEJS_DB_SNOWFLAKE_ACCOUNT,
      region: process.env.CUBEJS_DB_SNOWFLAKE_REGION,
      warehouse: process.env.CUBEJS_DB_SNOWFLAKE_WAREHOUSE,
      role: process.env.CUBEJS_DB_SNOWFLAKE_ROLE,
      clientSessionKeepAlive: process.env.CUBEJS_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE === 'true',
      database: process.env.CUBEJS_DB_NAME,
      username: process.env.CUBEJS_DB_USER,
      password: process.env.CUBEJS_DB_PASS,
      authenticator: process.env.CUBEJS_DB_SNOWFLAKE_AUTHENTICATOR,
      privateKeyPath: process.env.CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PATH,
      privateKeyPass: process.env.CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PASS,
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
      'CUBEJS_DB_SNOWFLAKE_ROLE',
      'CUBEJS_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE',
      'CUBEJS_DB_SNOWFLAKE_AUTHENTICATOR',
      'CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PATH',
      'CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PASS'
    ];
  }

  testConnection() {
    return this.query('SELECT 1 as number');
  }

  query(query, values) {
    return this.initialConnectPromise.then((connection) => this.execute(connection, 'ALTER SESSION SET TIMEZONE = \'UTC\'', [], false)
      .then(() => this.execute(connection, 'ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 600', [], false))
      .then(() => this.execute(connection, query, values)));
  }

  async execute(connection, query, values, rehydrate = true) {
    return new Promise((resolve, reject) => connection.execute({
      sqlText: query,
      binds: values,
      fetchAsString: ['Number'],
      complete: (err, stmt, rows) => {
        if (err) {
          reject(err);
          return;
        }

        if (rehydrate && rows.length) {
          const hydrationMap = {};
          const columns = stmt.getColumns();

          for (const column of columns) {
            for (const hydrator of hydrators) {
              if (hydrator.types.includes(column.getType())) {
                const fnOrNull = hydrator.toValue(column);
                if (fnOrNull) {
                  hydrationMap[column.getName()] = fnOrNull;
                }
              }
            }
          }

          if (Object.keys(hydrationMap).length) {
            for (const row of rows) {
              for (const [field, toValue] of Object.entries(hydrationMap)) {
                if (row.hasOwnProperty(field)) {
                  row[field] = toValue(row[field]);
                }
              }
            }
          }
        }

        resolve(rows);
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

  async release() {
    return this.initialConnectPromise.then((connection) => new Promise(
      (resolve, reject) => connection.destroy((err, conn) => (err ? reject(err) : resolve(conn)))
    ));
  }

  toGenericType(columnType) {
    return SnowflakeToGenericType[columnType.toLowerCase()] || super.toGenericType(columnType);
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
      [name.toUpperCase(), schema.toUpperCase()]
    );

    return columns.map(c => ({ name: c.COLUMN_NAME, type: this.toGenericType(c.DATA_TYPE) }));
  }

  async getTablesQuery(schemaName) {
    const tables = await super.getTablesQuery(schemaName.toUpperCase());
    return tables.map(t => ({ table_name: t.TABLE_NAME && t.TABLE_NAME.toLowerCase() }));
  }
}

module.exports = SnowflakeDriver;
