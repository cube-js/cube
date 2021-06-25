const mysql = require('mysql');
const genericPool = require('generic-pool');
const { promisify } = require('util');
const { BaseDriver } = require('@cubejs-backend/query-orchestrator');

const GenericTypeToMySql = {
  string: 'varchar(255) CHARACTER SET utf8mb4',
  text: 'varchar(255) CHARACTER SET utf8mb4',
  decimal: 'decimal(38,10)',
};

/**
 * MySQL Native types -> SQL type
 * @link https://github.com/mysqljs/mysql/blob/master/lib/protocol/constants/types.js#L9
 */
const MySqlNativeToMySqlType = {
  [mysql.Types.DECIMAL]: 'decimal',
  [mysql.Types.NEWDECIMAL]: 'decimal',
  [mysql.Types.TINY]: 'tinyint',
  [mysql.Types.SHORT]: 'smallint',
  [mysql.Types.LONG]: 'int',
  [mysql.Types.INT24]: 'mediumint',
  [mysql.Types.LONGLONG]: 'bigint',
  [mysql.Types.NEWDATE]: 'datetime',
  [mysql.Types.TIMESTAMP2]: 'timestamp',
  [mysql.Types.DATETIME2]: 'datetime',
  [mysql.Types.TIME2]: 'time',
  [mysql.Types.TINY_BLOB]: 'tinytext',
  [mysql.Types.MEDIUM_BLOB]: 'mediumtext',
  [mysql.Types.LONG_BLOB]: 'longtext',
  [mysql.Types.BLOB]: 'text',
  [mysql.Types.VAR_STRING]: 'varchar',
  [mysql.Types.STRING]: 'binary',
};

const MySqlToGenericType = {
  mediumtext: 'text',
  longtext: 'text',
  mediumint: 'int',
  smallint: 'int',
  bigint: 'int',
  tinyint: 'int',
  'mediumint unsigned': 'int',
  'smallint unsigned': 'int',
  'bigint unsigned': 'int',
  'tinyint unsigned': 'int',
};

class MySqlDriver extends BaseDriver {
  constructor(config) {
    super();
    const { pool, ...restConfig } = config || {};

    this.config = {
      host: process.env.CUBEJS_DB_HOST,
      database: process.env.CUBEJS_DB_NAME,
      port: process.env.CUBEJS_DB_PORT,
      user: process.env.CUBEJS_DB_USER,
      password: process.env.CUBEJS_DB_PASS,
      socketPath: process.env.CUBEJS_DB_SOCKET_PATH,
      timezone: 'Z',
      ssl: this.getSslOptions(),
      dateStrings: true,
      readOnly: true,
      ...restConfig,
    };

    this.pool = genericPool.createPool({
      create: async () => {
        const conn = mysql.createConnection(this.config);
        const connect = promisify(conn.connect.bind(conn));

        if (conn.on) {
          conn.on('error', () => {
            conn.destroy();
          });
        }
        conn.execute = promisify(conn.query.bind(conn));

        await connect();

        return conn;
      },
      validate: async (connection) => {
        try {
          await connection.execute('SELECT 1');
        } catch (e) {
          this.databasePoolError(e);
          return false;
        }
        return true;
      },
      destroy: (connection) => promisify(connection.end.bind(connection))(),
    }, {
      min: 0,
      max: process.env.CUBEJS_DB_MAX_POOL && parseInt(process.env.CUBEJS_DB_MAX_POOL, 10) || 8,
      evictionRunIntervalMillis: 10000,
      softIdleTimeoutMillis: 30000,
      idleTimeoutMillis: 30000,
      testOnBorrow: true,
      acquireTimeoutMillis: 20000,
      ...pool
    });
  }

  readOnly() {
    return !!this.config.readOnly;
  }

  withConnection(fn) {
    const self = this;
    const connectionPromise = this.pool.acquire();

    let cancelled = false;
    const cancelObj = {};
    const promise = connectionPromise.then(async conn => {
      const [{ connectionId }] = await conn.execute('select connection_id() as connectionId');
      cancelObj.cancel = async () => {
        cancelled = true;
        await self.withConnection(async processConnection => {
          await processConnection.execute(`KILL ${connectionId}`);
        });
      };
      return fn(conn)
        .then(res => this.pool.release(conn).then(() => {
          if (cancelled) {
            throw new Error('Query cancelled');
          }
          return res;
        }))
        .catch((err) => this.pool.release(conn).then(() => {
          if (cancelled) {
            throw new Error('Query cancelled');
          }
          throw err;
        }));
    });
    promise.cancel = () => cancelObj.cancel();
    return promise;
  }

  async testConnection() {
    // eslint-disable-next-line no-underscore-dangle
    const conn = await this.pool._factory.create();
    try {
      return await conn.execute('SELECT 1');
    } finally {
      // eslint-disable-next-line no-underscore-dangle
      await this.pool._factory.destroy(conn);
    }
  }

  query(query, values) {
    return this.withConnection(db => this.setTimeZone(db)
      .then(() => db.execute(query, values))
      .then(res => res));
  }

  setTimeZone(db) {
    return db.execute(`SET time_zone = '${this.config.storeTimezone || '+00:00'}'`, []);
  }

  async release() {
    await this.pool.drain();
    await this.pool.clear();
  }

  informationSchemaQuery() {
    return `${super.informationSchemaQuery()} AND columns.table_schema = '${this.config.database}'`;
  }

  quoteIdentifier(identifier) {
    return `\`${identifier}\``;
  }

  fromGenericType(columnType) {
    return GenericTypeToMySql[columnType] || super.fromGenericType(columnType);
  }

  loadPreAggregationIntoTable(preAggregationTableName, loadSql, params, tx) {
    if (this.config.loadPreAggregationWithoutMetaLock) {
      return this.cancelCombinator(async saveCancelFn => {
        await saveCancelFn(this.query(`${loadSql} LIMIT 0`, params));
        await saveCancelFn(this.query(loadSql.replace(/^CREATE TABLE (\S+) AS/i, 'INSERT INTO $1'), params));
      });
    }
    return super.loadPreAggregationIntoTable(preAggregationTableName, loadSql, params, tx);
  }

  async stream(query, values, { highWaterMark }) {
    // eslint-disable-next-line no-underscore-dangle
    const conn = await this.pool._factory.create();

    try {
      await this.setTimeZone(conn);

      return await new Promise((resolve, reject) => {
        const response = conn.query(query, values, (err, result, fields) => {
          if (err) {
            reject(err);
          } else {
            resolve({
              // eslint-disable-next-line no-underscore-dangle
              rowStream: response.stream({ highWaterMark }),
              types: this.mapFieldsToGenericTypes(fields),
              release: async () => {
                // eslint-disable-next-line no-underscore-dangle
                await this.pool._factory.destroy(conn);
              }
            });
          }
        });
      });
    } catch (e) {
      // eslint-disable-next-line no-underscore-dangle
      await this.pool._factory.destroy(conn);

      throw e;
    }
  }

  mapFieldsToGenericTypes(fields) {
    return fields.map((field) => {
      let type = mysql.Types[field.type];

      if (field.type in MySqlNativeToMySqlType) {
        type = MySqlNativeToMySqlType[field.type];
      }

      return {
        name: field.name,
        type: this.toGenericType(type)
      };
    });
  }

  async downloadQueryResults(query, values, options) {
    if ((options || {}).streamImport) {
      return this.stream(query, values, options);
    }

    return this.withConnection(async (conn) => {
      await this.setTimeZone(conn);

      return new Promise((resolve, reject) => {
        conn.query(query, values, (err, rows, fields) => {
          if (err) {
            reject(err);
          } else {
            resolve({
              rows,
              types: this.mapFieldsToGenericTypes(fields),
            });
          }
        });
      });
    });
  }

  toColumnValue(value, genericType) {
    if (genericType === 'timestamp' && typeof value === 'string') {
      return value && value.replace('Z', '');
    }
    if (genericType === 'boolean' && typeof value === 'string') {
      if (value.toLowerCase() === 'true') {
        return true;
      }
      if (value.toLowerCase() === 'false') {
        return false;
      }
    }
    return super.toColumnValue(value, genericType);
  }

  async uploadTableWithIndexes(table, columns, tableData, indexesSql) {
    if (!tableData.rows) {
      throw new Error(`${this.constructor} driver supports only rows upload`);
    }
    await this.createTable(table, columns);
    try {
      const batchSize = 1000; // TODO make dynamic?
      for (let j = 0; j < Math.ceil(tableData.rows.length / batchSize); j++) {
        const currentBatchSize = Math.min(tableData.rows.length - j * batchSize, batchSize);
        const indexArray = Array.from({ length: currentBatchSize }, (v, i) => i);
        const valueParamPlaceholders =
          indexArray.map(i => `(${columns.map((c, paramIndex) => this.param(paramIndex + i * columns.length)).join(', ')})`).join(', ');
        const params = indexArray.map(i => columns
          .map(c => this.toColumnValue(tableData.rows[i + j * batchSize][c.name], c.type)))
          .reduce((a, b) => a.concat(b), []);

        await this.query(
          `INSERT INTO ${table}
        (${columns.map(c => this.quoteIdentifier(c.name)).join(', ')})
        VALUES ${valueParamPlaceholders}`,
          params
        );
      }

      for (let i = 0; i < indexesSql.length; i++) {
        const [query, p] = indexesSql[i].sql;
        await this.query(query, p);
      }
    } catch (e) {
      await this.dropTable(table);
      throw e;
    }
  }

  toGenericType(columnType) {
    return MySqlToGenericType[columnType.toLowerCase()] ||
      MySqlToGenericType[columnType.toLowerCase().split('(')[0]] ||
      super.toGenericType(columnType);
  }
}

module.exports = MySqlDriver;
