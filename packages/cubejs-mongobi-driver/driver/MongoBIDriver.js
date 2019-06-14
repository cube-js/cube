const mysql = require('mysql2');
const genericPool = require('generic-pool');
const { promisify } = require('util');
const BaseDriver = require('@cubejs-backend/query-orchestrator/driver/BaseDriver');

class MongoBIDriver extends BaseDriver {
  constructor(config) {
    super();

    let ssl;

    const sslOptions = [
      { name: 'ca', value: 'CUBEJS_DB_SSL_CA' },
      { name: 'cert', value: 'CUBEJS_DB_SSL_CERT' },
      { name: 'ciphers', value: 'CUBEJS_DB_SSL_CIPHERS' },
      { name: 'passphrase', value: 'CUBEJS_DB_SSL_PASSPHRASE' },
    ];

    if (
      process.env.CUBEJS_DB_SSL ||
      process.env.CUBEJS_DB_SSL_REJECT_UNAUTHORIZED ||
      sslOptions.find(o => !!process.env[o.value])
    ) {
      ssl = sslOptions.reduce(
        (agg, { name, value }) => ({
          ...agg,
          ...(process.env[value] ? { [name]: process.env[value] } : {}),
        }),
        {}
      );

      if (process.env.CUBEJS_DB_SSL_REJECT_UNAUTHORIZED) {
        ssl.rejectUnauthorized =
          process.env.CUBEJS_DB_SSL_REJECT_UNAUTHORIZED.toLowerCase() === 'true';
      }
    }

    this.config = {
      host: process.env.CUBEJS_DB_HOST,
      database: process.env.CUBEJS_DB_NAME,
      port: process.env.CUBEJS_DB_PORT,
      user: process.env.CUBEJS_DB_USER,
      password: process.env.CUBEJS_DB_PASS,
      ssl,
      authSwitchHandler: (data, cb) => {
        const buffer = Buffer.from((process.env.CUBEJS_DB_PASS || '').concat('\0'));
        cb(null, buffer);
      },
      ...config
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
      destroy: (connection) => promisify(connection.end.bind(connection))(),
      validate: async (connection) => {
        try {
          await connection.execute('SELECT 1');
        } catch (e) {
          return false;
        }
        return true;
      }
    }, {
      min: 0,
      max: 8,
      evictionRunIntervalMillis: 10000,
      softIdleTimeoutMillis: 30000,
      idleTimeoutMillis: 30000,
      testOnBorrow: true,
      acquireTimeoutMillis: 20000
    });
  }

  withConnection(fn) {
    const self = this;
    const connectionPromise = this.pool.acquire();

    let cancelled = false;
    const cancelObj = {};
    const promise = connectionPromise.then(conn => {
      cancelObj.cancel = async () => {
        cancelled = true;
        await self.withConnection(async processConnection => {
          const processRows = await processConnection.execute('SHOW PROCESSLIST');
          await Promise.all(processRows.filter(row => row.Time >= 599)
            .map(row => processConnection.execute(`KILL ${row.Id}`)));
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
    const self = this;
    return this.withConnection(db => db.execute(`SET time_zone = '${self.config.storeTimezone || '+00:00'}'`, [])
      .then(() => db.execute(query, values))
      .then(res => res));
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
}

module.exports = MongoBIDriver;
