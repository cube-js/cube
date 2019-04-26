const ClickHouse = require('clickhouse');
const genericPool = require('generic-pool');
const BaseDriver = require('@cubejs-backend/query-orchestrator/driver/BaseDriver');

class ClickHouseDriver extends BaseDriver {
  constructor(config) {
    super();
    this.config = {
      host: process.env.CUBEJS_DB_HOST,
      port: process.env.CUBEJS_DB_PORT,
      auth: process.env.CUBEJS_DB_USER + ":" + process.env.CUBEJS_DB_PASS,
      queryOptions: {
        database: process.env.CUBEJS_DB_NAME
      },
      ...config
    };
    this.pool = genericPool.createPool({
      create: () => new ClickHouse(this.config),
      destroy: (connection) => {
        return Promise.resolve();
      },
      validate: async (connection) => {
        try {
          await connection.querying('SELECT 1');
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
        await self.withConnection(async conn => {
          const processRows = await conn.querying('SHOW PROCESSLIST');
          await Promise.all(processRows.filter(row => row.elapsed >= 599).map(row => {
            return conn.execute(`KILL QUERY WHERE query_id = '${row.query_id}'`);
          }));
        });
      };
      return fn(conn)
        .then(res => {
          return this.pool.release(conn).then(() => {
            if (cancelled) {
              throw new Error('Query cancelled');
            }
            return res;
          });
        })
        .catch((err) => {
          return this.pool.release(conn).then(() => {
            if (cancelled) {
              throw new Error('Query cancelled');
            }
            throw err;
          });
        })
    });
    promise.cancel = () => cancelObj.cancel();
    return promise;
  }

  async testConnection() {
    const conn = await this.pool._factory.create();
    try {
      return await conn.querying('SELECT 1');
    } finally {
      await this.pool._factory.destroy(conn);
    }
  }

  query(query, values) {
    // TODO: handle values
    const self = this;
    return this.withConnection(connection => {
      return connection.querying(query)
        .then(res => res);
    });
  }

  async release() {
    await this.pool.drain();
    await this.pool.clear();
  }

  informationSchemaQuery() {
    return `
      SELECT name as column_name,
             table as table_name,
             database as table_schema,
             type as data_type
        FROM system.columns
       WHERE database = '${this.config.database}'
    `;
  }
}

module.exports = ClickHouseDriver;