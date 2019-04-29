const ClickHouse = require('@apla/clickhouse');
const genericPool = require('generic-pool');
const BaseDriver = require('@cubejs-backend/query-orchestrator/driver/BaseDriver');
const uuid = require('uuidv4');
const sqlstring = require('sqlstring');

class ClickHouseDriver extends BaseDriver {
  constructor(config) {
    super();
    this.config = {
      host: process.env.CUBEJS_DB_HOST,
      port: process.env.CUBEJS_DB_PORT,
      auth: process.env.CUBEJS_DB_USER || process.env.CUBEJS_DB_PASS ? process.env.CUBEJS_DB_USER + ":" + process.env.CUBEJS_DB_PASS : '',
      queryOptions: {
        database: process.env.CUBEJS_DB_NAME || config.database
      },
      ...config
    };
    this.pool = genericPool.createPool({
      create: () => new ClickHouse(Object.assign({}, this.config, { 
        queryOptions: { 
          join_use_nulls: 1,
          session_id: uuid()
        } 
      })),
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
    const queryId = uuid();

    let cancelled = false;
    const cancelObj = {};
    const promise = connectionPromise.then(connection => {
      cancelObj.cancel = async () => {
        cancelled = true;
        await self.withConnection(async connection => {
          await connection.querying(`KILL QUERY WHERE query_id = '${queryId}'`);
        });
      };
      return fn(connection, queryId)
        .then(res => {
          return this.pool.release(connection).then(() => {
            if (cancelled) {
              throw new Error('Query cancelled');
            }
            return res;
          });
        })
        .catch((err) => {
          return this.pool.release(connection).then(() => {
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

  testConnection() {
    return this.query("SELECT 1");
  }

  query(query, values) {
    const formattedQuery = sqlstring.format(query, values);
    
    return this.withConnection((connection, queryId) => {
      return connection.querying(formattedQuery, { dataObjects: true, queryOptions: { query_id: queryId, join_use_nulls: 1 } })
        .then(res => res.data);
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

  async createSchemaIfNotExists(schemaName) {
    await this.query(`CREATE DATABASE IF NOT EXISTS ${schemaName}`);
  }
  
  getTablesQuery(schemaName) {
    return this.query('SELECT name as table_name FROM system.tables WHERE database = ?', [schemaName])
  }
}

module.exports = ClickHouseDriver;
