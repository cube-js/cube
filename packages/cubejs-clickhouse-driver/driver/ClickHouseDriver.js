/* eslint-disable no-restricted-syntax */
const ClickHouse = require('@apla/clickhouse');
const { getEnv } = require('@cubejs-backend/shared');
const { BaseDriver } = require('@cubejs-backend/query-orchestrator');
const genericPool = require('generic-pool');
const { uuid } = require('uuidv4');
const sqlstring = require('sqlstring');

const ClickhouseTypeToGeneric = {
  string: 'text',
  datetime: 'timestamp',
  date: 'date',
  int64: 'bigint',
  uint64: 'bigint',
};

class ClickHouseDriver extends BaseDriver {
  constructor(config) {
    super();
    this.config = {
      host: process.env.CUBEJS_DB_HOST,
      port: process.env.CUBEJS_DB_PORT,
      auth: process.env.CUBEJS_DB_USER || process.env.CUBEJS_DB_PASS ? `${process.env.CUBEJS_DB_USER}:${process.env.CUBEJS_DB_PASS}` : '',
      protocol: getEnv('dbSsl') ? 'https:' : 'http:',
      queryOptions: {
        database: process.env.CUBEJS_DB_NAME || config && config.database || 'default'
      },
      ...config
    };
    this.readOnlyMode = process.env.CUBEJS_DB_CLICKHOUSE_READONLY === 'true';
    this.pool = genericPool.createPool({
      create: async () => new ClickHouse({
        ...this.config,
        queryOptions: {
          //
          //
          // If ClickHouse user's permissions are restricted with "readonly = 1",
          // change settings queries are not allowed. Thus, "join_use_nulls" setting
          // can not be changed
          //
          //
          ...(this.readOnlyMode ? {} : { join_use_nulls: 1 }),
          session_id: uuid(),
          ...this.config.queryOptions,
        }
      }),
      destroy: () => Promise.resolve()
    }, {
      min: 0,
      max: 8,
      evictionRunIntervalMillis: 10000,
      softIdleTimeoutMillis: 30000,
      idleTimeoutMillis: 30000,
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
        await self.withConnection(async conn => {
          await conn.querying(`KILL QUERY WHERE query_id = '${queryId}'`);
        });
      };
      return fn(connection, queryId)
        .then(res => this.pool.release(connection).then(() => {
          if (cancelled) {
            throw new Error('Query cancelled');
          }
          return res;
        }))
        .catch((err) => this.pool.release(connection).then(() => {
          if (cancelled) {
            throw new Error('Query cancelled');
          }
          throw err;
        }));
    });
    promise.cancel = () => cancelObj.cancel();
    return promise;
  }

  testConnection() {
    return this.query('SELECT 1');
  }

  readOnly() {
    return !!this.config.readOnly || this.readOnlyMode;
  }

  query(query, values) {
    return this.queryResponse(query, values).then(res => this.normaliseResponse(res));
  }

  queryResponse(query, values) {
    const formattedQuery = sqlstring.format(query, values);

    return this.withConnection((connection, queryId) => connection.querying(formattedQuery, {
      dataObjects: true,
      queryOptions: {
        query_id: queryId,
        //
        //
        // If ClickHouse user's permissions are restricted with "readonly = 1",
        // change settings queries are not allowed. Thus, "join_use_nulls" setting
        // can not be changed
        //
        //
        ...(this.readOnlyMode ? {} : { join_use_nulls: 1 }),
      }
    }));
  }

  normaliseResponse(res) {
    //
    //
    //  ClickHouse returns DateTime as strings in format "YYYY-DD-MM HH:MM:SS"
    //  cube.js expects them in format "YYYY-DD-MMTHH:MM:SS.000", so translate them based on the metadata returned
    //
    //  ClickHouse returns some number types as js numbers, others as js string, normalise them all to strings
    //
    //
    if (res.data) {
      res.data.forEach(row => {
        Object.keys(row).forEach(field => {
          const value = row[field];
          if (value !== null) {
            const meta = res.meta.find(m => m.name === field);
            if (meta.type.includes('DateTime')) {
              row[field] = `${value.substring(0, 10)}T${value.substring(11, 22)}.000`;
            } else if (meta.type.includes('Date')) {
              row[field] = `${value}T00:00:00.000`;
            } else if (meta.type.includes('Int') || meta.type.includes('Float')) {
              // convert all numbers into strings
              row[field] = `${value}`;
            }
          }
        });
      });
    }
    return res.data;
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
       WHERE database = '${this.config.queryOptions.database}'
    `;
  }

  async downloadQueryResults(query, values, options) {
    const response = await this.queryResponse(query, values);

    return {
      rows: this.normaliseResponse(response),
      types: response.meta.map((field) => ({
        name: field.name,
        type: this.toGenericType(field.type),
      })),
    };
  }

  toGenericType(columnType) {
    if (columnType.toLowerCase() in ClickhouseTypeToGeneric) {
      return ClickhouseTypeToGeneric[columnType.toLowerCase()];
    }

    /**
     * Example of types:
     *
     * Int64
     * Nullable(Int64) / Nullable(String)
     * Nullable(DateTime('UTC'))
     */
    if (columnType.includes('(')) {
      const types = columnType.toLowerCase().match(/([a-z']+)/g);
      for (const type of types) {
        if (type in ClickhouseTypeToGeneric) {
          return ClickhouseTypeToGeneric[type];
        }
      }
    }

    return super.toGenericType(columnType);
  }

  async createSchemaIfNotExists(schemaName) {
    await this.query(`CREATE DATABASE IF NOT EXISTS ${schemaName}`);
  }

  getTablesQuery(schemaName) {
    return this.query('SELECT name as table_name FROM system.tables WHERE database = ?', [schemaName]);
  }
}

module.exports = ClickHouseDriver;
