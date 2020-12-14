const pg = require('pg');
const { types } = require('pg');
const moment = require('moment');
const { BaseDriver } = require('@cubejs-backend/query-orchestrator');

const { Pool } = pg;

const GenericTypeToPostgres = {
  string: 'text',
  double: 'decimal'
};

const DataTypeMapping = {};
Object.entries(types.builtins).forEach(pair => {
  const [key, value] = pair;
  DataTypeMapping[value] = key;
});

const timestampDataTypes = [1114, 1184];

const timestampTypeParser = val => moment.utc(val).format(moment.HTML5_FMT.DATETIME_LOCAL_MS);

class PostgresDriver extends BaseDriver {
  constructor(config) {
    super();
    this.config = config || {};

    this.pool = new Pool({
      max: process.env.CUBEJS_DB_MAX_POOL && parseInt(process.env.CUBEJS_DB_MAX_POOL, 10) || 8,
      idleTimeoutMillis: 30000,
      host: process.env.CUBEJS_DB_HOST,
      database: process.env.CUBEJS_DB_NAME,
      port: process.env.CUBEJS_DB_PORT,
      user: process.env.CUBEJS_DB_USER,
      password: process.env.CUBEJS_DB_PASS,
      ssl: this.getSslOptions(),
      ...config
    });
    this.pool.on('error', (err) => {
      console.log(`Unexpected error on idle client: ${err.stack || err}`); // TODO
    });
  }

  async testConnection() {
    try {
      return await this.pool.query('SELECT $1::int AS number', ['1']);
    } catch (e) {
      if (e.toString().indexOf('no pg_hba.conf entry for host') !== -1) {
        throw new Error(`Please use CUBEJS_DB_SSL=true to connect: ${e.toString()}`);
      }
      throw e;
    }
  }

  async queryResponse(query, values) {
    const client = await this.pool.connect();
    try {
      await client.query(`SET TIME ZONE '${this.config.storeTimezone || 'UTC'}'`);
      await client.query(`set statement_timeout to ${(this.config.hasOwnProperty('executionTimeout')) ? this.config.executionTimeout * 1000 : 600000}`);
      const res = await client.query({
        text: query,
        values: values || [],
        types: {
          getTypeParser: (dataType, format) => {
            const isTimestamp = timestampDataTypes.indexOf(dataType) > -1;
            let parser = types.getTypeParser(dataType, format);

            if (isTimestamp) {
              parser = timestampTypeParser;
            }

            return val => parser(val);
          },
        },
      });
      return res;
    } finally {
      await client.release();
    }
  }

  async query(query, values) {
    return (await this.queryResponse(query, values)).rows;
  }

  async downloadQueryResults(query, values) {
    const res = await this.queryResponse(query, values);
    return {
      rows: res.rows,
      types: res.fields.map(f => ({
        name: f.name,
        type: this.toGenericType(DataTypeMapping[f.dataTypeID].toLowerCase())
      })),
    };
  }

  readOnly() {
    return !!this.config.readOnly;
  }

  async uploadTableWithIndexes(table, columns, tableData, indexesSql) {
    if (!tableData.rows) {
      throw new Error(`${this.constructor} driver supports only rows upload`);
    }
    await this.createTable(table, columns);
    try {
      await this.query(
        `INSERT INTO ${table}
      (${columns.map(c => this.quoteIdentifier(c.name)).join(', ')})
      SELECT * FROM UNNEST (${columns.map((c, columnIndex) => `${this.param(columnIndex)}::${this.fromGenericType(c.type)}[]`).join(', ')})`,
        columns.map(c => tableData.rows.map(r => r[c.name]))
      );
      for (let i = 0; i < indexesSql.length; i++) {
        const [query, p] = indexesSql[i].sql;
        await this.query(query, p);
      }
    } catch (e) {
      await this.dropTable(table);
      throw e;
    }
  }

  release() {
    return this.pool.end();
  }

  param(paramIndex) {
    return `$${paramIndex + 1}`;
  }

  fromGenericType(columnType) {
    return GenericTypeToPostgres[columnType] || super.fromGenericType(columnType);
  }
}

module.exports = PostgresDriver;
