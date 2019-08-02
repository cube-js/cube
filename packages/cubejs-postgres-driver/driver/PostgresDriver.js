const BaseDriver = require('@cubejs-backend/query-orchestrator/driver/BaseDriver');
const { Pool } = require('pg');

const GenericTypeToPostgres = {
  string: 'text'
};

class PostgresDriver extends BaseDriver {
  constructor(config) {
    super();
    this.config = config || {};
    this.pool = new Pool({
      max: 8,
      idleTimeoutMillis: 30000,
      host: process.env.CUBEJS_DB_HOST,
      database: process.env.CUBEJS_DB_NAME,
      port: process.env.CUBEJS_DB_PORT,
      user: process.env.CUBEJS_DB_USER,
      password: process.env.CUBEJS_DB_PASS,
      ssl: process.env.CUBEJS_DB_SSL ? {} : undefined,
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

  async query(query, values) {
    const client = await this.pool.connect();
    try {
      await client.query(`SET TIME ZONE '${this.config.storeTimezone || 'UTC'}'`);
      await client.query("set statement_timeout to 600000");
      const res = await client.query({
        text: query,
        values: values || []
      });
      return res && res.rows;
    } finally {
      await client.release();
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
