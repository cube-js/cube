const { Pool } = require('vertica-nodejs');
const { BaseDriver } = require('@cubejs-backend/query-orchestrator');
const VerticaQuery = require('./VerticaQuery');

const defaultGenericType = 'text';
const VerticaTypeToGenericType = {
  boolean: 'boolean',
  int: 'bigint',
  float: 'double',
  date: 'date',
  timestamp: 'timestamp',
  timestamptz: 'timestamp',
  numeric: 'decimal',
};

const connectListener = async (client) => {
  await client.query('SET TIMEZONE TO \'UTC\'');
};

class VerticaDriver extends BaseDriver {
  constructor(config) {
    super();
    this.pool = new Pool({
      max:
        process.env.CUBEJS_DB_MAX_POOL && parseInt(process.env.CUBEJS_DB_MAX_POOL, 10) ||
        config.maxPoolSize || 8,
      host: process.env.CUBEJS_DB_HOST,
      database: process.env.CUBEJS_DB_NAME,
      port: process.env.CUBEJS_DB_PORT,
      user: process.env.CUBEJS_DB_USER,
      password: process.env.CUBEJS_DB_PASS,
      ssl: this.getSslOptions(),
      ...config,
    });

    this.pool.addListener('connect', connectListener);
  }

  static dialectClass() {
    return VerticaQuery;
  }

  async query(query, values) {
    const queryResult = await this.pool.query(query, values);
    return queryResult.rows;
  }

  readOnly() {
    return true;
  }

  async testConnection() {
    return this.query('SELECT 1 AS n');
  }

  async release() {
    this.pool.end();
  }

  informationSchemaQuery() {
    return `
      SELECT
        column_name,
        table_name, 
        table_schema,
        data_type
      FROM v_catalog.columns;
    `;
  }

  async createSchemaIfNotExists(schemaName) {
    return this.query(`CREATE SCHEMA IF NOT EXISTS ${schemaName};`);
  }

  getTablesQuery(schemaName) {
    return this.query(
      `SELECT table_name FROM v_catalog.tables WHERE table_schema = ${this.param(0)}`,
      [schemaName]
    );
  }

  async tableColumnTypes(table) {
    const [schema, name] = table.split('.');

    const columns = await this.query(
      `SELECT
        column_name,
        data_type
      FROM v_catalog.columns
      WHERE table_name = ${this.param(0)}
        AND table_schema = ${this.param(1)}`,
      [name, schema]
    );

    return columns.map(c => ({ name: c.column_name, type: this.toGenericType(c.data_type) }));
  }

  toGenericType(columnType) {
    const type = columnType.toLowerCase().replace(/\([0-9,]+\)/, '');
    return VerticaTypeToGenericType[type] || defaultGenericType;
  }
}

module.exports = VerticaDriver;
