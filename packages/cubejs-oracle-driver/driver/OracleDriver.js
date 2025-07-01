/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `OracleDriver` and related types declaration.
 */

const {
  getEnv,
  assertDataSource,
} = require('@cubejs-backend/shared');
const { BaseDriver, TableColumn } = require('@cubejs-backend/base-driver');
const oracledb = require('oracledb');
const { reduce } = require('ramda');

const sortByKeys = (unordered) => {
  const ordered = {};

  Object.keys(unordered).sort().forEach((key) => {
    ordered[key] = unordered[key];
  });

  return ordered;
};

const reduceCb = (result, i) => {
  let schema = (result[i.table_schema] || {});
  let tables = (schema[i.table_name] || []);
  let attributes = new Array();

  if (i.key_type === "P" || i.key_type === "U") {
    attributes.push(["primaryKey"]);
  }

  tables.push({
    name: i.column_name,
    type: i.data_type,
    attributes
  });

  schema[i.table_name] = tables.sort();
  result[i.table_schema] = sortByKeys(schema);

  return sortByKeys(result);
};

/**
 * Oracle driver class.
 */
class OracleDriver extends BaseDriver {
  /**
   * Returns default concurrency value.
   */
  static getDefaultConcurrency() {
    return 2;
  }

  /**
   * Class constructor.
   */
  constructor(config = {}) {
    super({
      testConnectionTimeout: config.testConnectionTimeout,
    });

    const dataSource =
      config.dataSource ||
      assertDataSource('default');

    this.db = oracledb;
    this.db.outFormat = this.db.OBJECT;
    this.db.partRows = 100000;
    this.db.maxRows = 100000;
    this.db.prefetchRows = 500;
    this.config = {
      user: getEnv('dbUser', { dataSource }),
      password: getEnv('dbPass', { dataSource }),
      db: getEnv('dbName', { dataSource }),
      host: getEnv('dbHost', { dataSource }),
      port: getEnv('dbPort', { dataSource }) || 1521,
      poolMin: 0,
      poolMax:
        config.maxPoolSize ||
        getEnv('dbMaxPoolSize', { dataSource }) ||
        50,
      ...config
    };
    this.config.connectionString = this.config.connectionString || `${this.config.host}:${this.config.port}/${this.config.db}`;
  }

  async tablesSchema() {
    const data = await this.query(`
      select tc.owner         "table_schema"
          , tc.table_name     "table_name"
          , tc.column_name    "column_name"
          , tc.data_type      "data_type"
          , c.constraint_type "key_type"
      from all_tab_columns tc
      left join all_cons_columns cc
        on (tc.owner, tc.table_name, tc.column_name)
        in ((cc.owner, cc.table_name, cc.column_name))
      left join all_constraints c
        on (tc.owner, tc.table_name, cc.constraint_name)
        in ((c.owner, c.table_name, c.constraint_name))
        and c.constraint_type
        in ('P','U')
      where tc.owner = user
    `);

    return reduce(reduceCb, {}, data);
  }

  async getConnectionFromPool() {
    if (!this.pool) {
      this.pool = await this.db.createPool(this.config);
    }

    return this.pool.getConnection()
  }

  async testConnection() {
    await this.query('SELECT 1 FROM DUAL', {});
  }

  async createTable(quotedTableName, columns) {
    if (quotedTableName.length > 128) {
      throw new Error('Oracle can not work with table names longer than 128 symbols. ' +
        `Consider using the 'sqlAlias' attribute in your cube definition for ${quotedTableName}.`);
    }
    return super.createTable(quotedTableName, columns);
  }

  async query(query, values) {
    const conn = await this.getConnectionFromPool();

    try {
      const res = await conn.execute(query, values || {});
      return res && res.rows;
    } catch (e) {
      throw (e);
    } finally {
      try {
        await conn.close();
      } catch (e) {
        throw e;
      }
    }
  }

  release() {
    return this.pool && this.pool.close();
  }

  readOnly() {
    return true;
  }

  wrapQueryWithLimit(query) {
    query.query = `SELECT * FROM (${query.query}) AS t WHERE ROWNUM <= ${query.limit}`;
  }
}

module.exports = OracleDriver;
