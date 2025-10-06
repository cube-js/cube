/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `SqliteDriver` and related types declaration.
 */

const {
  getEnv,
  assertDataSource,
} = require('@cubejs-backend/shared');
const sqlite3 = require('sqlite3');
const { BaseDriver } = require('@cubejs-backend/base-driver');

/**
 * SQLight driver class.
 */
class SqliteDriver extends BaseDriver {
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

    this.config = {
      database: getEnv('dbName', { dataSource }),
      ...config
    };

    if (!this.config.db) {
      this.config.db = new sqlite3.Database(this.config.database);
    }
  }

  async testConnection() {
    return this.query('SELECT 1');
  }

  query(query, values) {
    return new Promise(
      (resolve, reject) => this.config.db.all(
        query,
        values || [],
        (err, result) => (err ? reject(err) : resolve(result))
      )
    );
  }

  async release() {
    await new Promise((resolve, reject) => this.config.db.close((err) => (err ? reject(err) : resolve())));
  }

  informationSchemaQuery() {
    return `
      SELECT name
      FROM sqlite_master
      WHERE type='table'
      AND name!='sqlite_sequence'
      ORDER BY name
   `;
  }

  tableColumnsQuery(tableName) {
    return `
      SELECT name, type
      FROM pragma_table_info('${tableName}')
    `;
  }

  async tablesSchema() {
    const query = this.informationSchemaQuery();

    const tables = await this.query(query);

    const tableColumns = await Promise.all(tables.map(async table => {
      const columns = await this.query(this.tableColumnsQuery(table.name));
      return [table.name, columns];
    }));

    return {
      main: Object.fromEntries(tableColumns)
    };
  }

  createSchemaIfNotExists(schemaName) {
    return this.query(
      'PRAGMA database_list'
    ).then((schemas) => {
      if (!schemas.find(s => s.name === schemaName)) {
        return this.query(`ATTACH DATABASE ${schemaName} AS ${schemaName}`);
      }
      return null;
    });
  }

  async getTablesQuery(schemaName) {
    const attachedDatabases = await this.query(
      'PRAGMA database_list'
    );
    if (!attachedDatabases.find(s => s.name === schemaName)) {
      return [];
    }
    return this.query(`SELECT name as table_name FROM ${schemaName}.sqlite_master WHERE type='table' ORDER BY name`);
  }
}

module.exports = SqliteDriver;
