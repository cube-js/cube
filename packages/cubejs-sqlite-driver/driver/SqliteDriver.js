const sqlite3 = require('sqlite3');
const { BaseDriver } = require('@cubejs-backend/query-orchestrator');

class SqliteDriver extends BaseDriver {
  /**
   * Returns default concurrency value.
   */
  static getDefaultConcurrency() {
    return 2;
  }

  constructor(config = {}) {
    super();
    this.config = {
      database: process.env.CUBEJS_DB_NAME,
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
      SELECT name, sql
      FROM sqlite_master
      WHERE type='table'
      AND name!='sqlite_sequence'
      ORDER BY name
   `;
  }

  async tablesSchema() {
    const query = this.informationSchemaQuery();

    const tables = await this.query(query);

    return {
      main: tables.reduce((acc, table) => ({
        ...acc,
        [table.name]: table.sql
          // remove EOL for next .match to read full string
          .replace(/\n/g, '')
          // extract fields
          .match(/\((.*)\)/)[1]
          // split fields
          .split(',')
          .map((nameAndType) => {
            const match = nameAndType
              .trim()
              // replace \t with whitespace
              .replace(/\t/g, ' ')
              // obtain "([|`|")?name(]|`|")? type"
              .match(/([|`|"])?([^[\]"`]+)(]|`|")?\s+(\w+)/);
            return { name: match[2], type: match[4] };
          })
      }), {}),
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
