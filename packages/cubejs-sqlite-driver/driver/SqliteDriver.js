const sqlite3 = require('sqlite3');
const { BaseDriver } = require('@cubejs-backend/query-orchestrator');

class SqliteDriver extends BaseDriver {
  constructor(config) {
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
      SELECT m.name AS table_name,
             p.name AS column_name,
             p.type AS column_type
        FROM sqlite_master AS m
        JOIN pragma_table_info(m.name) AS p
       WHERE m.name NOT IN ('sqlite_sequence', 'sqlite_stat1')
       ORDER BY m.name, p.cid
   `;
  }

  async tablesSchema() {
    const query = this.informationSchemaQuery();

    const tables = await this.query(query);

    return {
      main: tables.reduce((acc, curr) => {
        if (!acc[curr.table_name]) {
          acc[curr.table_name] = [];
        }
        acc[curr.table_name].push({
          name: curr.column_name,
          type: curr.column_type
        });
        return acc;
      }, {})
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
