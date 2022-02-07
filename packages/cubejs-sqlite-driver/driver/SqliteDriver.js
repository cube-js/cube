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
      WITH column_schema
           AS (SELECT m.name AS table_name,
                      p.name AS column_name,
                      p.type AS column_type
                 FROM sqlite_master AS m
                 JOIN pragma_table_info(m.name) AS p
                WHERE m.name NOT IN ('sqlite_sequence', 'sqlite_stat1')
                ORDER BY m.name, p.cid
              ),
           table_schema
           AS (SELECT table_name,
                      JSON_GROUP_ARRAY(JSON_OBJECT('name', column_name, 'type', column_type)) AS columns_as_json
                 FROM column_schema
                GROUP BY table_name
                ORDER BY table_name
              )
      SELECT JSON_GROUP_OBJECT(table_name, JSON(columns_as_json)) AS schema_as_json
        FROM table_schema
   `;
  }

  async tablesSchema() {
    const query = this.informationSchemaQuery();

    const tables = await this.query(query);

    if (1 === tables.length && Object.prototype.hasOwnProperty.call(tables[0], 'schema_as_json')) {
      return {
        main: JSON.parse(tables[0].schema_as_json)
      };
    } else {
      throw new Error(`Unable to extract schema from SQLite database.`, JSON.stringify(tables));
    }
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
    return this.query(`SELECT name AS table_name FROM ${schemaName}.sqlite_master WHERE type = 'table' ORDER BY name`);
  }
}

module.exports = SqliteDriver;
