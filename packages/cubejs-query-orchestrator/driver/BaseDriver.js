const { reduce } = require('ramda');

const sortByKeys = (unordered) => {
  const ordered = {};

  Object.keys(unordered).sort().forEach(function(key) {
    ordered[key] = unordered[key];
  });

  return ordered;
};

const DbTypeToGenericType = {
  'timestamp without time zone': 'timestamp',
  'integer': 'int',
  'character varying': 'text',
  'varchar': 'text',
  'text': 'text',
  'string': 'text',
  'boolean': 'boolean',
  'bigint': 'bigint',
  'time': 'string',
  'datetime': 'timestamp',
  'date': 'date',
  'double precision': 'decimal'
};

class BaseDriver {
  informationSchemaQuery() {
    return `
      SELECT columns.column_name,
             columns.table_name,
             columns.table_schema,
             columns.data_type
      FROM information_schema.columns
      WHERE columns.table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
   `;
  }

  testConnection() {
    throw 'Not implemented';
  }

  query() {
    throw 'Not implemented';
  }

  tablesSchema() {
    const query = this.informationSchemaQuery();

    const reduceCb = (result, i) => {
      let schema = (result[i.table_schema] || {});
      let tables = (schema[i.table_name] || []);

      tables.push({ name: i.column_name, type: i.data_type, attributes: i.key_type ? ['primaryKey'] : [] });

      tables.sort();
      schema[i.table_name] = tables;
      schema = sortByKeys(schema);
      result[i.table_schema] = schema;

      return sortByKeys(result);
    };

    return this.query(query).then(data => reduce(reduceCb, {}, data));
  }

  createSchemaIfNotExists(schemaName) {
    return this.query(
      `SELECT schema_name FROM information_schema.schemata WHERE schema_name = ${this.param(0)}`,
      [schemaName]
    ).then((schemas) => {
        if (schemas.length === 0) {
          return this.query("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        }
      });
  }

  getTablesQuery(schemaName) {
    return this.query(
      `SELECT table_name FROM information_schema.tables WHERE table_schema = ${this.param(0)}`,
      [schemaName]
    );
  }

  loadPreAggregationIntoTable(preAggregationTableName, loadSql, params, tx) {
    return this.query(loadSql, params, tx);
  }

  dropTable(tableName, tx) {
    return this.query(`DROP TABLE ${tableName}`, [], tx);
  }

  param(/* paramIndex */) {
    return '?';
  }

  testConnectionTimeout() {
    return 10000;
  }

  async downloadTable(table) {
    return { rows: await this.query(`SELECT * FROM ${table}`) };
  }

  async uploadTable(table, columns, tableData) {
    if (!tableData.rows) {
      throw new Error(`${this.constructor} driver supports only rows upload`);
    }
    await this.createTable(table, columns);
    try {
      for (let i = 0; i < tableData.rows.length; i++) {
        await this.query(
          `INSERT INTO ${table}
        (${columns.map(c => this.quoteIdentifier(c.name)).join(', ')})
        VALUES (${columns.map((c, paramIndex) => this.param(paramIndex)).join(', ')})`,
          columns.map(c => this.toColumnValue(tableData.rows[i][c.name], c.type))
        );
      }
    } catch (e) {
      await this.dropTable(table);
      throw e;
    }
  }

  toColumnValue(value, genericType) {
    return value;
  }

  async tableColumnTypes(table) {
    const [schema, name] = table.split('.');
    const columns = await this.query(
      `SELECT columns.column_name,
             columns.table_name,
             columns.table_schema,
             columns.data_type
      FROM information_schema.columns 
      WHERE table_name = ${this.param(0)} AND table_schema = ${this.param(1)}`,
      [name, schema]
    );
    return columns.map(c => ({ name: c.column_name, type: this.toGenericType(c.data_type) }));
  }

  createTable(quotedTableName, columns) {
    return this.query(this.createTableSql(quotedTableName, columns), []);
  }

  createTableSql(quotedTableName, columns) {
    columns = columns.map(c => `${this.quoteIdentifier(c.name)} ${this.fromGenericType(c.type)}`);
    return `CREATE TABLE ${quotedTableName} (${columns.join(', ')})`;
  }

  toGenericType(columnType) {
    return DbTypeToGenericType[columnType] || columnType;
  }

  fromGenericType(columnType) {
    return columnType;
  }

  quoteIdentifier(identifier) {
    return `"${identifier}"`;
  }
}

module.exports = BaseDriver;
