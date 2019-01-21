const { reduce } = require('ramda');

const sortByKeys = (unordered) => {
  const ordered = {};

  Object.keys(unordered).sort().forEach(function(key) {
    ordered[key] = unordered[key];
  });

  return ordered;
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
    )
  }

  loadPreAggregationIntoTable(preAggregationTableName, loadSql, params, tx) {
    return this.query(loadSql, params, tx);
  }

  dropTable(tableName, tx) {
    return this.query(`DROP TABLE ${tableName}`, [], tx);
  }

  param(/* paramIndex */) {
    return '?'
  }

  testConnectionTimeout() {
    return 10000;
  }
}

module.exports = BaseDriver;
