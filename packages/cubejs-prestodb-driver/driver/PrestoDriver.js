const presto = require('presto-client');
const {
  map, zipObj, prop, concat
} = require('ramda');
const BaseDriver = require('@cubejs-backend/query-orchestrator/driver/BaseDriver');
const SqlString = require('sqlstring');

class PrestoDriver extends BaseDriver {
  constructor(config) {
    super();

    this.config = {
      host: process.env.CUBEJS_DB_HOST,
      port: process.env.CUBEJS_DB_PORT,
      catalog: process.env.CUBEJS_DB_CATALOG,
      schema: process.env.CUBEJS_DB_SCHEMA,
      user: process.env.CUBEJS_DB_USER,
      basic_auth: process.env.CUBEJS_DB_PASS ? {
        user: process.env.CUBEJS_DB_USER,
        password: process.env.CUBEJS_DB_PASS
      } : undefined,
      ...config
    };

    this.catalog = this.config.catalog;
    this.client = new presto.Client(this.config);
  }

  testConnection() {
    const query = SqlString.format(`show catalogs like ?`, [`%${this.catalog}%`]);

    return this.queryPromised(query)
      .then(catalogs => {
        if (catalogs.length === 0) {
          throw new Error(`Catalog not found '${this.catalog}'`);
        }
      });
  }

  query(query, values) {
    const queryWithParams = SqlString.format(query, values);
    return this.queryPromised(queryWithParams);
  }

  queryPromised(query) {
    return new Promise((resolve, reject) => {
      let fullData = [];

      this.client.execute({
        query,
        schema: this.config.schema || 'default',
        timezone: this.config.timezone,
        data: (error, data, columns) => {
          const normalData = this.normalizeResultOverColumns(data, columns);
          fullData = concat(normalData, fullData);
        },
        success: () => {
          resolve(fullData);
        },
        error: error => {
          reject(new Error(`${error.message}\n${error.error}`));
        }
      });
    });
  }

  normalizeResultOverColumns(data, columns) {
    const columnNames = map(prop('name'), columns || []);
    const arrayToObject = zipObj(columnNames);
    return map(arrayToObject, data || []);
  }
}

module.exports = PrestoDriver;
