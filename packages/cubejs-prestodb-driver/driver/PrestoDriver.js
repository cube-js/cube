/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `PrestoDriver` and related types declaration.
 */

const {
  getEnv,
  assertDataSource,
} = require('@cubejs-backend/shared');
const presto = require('presto-client');
const {
  map, zipObj, prop, concat
} = require('ramda');
const { BaseDriver } = require('@cubejs-backend/base-driver');
const SqlString = require('sqlstring');

/**
 * Presto driver class.
 */
class PrestoDriver extends BaseDriver {
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
    super();

    const dataSource =
      config.dataSource ||
      assertDataSource('default');
      
    this.config = {
      host: getEnv('dbHost', { dataSource }),
      port: getEnv('dbPort', { dataSource }),
      catalog:
        getEnv('prestoCatalog', { dataSource }) ||
        getEnv('dbCatalog', { dataSource }),
      schema:
        getEnv('dbName', { dataSource }) ||
        getEnv('dbSchema', { dataSource }),
      user: getEnv('dbUser', { dataSource }),
      basic_auth: getEnv('dbPass', { dataSource })
        ? {
          user: getEnv('dbUser', { dataSource }),
          password: getEnv('dbPass', { dataSource }),
        }
        : undefined,
      ssl: this.getSslOptions(dataSource),
      ...config
    };
    this.catalog = this.config.catalog;
    this.client = new presto.Client(this.config);
  }

  testConnection() {
    const query = SqlString.format('show catalogs like ?', [`%${this.catalog}%`]);

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
