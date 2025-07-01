/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `ElasticSearchDriver` and related types declaration.
 */

const {
  getEnv,
  assertDataSource,
} = require('@cubejs-backend/shared');
const { Client } = require('@elastic/elasticsearch');
const SqlString = require('sqlstring');
const { BaseDriver } = require('@cubejs-backend/base-driver');

/**
 * ElasticSearch driver class.
 */
class ElasticSearchDriver extends BaseDriver {
  /**
   * Returns default concurrency value.
   * @return {number}
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

    const auth = {
      username: getEnv('dbUser', { dataSource }),
      password: getEnv('dbPass', { dataSource }),
    };
    if (
      getEnv('elasticApiId', { dataSource }) ||
      getEnv('elasticApiKey', { dataSource })
    ) {
      auth.apiKey = {
        id: getEnv('elasticApiId', { dataSource }),
        api_key: getEnv('elasticApiKey', { dataSource }),
      };
    }

    // TODO: This config applies to AWS ES, Elastic.co ES, Native ES
    // and OpenDistro ES. They have different dialects according to
    // their respective documentation.
    this.config = {
      auth,
      url: getEnv('dbUrl', { dataSource }),
      ssl: this.getSslOptions(dataSource),
      openDistro:
        (getEnv('elasticOpenDistro', { dataSource }) || 'false')
          .toLowerCase() === 'true' ||
        getEnv('dbType', { dataSource }) === 'odelasticsearch',
      queryFormat:
        getEnv('elasticQueryFormat', { dataSource }) || 'jdbc',
      ...config,
    };

    this.client = new Client({
      node: this.config.url,
      cloud: this.config.cloud,
      auth: this.config.auth,
      ssl: this.config.ssl
    });

    if (this.config.openDistro) {
      this.sqlClient = new Client({
        node: `${this.config.url}/_opendistro`,
        ssl: this.config.ssl,
        auth: this.config.auth,
      });
    } else {
      this.sqlClient = this.client;
    }
  }

  /**
   * Returns the configurable driver options
   * Note: It returns the unprefixed option names.
   * In case of using multisources options need to be prefixed manually.
   */
  static driverEnvVariables() {
    return [
      'CUBEJS_DB_URL',
      'CUBEJS_DB_ELASTIC_QUERY_FORMAT',
      'CUBEJS_DB_ELASTIC_OPENDISTRO',
      'CUBEJS_DB_ELASTIC_APIKEY_ID',
      'CUBEJS_DB_ELASTIC_APIKEY_KEY',
    ];
  }

  async testConnection() {
    return this.client.cat.indices({
      format: 'json'
    });
  }

  async release() {
    await this.client.close();

    if (this.config.openDistro && this.sqlClient) {
      await this.sqlClient.close();
    }
  }

  readOnly() {
    // Both ES X-Pack & Open Distro don't support table creation
    return true;
  }

  async query(query, values) {
    try {
      const result = (await this.sqlClient.sql.query({ // TODO cursor
        format: this.config.queryFormat,
        body: {
          query: SqlString.format(query, values)
        }
      })).body;

      // INFO: cloud left in place for backward compatibility
      if (this.config.cloud || ['jdbc', 'json'].includes(this.config.queryFormat)) {
        const compiled = result.rows.map(
          r => result.columns.reduce((prev, cur, idx) => ({ ...prev, [cur.name]: r[idx] }), {})
        );

        return compiled;
      }
      return result && result.aggregations && this.traverseAggregations(result.aggregations);
    } catch (e) {
      if (e.body) {
        throw new Error(JSON.stringify(e.body, null, 2));
      }

      throw e;
    }
  }

  traverseAggregations(aggregations) {
    const fields = Object.keys(aggregations).filter(k => k !== 'key' && k !== 'doc_count');
    if (fields.find(f => aggregations[f].hasOwnProperty('value'))) {
      return [fields.map(f => ({ [f]: aggregations[f].value })).reduce((a, b) => ({ ...a, ...b }))];
    }
    if (fields.length === 0) {
      return [{}];
    }
    if (fields.length !== 1) {
      throw new Error(`Unexpected multiple fields at ${fields.join(', ')}`);
    }
    const dimension = fields[0];
    if (!aggregations[dimension].buckets) {
      throw new Error(`Expecting buckets at dimension ${dimension}: ${aggregations[dimension]}`);
    }
    return aggregations[dimension].buckets.map(b => this.traverseAggregations(b).map(
      innerRow => ({ ...innerRow, [dimension]: b.key })
    )).reduce((a, b) => a.concat(b), []);
  }

  async tablesSchema() {
    const indices = await this.client.cat.indices({
      format: 'json'
    });

    const schema = (await Promise.all(indices.body.map(async i => {
      const props = (await this.client.indices.getMapping({ index: i.index })).body[i.index].mappings.properties || {};
      return {
        [i.index]: Object.keys(props).map(p => ({ name: p, type: props[p].type })).filter(c => !!c.type)
      };
    }))).reduce((a, b) => ({ ...a, ...b }));

    return {
      main: schema
    };
  }
}

module.exports = ElasticSearchDriver;
