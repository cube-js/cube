/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `DremioDriver` and related types declaration.
 */

const {
  getEnv,
  assertDataSource,
  pausePromise,
} = require('@cubejs-backend/shared');
const axios = require('axios');
const SqlString = require('sqlstring');
const { BaseDriver } = require('@cubejs-backend/base-driver');
const DremioQuery = require('./DremioQuery');

// limit - Determines how many rows are returned (maximum of 500). Default: 100
// @see https://docs.dremio.com/rest-api/jobs/get-job.html
const DREMIO_JOB_LIMIT = 500;

const applyParams = (query, params) => SqlString.format(query, params);

/**
 * Dremio driver class.
 */
class DremioDriver extends BaseDriver {
  static dialectClass() {
    return DremioQuery;
  }

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

    this.config = {
      dbUrl:
        config.dbUrl ||
        getEnv('dbUrl', { dataSource }) ||
        '',
      dremioAuthToken:
        config.dremioAuthToken ||
        getEnv('dremioAuthToken', { dataSource }) ||
        '',
      host:
        config.host ||
        getEnv('dbHost', { dataSource }) ||
        'localhost',
      port:
        config.port ||
        getEnv('dbPort', { dataSource }) ||
        9047,
      user:
        config.user ||
        getEnv('dbUser', { dataSource }),
      password:
        config.password ||
        getEnv('dbPass', { dataSource }),
      database:
        config.database ||
        getEnv('dbName', { dataSource }),
      ssl:
        config.ssl ||
        getEnv('dbSsl', { dataSource }),
      ...config,
      pollTimeout: (
        config.pollTimeout ||
        getEnv('dbPollTimeout', { dataSource }) ||
        getEnv('dbQueryTimeout', { dataSource })
      ) * 1000,
      pollMaxInterval: (
        config.pollMaxInterval ||
        getEnv('dbPollMaxInterval', { dataSource })
      ) * 1000,
    };

    if (this.config.dbUrl) {
      this.config.url = this.config.dbUrl;
      this.config.apiVersion = '';
      if (this.config.dremioAuthToken === '') {
        throw new Error('dremioAuthToken is blank');
      }
    } else {
      const protocol = (this.config.ssl === true || this.config.ssl === 'true')
        ? 'https'
        : 'http';
      this.config.url = `${protocol}://${this.config.host}:${this.config.port}`;
      this.config.apiVersion = '/api/v3';
    }
  }

  /**
   * @public
   * @return {Promise<void>}
   */
  async testConnection() {
    return this.getToken();
  }

  quoteIdentifier(identifier) {
    return `"${identifier}"`;
  }

  /**
   * @protected
   */
  async getToken() {
    if (this.config.dremioAuthToken) {
      const bearerToken = `Bearer ${this.config.dremioAuthToken}`;
      await axios.get(
        `${this.config.url}${this.config.apiVersion}/catalog`,
        {
          headers: {
            Authorization: bearerToken
          },
        },
      );

      return bearerToken;
    }

    if (this.authToken && this.authToken.expires > new Date().getTime()) {
      return `_dremio${this.authToken.token}`;
    }

    const { data } = await axios.post(`${this.config.url}/apiv2/login`, {
      userName: this.config.user,
      password: this.config.password
    });

    this.authToken = data;
    return `_dremio${this.authToken.token}`;
  }

  /**
   * @protected
   *
   * @param {string} method
   * @param {string} url
   * @param {object} [data]
   * @return {Promise<AxiosResponse<any>>}
   */
  async restDremioQuery(method, url, data) {
    const token = await this.getToken();

    return axios.request({
      method,
      url: `${this.config.url}${this.config.apiVersion}${url}`,
      headers: {
        Authorization: token
      },
      data,
    });
  }

  /**
   * @protected
   */
  async getJobStatus(jobId) {
    const { data } = await this.restDremioQuery('get', `/job/${jobId}`);

    if (data.jobState === 'FAILED') {
      throw new Error(data.errorMessage);
    }

    if (data.jobState === 'CANCELED') {
      throw new Error(`Job ${jobId} has been canceled`);
    }

    if (data.jobState === 'COMPLETED') {
      return data;
    }

    return null;
  }

  /**
   * @protected
   */
  async getJobResults(jobId, limit = 500, offset = 0) {
    return this.restDremioQuery('get', `/job/${jobId}/results?offset=${offset}&limit=${limit}`);
  }

  /**
   * @protected
   * @param {string} sql
   * @return {Promise<*>}
   */
  async executeQuery(sql) {
    const { data } = await this.restDremioQuery('post', '/sql', { sql });
    return data.id;
  }

  async query(query, values) {
    const queryString = applyParams(
      query,
      (values || []).map(s => (typeof s === 'string' ? {
        toSqlString: () => SqlString.escape(s).replace(/\\\\([_%])/g, '\\$1').replace(/\\'/g, '\'\'')
      } : s))
    );

    await this.getToken();
    const jobId = await this.executeQuery(queryString);

    const startedTime = Date.now();

    for (let i = 0; Date.now() - startedTime <= this.config.pollTimeout; i++) {
      const job = await this.getJobStatus(jobId);
      if (job) {
        const queries = [];

        for (let offset = 0; offset < job.rowCount; offset += DREMIO_JOB_LIMIT) {
          queries.push(this.getJobResults(jobId, DREMIO_JOB_LIMIT, offset));
        }

        const results = await Promise.all(queries);

        return results.reduce(
          (result, { data }) => result.concat(data.rows),
          []
        );
      }

      await pausePromise(
        Math.min(this.config.pollMaxInterval, 200 * i),
      );
    }

    throw new Error(
      `DremioQuery job timeout reached ${this.config.pollTimeout}ms`,
    );
  }

  async refreshTablesSchema(path) {
    const { data } = await this.restDremioQuery('get', `/catalog/by-path/${path}`);
    if (!data || !data.children) {
      return true;
    }

    const queries = data.children.map(element => {
      const url = element.path.join('/');
      return this.refreshTablesSchema(url);
    });

    return Promise.all(queries);
  }

  async tablesSchema() {
    if (!this.config.database) {
      throw new Error('CUBEJS_DB_NAME can`t be empty.');
    }

    // By some reason query generated by super.tablesSchema will return only tables that we usd before
    // So, function refreshTablesSchema used for get all tables by rest api.
    // After this, query generated by super.tablesSchema will return all table`s.
    await this.refreshTablesSchema(this.config.database);
    return super.tablesSchema();
  }

  informationSchemaQuery() {
    const q = `${super.informationSchemaQuery()} AND columns.table_schema NOT IN ('INFORMATION_SCHEMA', 'sys.cache')`;
    console.log(q);
    return q;
  }
}

module.exports = DremioDriver;
