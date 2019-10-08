/**
 * Vanilla JavaScript Cube.js client.
 * @module @cubejs-client/core
 * @permalink /@cubejs-client-core
 * @category Cube.js Frontend
 * @menuOrder 2
 */

import fetch from 'cross-fetch';
import ResultSet from './ResultSet';
import SqlQuery from './SqlQuery';
import Meta from './Meta';
import ProgressResult from './ProgressResult';

const API_URL = process.env.CUBEJS_API_URL;

let mutexCounter = 0;

const MUTEX_ERROR = 'Mutex has been changed';

const mutexPromise = (promise) => {
  return new Promise((resolve, reject) => {
    promise.then(r => resolve(r), e => e !== MUTEX_ERROR && reject(e));
  });
};

/**
 * Main class for accessing Cube.js API
 * @order -5
 */
class CubejsApi {
  constructor(apiToken, options) {
    options = options || {};
    this.apiToken = apiToken;
    this.apiUrl = options.apiUrl || API_URL;
  }

  request(url, config) {
    return fetch(
      `${this.apiUrl}${url}`,
      Object.assign({ headers: { Authorization: this.apiToken, 'Content-Type': 'application/json' }}, config || {})
    );
  }

  loadMethod(request, toResult, options, callback) {
    const mutexValue = ++mutexCounter;
    if (typeof options === 'function' && !callback) {
      callback = options;
      options = undefined;
    }

    options = options || {};

    const mutexKey = options.mutexKey || 'default';
    if (options.mutexObj) {
      options.mutexObj[mutexKey] = mutexValue;
    }

    const checkMutex = () => {
      if (options.mutexObj && options.mutexObj[mutexKey] !== mutexValue) {
        throw MUTEX_ERROR;
      }
    };

    const loadImpl = async () => {
      const response = await request();
      if (response.status === 502) {
        checkMutex();
        return loadImpl(); // TODO backoff wait
      }
      const body = await response.json();
      if (body.error === 'Continue wait') {
        checkMutex();
        if (options.progressCallback) {
          options.progressCallback(new ProgressResult(body));
        }
        return loadImpl();
      }
      if (response.status !== 200) {
        checkMutex();
        throw new Error(body.error); // TODO error class
      }
      checkMutex();
      return toResult(body);
    };
    if (callback) {
      mutexPromise(loadImpl()).then(r => callback(null, r), e => callback(e));
    } else {
      return mutexPromise(loadImpl());
    }
  }

  /**
   * Fetch data for passed `query`.
   *
   * ```js
   * import cubejs from '@cubejs-client/core';
   * import Chart from 'chart.js';
   * import chartjsConfig from './toChartjsData';
   *
   * const cubejsApi = cubejs('CUBEJS_TOKEN');
   *
   * const resultSet = await cubejsApi.load({
   *  measures: ['Stories.count'],
   *  timeDimensions: [{
   *    dimension: 'Stories.time',
   *    dateRange: ['2015-01-01', '2015-12-31'],
   *    granularity: 'month'
   *   }]
   * });
   *
   * const context = document.getElementById('myChart');
   * new Chart(context, chartjsConfig(resultSet));
   * ```
   * @param query - [Query object](query-format)
   * @param options
   * @param callback
   * @returns {Promise} for {@link ResultSet} if `callback` isn't passed
   */
  load(query, options, callback) {
    return this.loadMethod(
      () => this.request(`/load?query=${encodeURIComponent(JSON.stringify(query))}`),
      (body) => new ResultSet(body),
      options,
      callback
    );
  }

  sql(query, options, callback) {
    return this.loadMethod(
      () => this.request(`/sql?query=${JSON.stringify(query)}`),
      (body) => new SqlQuery(body),
      options,
      callback
    );
  }

  meta(options, callback) {
    return this.loadMethod(
      () => this.request(`/meta`),
      (body) => new Meta(body),
      options,
      callback
    );
  }
}

/**
 * Create instance of `CubejsApi`.
 * API entry point.
 *
 * ```javascript
 import cubejs from '@cubejs-client/core';

 const cubejsApi = cubejs(
 'CUBEJS-API-TOKEN',
 { apiUrl: 'http://localhost:4000/cubejs-api/v1' }
 );
 ```
 * @name cubejs
 * @param apiToken - [API token](security) is used to authorize requests and determine SQL database you're accessing.
 * In the development mode, Cube.js Backend will print the API token to the console on on startup.
 * @param options - options object.
 * @param options.apiUrl - URL of your Cube.js Backend.
 * By default, in the development environment it is `http://localhost:4000/cubejs-api/v1`.
 * @returns {CubejsApi}
 * @order -10
 */
export default (apiToken, options) => {
  return new CubejsApi(apiToken, options);
};
