/**
 * Vanilla JavaScript Cube.js client.
 * @module @cubejs-client/core
 * @permalink /@cubejs-client-core
 * @category Cube.js Frontend
 * @menuOrder 2
 */

import ResultSet from './ResultSet';
import SqlQuery from './SqlQuery';
import Meta from './Meta';
import ProgressResult from './ProgressResult';
import HttpTransport from './HttpTransport';

const API_URL = process.env.CUBEJS_API_URL;

let mutexCounter = 0;

const MUTEX_ERROR = 'Mutex has been changed';

const mutexPromise = (promise) => new Promise((resolve, reject) => {
  promise.then(r => resolve(r), e => e !== MUTEX_ERROR && reject(e));
});

/**
 * Main class for accessing Cube.js API
 * @order -5
 */
class CubejsApi {
  constructor(apiToken, options) {
    if (typeof apiToken === 'object') {
      options = apiToken;
      apiToken = undefined;
    }
    options = options || {};
    this.apiToken = apiToken;
    this.apiUrl = options.apiUrl || API_URL;
    this.headers = options.headers || {};
    this.transport = options.transport || new HttpTransport({
      authorization: typeof apiToken === 'function' ? undefined : apiToken,
      apiUrl: this.apiUrl,
      headers: this.headers
    });
    this.pollInterval = options.pollInterval || 5;
  }

  request(method, params) {
    return this.transport.request(method, params);
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

    const requestPromise = this.updateTransportAuthorization().then(() => request());

    let unsubscribed = false;

    const checkMutex = async () => {
      const requestInstance = await requestPromise;

      if (options.mutexObj && options.mutexObj[mutexKey] !== mutexValue) {
        unsubscribed = true;
        if (requestInstance.unsubscribe) {
          await requestInstance.unsubscribe();
        }
        throw MUTEX_ERROR;
      }
    };

    const loadImpl = async (response, next) => {
      const requestInstance = await requestPromise;

      const subscribeNext = async () => {
        if (options.subscribe && !unsubscribed) {
          if (requestInstance.unsubscribe) {
            return next();
          } else {
            await new Promise(resolve => setTimeout(() => resolve(), this.pollInterval * 1000));
            return next();
          }
        }
        return null;
      };

      const continueWait = async (wait) => {
        if (!unsubscribed) {
          if (wait) {
            await new Promise(resolve => setTimeout(() => resolve(), this.pollInterval * 1000));
          }
          return next();
        }
        return null;
      };

      await this.updateTransportAuthorization();

      if (response.status === 502) {
        await checkMutex();
        return continueWait(true);
      }
      const body = await response.json();
      if (body.error === 'Continue wait') {
        await checkMutex();
        if (options.progressCallback) {
          options.progressCallback(new ProgressResult(body));
        }
        return continueWait();
      }
      if (response.status !== 200) {
        await checkMutex();
        if (!options.subscribe && requestInstance.unsubscribe) {
          await requestInstance.unsubscribe();
        }
        const error = new Error(body.error); // TODO error class
        if (callback) {
          callback(error);
        } else {
          throw error;
        }

        return subscribeNext();
      }
      await checkMutex();
      if (!options.subscribe && requestInstance.unsubscribe) {
        await requestInstance.unsubscribe();
      }
      const result = toResult(body);
      if (callback) {
        callback(null, result);
      } else {
        return result;
      }

      return subscribeNext();
    };

    const promise = requestPromise.then(requestInstance => mutexPromise(requestInstance.subscribe(loadImpl)));

    if (callback) {
      return {
        unsubscribe: async () => {
          const requestInstance = await requestPromise;

          unsubscribed = true;
          if (requestInstance.unsubscribe) {
            return requestInstance.unsubscribe();
          }
          return null;
        }
      };
    } else {
      return promise;
    }
  }

  async updateTransportAuthorization() {
    if (typeof this.apiToken === 'function') {
      const token = await this.apiToken();
      if (this.transport.authorization !== token) {
        this.transport.authorization = token;
      }
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
      () => this.request(`load`, { query }),
      (body) => new ResultSet(body),
      options,
      callback
    );
  }

  /**
   * Get generated SQL string for given `query`.
   * @param query - [Query object](query-format)
   * @param options
   * @param callback
   * @return {Promise} for {@link SqlQuery} if `callback` isn't passed
   */
  sql(query, options, callback) {
    return this.loadMethod(
      () => this.request(`sql`, { query }),
      (body) => new SqlQuery(body),
      options,
      callback
    );
  }

  /**
   * Get meta description of cubes available for querying.
   * @param options
   * @param callback
   * @return {Promise} for {@link Meta} if `callback` isn't passed
   */
  meta(options, callback) {
    return this.loadMethod(
      () => this.request(`meta`),
      (body) => new Meta(body),
      options,
      callback
    );
  }

  subscribe(query, options, callback) {
    return this.loadMethod(
      () => this.request(`subscribe`, { query }),
      (body) => new ResultSet(body),
      { ...options, subscribe: true },
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
 * Can be an async function without arguments that returns API token.
 * @param options - options object.
 * @param options.apiUrl - URL of your Cube.js Backend.
 * By default, in the development environment it is `http://localhost:4000/cubejs-api/v1`.
 * @returns {CubejsApi}
 * @order -10
 */
export default (apiToken, options) => new CubejsApi(apiToken, options);

export { HttpTransport };
