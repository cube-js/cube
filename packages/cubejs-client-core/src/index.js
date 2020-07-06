import uuid from 'uuid/v4';
import ResultSet from './ResultSet';
import SqlQuery from './SqlQuery';
import Meta from './Meta';
import ProgressResult from './ProgressResult';
import HttpTransport from './HttpTransport';
import RequestError from './RequestError';

const API_URL = process.env.CUBEJS_API_URL;

let mutexCounter = 0;

const MUTEX_ERROR = 'Mutex has been changed';

const mutexPromise = (promise) => new Promise((resolve, reject) => {
  promise.then(r => resolve(r), e => e !== MUTEX_ERROR && reject(e));
});

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
    this.credentials = options.credentials;
    this.transport = options.transport || new HttpTransport({
      authorization: typeof apiToken === 'function' ? undefined : apiToken,
      apiUrl: this.apiUrl,
      headers: this.headers,
      credentials: this.credentials
    });
    this.pollInterval = options.pollInterval || 5;
    this.parseDateMeasures = options.parseDateMeasures;
  }

  request(method, params) {
    return this.transport.request(method, { baseRequestId: uuid(), ...params });
  }

  /**
   * Base method used to perform all API calls.
   * Shouldn't be used directly.
   * @param request - function that invoked to perform actual request using `transport.request()` method.
   * @param toResult - function that maps results of invocation to method return result
   * @param [options] - options object
   * @param options.mutexObj - object to use to store MUTEX
   * @param [options.mutexKey='default'] - key to use to store current request MUTEX inside `mutexObj`.
   * MUTEX object is used to reject orphaned queries results when new queries are sent.
   * For example if two queries are sent with same `mutexKey` only last one will return results.
   * @param options.subscribe - pass `true` to use continuous fetch behavior.
   * @param {Function} options.progressCallback - function that receives `ProgressResult` on each
   * `Continue wait` message.
   * @param [callback] - if passed `callback` function will be called instead of `Promise` returned
   * @return {{unsubscribe: function()}}
   */
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
        
        const error = new RequestError(body.error, body); // TODO error class
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

  load(query, options, callback) {
    return this.loadMethod(
      () => this.request('load', { query }),
      (body) => new ResultSet(body, { parseDateMeasures: this.parseDateMeasures }),
      options,
      callback
    );
  }

  sql(query, options, callback) {
    return this.loadMethod(
      () => this.request('sql', { query }),
      (body) => new SqlQuery(body),
      options,
      callback
    );
  }

  meta(options, callback) {
    return this.loadMethod(
      () => this.request('meta'),
      (body) => new Meta(body),
      options,
      callback
    );
  }

  subscribe(query, options, callback) {
    return this.loadMethod(
      () => this.request('subscribe', { query }),
      (body) => new ResultSet(body, { parseDateMeasures: this.parseDateMeasures }),
      { ...options, subscribe: true },
      callback
    );
  }
}

export default (apiToken, options) => new CubejsApi(apiToken, options);

export { HttpTransport, ResultSet };
