import 'isomorphic-fetch';
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
  })
};

class CubejsApi {
  constructor(apiToken, options) {
    options = options || {};
    this.apiToken = apiToken;
    this.apiUrl = options.apiUrl || API_URL;
  }

  request(url, config) {
    // eslint-disable-next-line no-undef
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

export default (apiToken, options) => {
  return new CubejsApi(apiToken, options);
};