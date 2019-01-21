import { fetch } from 'whatwg-fetch';
import ResultSet from './ResultSet';
import SqlQuery from './SqlQuery';
import ProgressResult from './ProgressResult';

const API_URL = process.env.CUBEJS_API_URL;

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
    )
  }

  loadMethod(request, toResult, options, callback) {
    if (typeof options === 'function' && !callback) {
      callback = options;
      options = undefined;
    }

    options = options || {};


    const loadImpl = async () => {
      const response = await request();
      if (response.status === 502) {
        return loadImpl(); // TODO backoff wait
      }
      const body = await response.json();
      if (body.error === 'Continue wait') {
        if (options.progressCallback) {
          options.progressCallback(new ProgressResult(body));
        }
        return loadImpl();
      }
      if (response.status !== 200) {
        throw new Error(body.error); // TODO error class
      }
      return toResult(body);
    };
    if (callback) {
      loadImpl().then(r => callback(null, r), e => callback(e));
    } else {
      return loadImpl();
    }
  }

  load(query, options, callback) {
    return this.loadMethod(
      () => this.request(`/load?query=${JSON.stringify(query)}`),
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
}

export default (apiToken) => {
  return new CubejsApi(apiToken);
};