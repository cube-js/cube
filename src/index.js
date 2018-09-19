import { fetch } from 'whatwg-fetch';
import ResultSet from './ResultSet';
import ProgressResult from './ProgressResult';

const API_URL = process.env.CUBEJS_API_URL;

class CubejsApi {
  constructor(apiToken) {
    this.apiToken = apiToken;
  }

  request(url, config) {
    return fetch(
      `${API_URL}${url}`,
      Object.assign({ headers: { Authorization: this.apiToken, 'Content-Type': 'application/json' }}, config || {})
    )
  }

  load(query, options, callback) {
    if (typeof options === 'function' && !callback) {
      callback = options;
      options = undefined;
    }

    const loadImpl = async () => {
      const response = await this.request(`/load?query=${JSON.stringify(query)}`);
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
      return new ResultSet(body);
    };
    if (callback) {
      loadImpl().then(r => callback(null, r), e => callback(e));
    } else {
      return loadImpl();
    }
  }
}

export default (apiToken) => {
  return new CubejsApi(apiToken);
};