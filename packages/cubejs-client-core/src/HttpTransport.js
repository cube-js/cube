import fetch from 'cross-fetch';
import 'url-search-params-polyfill';

class HttpTransport {
  constructor({ authorization, apiUrl, headers = {} }) {
    this.authorization = authorization;
    this.apiUrl = apiUrl;
    this.headers = headers
  }

  request(method, params) {
    const searchParams = new URLSearchParams(
      params && Object.keys(params)
        .map(k => ({ [k]: typeof params[k] === 'object' ? JSON.stringify(params[k]) : params[k] }))
        .reduce((a, b) => ({ ...a, ...b }), {})
    );

    const runRequest = () => fetch(
      `${this.apiUrl}/${method}${searchParams.toString().length ? `?${searchParams}` : ''}`, {
        headers: Object.assign({ Authorization: this.authorization, 'Content-Type': 'application/json' }, this.headers)
      }
    );

    return {
      async subscribe(callback) {
        const result = await runRequest();
        return callback(result, () => this.subscribe(callback));
      }
    };
  }
}

export default HttpTransport;
