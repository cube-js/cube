import fetch from 'cross-fetch';
import 'url-search-params-polyfill';

class HttpTransport {
  constructor({ authorization, apiUrl }) {
    this.authorization = authorization;
    this.apiUrl = apiUrl;
  }

  request(method, params) {
    const searchParams = new URLSearchParams(params);

    const runRequest = () => fetch(
      `${this.apiUrl}${method}?${searchParams}`, {
        headers: { Authorization: this.authorization, 'Content-Type': 'application/json' }
      }
    );

    return {
      async subscribe(callback) {
        const result = await runRequest();
        return callback(result, () => this.subscribe(callback));
      },
      async unsubscribe() {
        return null;
      }
    };
  }
}

export default HttpTransport;
