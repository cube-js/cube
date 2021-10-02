import fetch from 'cross-fetch';
import 'url-search-params-polyfill';

class HttpTransport {
  constructor({ authorization, apiUrl, method, headers = {}, credentials }) {
    this.authorization = authorization;
    this.apiUrl = apiUrl;
    this.method = method;
    this.headers = headers;
    this.credentials = credentials;
  }

  request(method, { baseRequestId, ...params }) {
    let spanCounter = 1;
    const searchParams = new URLSearchParams(
      params && Object.keys(params)
        .map(k => ({ [k]: typeof params[k] === 'object' ? JSON.stringify(params[k]) : params[k] }))
        .reduce((a, b) => ({ ...a, ...b }), {})
    );

    let url = `${this.apiUrl}/${method}${searchParams.toString().length ? `?${searchParams}` : ''}`;

    const requestMethod = this.method || (url.length < 2000 ? 'GET' : 'POST');
    if (requestMethod === 'POST') {
      url = `${this.apiUrl}/${method}`;
      this.headers['Content-Type'] = 'application/json';
    }

    // Currently, all methods make GET requests. If a method makes a request with a body payload,
    // remember to add {'Content-Type': 'application/json'} to the header.
    const runRequest = () => fetch(url, {
      method: requestMethod,
      headers: {
        Authorization: this.authorization,
        'x-request-id': baseRequestId && `${baseRequestId}-span-${spanCounter++}`,
        ...this.headers
      },
      credentials: this.credentials,
      body: requestMethod === 'POST' ? JSON.stringify(params) : null
    });

    return {
      /* eslint no-unsafe-finally: off */
      async subscribe(callback) {
        let result = {
          error: 'network Error' // add default error message
        };
        try {
          result = await runRequest();
        } finally {
          return callback(result, () => this.subscribe(callback));
        }
      }
    };
  }
}

export default HttpTransport;
