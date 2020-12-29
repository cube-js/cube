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
    
    this.method = this.method || (url.length < 2000 ? 'GET' : 'POST');
    if (this.method === 'POST') {
      url = `${this.apiUrl}/${method}`;
      this.headers['Content-Type'] = 'application/json';
    }

    // Currently, all methods make GET requests. If a method makes a request with a body payload,
    // remember to add {'Content-Type': 'application/json'} to the header.
    const runRequest = () => fetch(url, {
      method: this.method,
      headers: {
        Authorization: this.authorization,
        'x-request-id': baseRequestId && `${baseRequestId}-span-${spanCounter++}`,
        ...this.headers
      },
      credentials: this.credentials,
      body: this.method === 'POST' ? JSON.stringify(params) : null
    });

    return {
      async subscribe(callback) {
        const result = await runRequest();
        return callback(result, () => this.subscribe(callback));
      }
    };
  }
}

export default HttpTransport;
