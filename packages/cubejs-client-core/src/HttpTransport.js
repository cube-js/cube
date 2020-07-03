import fetch from 'cross-fetch';
import 'url-search-params-polyfill';

class HttpTransport {
  constructor({ authorization, apiUrl, headers = {}, credentials }) {
    this.authorization = authorization;
    this.apiUrl = apiUrl;
    this.headers = headers;
    this.credentials = credentials;
  }

  request(method, { baseRequestId, ...params }) {
    const searchParams = new URLSearchParams(
      params && Object.keys(params)
        .map(k => ({ [k]: typeof params[k] === 'object' ? JSON.stringify(params[k]) : params[k] }))
        .reduce((a, b) => ({ ...a, ...b }), {})
    );

    let spanCounter = 1;

    // Currently, all methods make GET requests. If a method makes a request with a body payload,
    // remember to add a 'Content-Type' header.
    const runRequest = () => fetch(
      `${this.apiUrl}/${method}${searchParams.toString().length ? `?${searchParams}` : ''}`, {
        headers: {
          Authorization: this.authorization,
          'x-request-id': baseRequestId && `${baseRequestId}-span-${spanCounter++}`,
          ...this.headers
        },
        credentials: this.credentials
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
