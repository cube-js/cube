/**
 * @module @cubejs-client/core
 */

import fetch from 'cross-fetch';
import 'url-search-params-polyfill';

/**
 * Default transport implementation.
 */
class HttpTransport {
  /**
   * @param options - mandatory options object
   * @param options.authorization - [jwt auth token](security)
   * @param options.apiUrl - path to `/cubejs-api/v1`
   * @param [options.headers] - object of custom headers
   */
  constructor({ authorization, apiUrl, headers = {} }) {
    this.authorization = authorization;
    this.apiUrl = apiUrl;
    this.headers = headers;
  }

  request(method, { baseRequestId, ...params }) {
    const searchParams = new URLSearchParams(
      params && Object.keys(params)
        .map(k => ({ [k]: typeof params[k] === 'object' ? JSON.stringify(params[k]) : params[k] }))
        .reduce((a, b) => ({ ...a, ...b }), {})
    );

    let spanCounter = 1;

    const runRequest = () => fetch(
      `${this.apiUrl}/${method}${searchParams.toString().length ? `?${searchParams}` : ''}`, {
        headers: {
          Authorization: this.authorization,
          'Content-Type': 'application/json',
          'x-request-id': baseRequestId && `${baseRequestId}-span-${spanCounter++}`,
          ...this.headers
        }
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
