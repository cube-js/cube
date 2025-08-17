import fetch from 'cross-fetch';
import 'url-search-params-polyfill';
import { responseChunks } from './streaming';

export interface ErrorResponse {
  error: string;
}

export type TransportOptions = {
  /**
   * [jwt auth token](security)
   */
  authorization?: string;
  /**
   * path to `/cubejs-api/v1`
   */
  apiUrl: string;
  /**
   * custom headers
   */
  headers: Record<string, string>;
  credentials?: 'omit' | 'same-origin' | 'include';
  method?: 'GET' | 'PUT' | 'POST' | 'PATCH';
  /**
   * Fetch timeout in milliseconds. Would be passed as AbortSignal.timeout()
   */
  fetchTimeout?: number;
  /**
   * AbortSignal to cancel requests
   */
  signal?: AbortSignal;
};

export interface ITransportResponse<R> {
  subscribe: <CBResult>(
    cb: (
      result: R | ErrorResponse,
      resubscribe: () => Promise<CBResult>
    ) => CBResult
  ) => Promise<CBResult>;
  // Optional, supported in WebSocketTransport
  unsubscribe?: () => Promise<void>;
}

export interface ITransportStreamResponse {
  stream(): Promise<AsyncIterable<Uint8Array>>;
  unsubscribe?: () => Promise<void>;
}

export interface ITransportStreamParams<T extends Record<string, unknown> = Record<string, unknown>> {
  method?: 'GET' | 'POST' | 'PUT' | 'PATCH';
  fetchTimeout?: number;
  baseRequestId?: string;
  signal?: AbortSignal;
  params?: T;
}

export interface ITransport<R> {
  request(
    method: string,
    params: Record<string, unknown>
  ): ITransportResponse<R>;
  requestStream?<T extends Record<string, unknown> = Record<string, unknown>>(
    method: string,
    params: ITransportStreamParams<T>
  ): ITransportStreamResponse;
  authorization: TransportOptions['authorization'];
}

/**
 * Default transport implementation.
 */
export class HttpTransport implements ITransport<Response> {
  public authorization: TransportOptions['authorization'];

  protected apiUrl: TransportOptions['apiUrl'];

  protected method: TransportOptions['method'];

  protected headers: TransportOptions['headers'];

  protected credentials: TransportOptions['credentials'];

  protected fetchTimeout: number | undefined;

  private readonly signal: AbortSignal | undefined;

  public constructor({
    authorization,
    apiUrl,
    method,
    headers = {},
    credentials,
    fetchTimeout,
    signal,
  }: Omit<TransportOptions, 'headers'> & {
    headers?: TransportOptions['headers'];
  }) {
    this.authorization = authorization;
    this.apiUrl = apiUrl;
    this.method = method;
    this.headers = headers;
    this.credentials = credentials;
    this.fetchTimeout = fetchTimeout;
    this.signal = signal;
  }

  public request(
    apiMethod: string,
    { method, fetchTimeout, baseRequestId, signal, ...params }: any
  ): ITransportResponse<Response> {
    let spanCounter = 1;
    const searchParams = new URLSearchParams(
      params &&
        Object.keys(params)
          .map((k) => ({
            [k]:
              typeof params[k] === 'object'
                ? JSON.stringify(params[k])
                : params[k],
          }))
          .reduce((a, b) => ({ ...a, ...b }), {})
    );

    let url = `${this.apiUrl}/${apiMethod}${
      searchParams.toString().length ? `?${searchParams}` : ''
    }`;

    const requestMethod =
      method ?? this.method ?? (url.length < 2000 ? 'GET' : 'POST');
    if (requestMethod === 'POST') {
      url = `${this.apiUrl}/${apiMethod}`;
      this.headers['Content-Type'] = 'application/json';
    }

    const effectiveFetchTimeout = fetchTimeout ?? this.fetchTimeout;
    const actualSignal =
      signal ||
      this.signal ||
      (effectiveFetchTimeout
        ? AbortSignal.timeout(effectiveFetchTimeout)
        : undefined);

    // Currently, all methods make GET requests. If a method makes a request with a body payload,
    // remember to add {'Content-Type': 'application/json'} to the header.
    const runRequest = () => fetch(url, {
      method: requestMethod,
      headers: {
        Authorization: this.authorization,
        'x-request-id':
          baseRequestId && `${baseRequestId}-span-${spanCounter++}`,
        ...this.headers,
      } as HeadersInit,
      credentials: this.credentials,
      body: requestMethod === 'POST' ? JSON.stringify(params) : null,
      signal: actualSignal,
    });

    return {
      /* eslint no-unsafe-finally: off */
      async subscribe(callback) {
        try {
          const result = await runRequest();
          return callback(result, () => this.subscribe(callback));
        } catch (e: any) {
          let errorMessage = 'network Error';

          if (e.name === 'AbortError') {
            if (
              actualSignal?.reason === 'TimeoutError' ||
              actualSignal?.reason?.name === 'TimeoutError'
            ) {
              errorMessage = 'timeout';
            } else {
              errorMessage = 'aborted';
            }
          }

          const result: ErrorResponse = { error: errorMessage };
          return callback(result, () => this.subscribe(callback));
        }
      },
    };
  }

  public requestStream<T extends Record<string, unknown> = Record<string, unknown>>(
    apiMethod: string,
    { method, fetchTimeout, baseRequestId, signal, params }: ITransportStreamParams<T>
  ): ITransportStreamResponse {
    const processedParams: Record<string, string> = {};

    // Handle the generic params object
    if (params) {
      Object.keys(params).forEach((k) => {
        const value = params[k];
        if (value !== undefined) {
          processedParams[k] = typeof value === 'object' ? JSON.stringify(value) : String(value);
        }
      });
    }

    const searchParams = new URLSearchParams(processedParams);

    let url = `${this.apiUrl}/${apiMethod}${
      searchParams.toString().length ? `?${searchParams}` : ''
    }`;

    const requestMethod = method ?? this.method ?? 'POST';
    if (requestMethod === 'POST') {
      url = `${this.apiUrl}/${apiMethod}`;
      this.headers['Content-Type'] = 'application/json';
    }

    const effectiveFetchTimeout = fetchTimeout ?? this.fetchTimeout;

    let controller: AbortController | undefined;
    let actualSignal: AbortSignal | undefined = signal || this.signal;

    if (!actualSignal && effectiveFetchTimeout) {
      controller = new AbortController();
      actualSignal = controller.signal;
      setTimeout(() => controller?.abort(), effectiveFetchTimeout);
    }

    return {
      stream: async () => {
        const response = await fetch(url, {
          method: requestMethod,
          headers: {
            Authorization: this.authorization,
            'x-request-id': baseRequestId || 'stream-request',
            ...this.headers,
          } as HeadersInit,
          credentials: this.credentials,
          body: requestMethod === 'POST' ? JSON.stringify(params || {}) : null,
          signal: actualSignal,
        });

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        if (!response.body) {
          throw new Error('No response body available for streaming');
        }

        return responseChunks(response);
      },
      unsubscribe: async () => {
        if (controller) {
          controller.abort();
        }
      },
    };
  }
}

export default HttpTransport;
