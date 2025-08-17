import { v4 as uuidv4 } from 'uuid';
import ResultSet from './ResultSet';
import SqlQuery from './SqlQuery';
import Meta from './Meta';
import ProgressResult from './ProgressResult';
import HttpTransport, { ErrorResponse, ITransport, TransportOptions } from './HttpTransport';
import RequestError from './RequestError';
import {
  ExtractTimeMembers,
  LoadResponse,
  MetaResponse,
  PivotQuery,
  ProgressResponse,
  Query,
  QueryOrder,
  QueryType,
  TransformedQuery
} from './types';

export type LoadMethodCallback<T> = (error: Error | null, resultSet: T) => void;

export type LoadMethodOptions = {
  /**
   * Key to store the current request's MUTEX inside the `mutexObj`. MUTEX object is used to reject orphaned queries results when new queries are sent. For example: if two queries are sent with the same `mutexKey` only the last one will return results.
   */
  mutexKey?: string;
  /**
   * Object to store MUTEX
   */
  mutexObj?: { [key: string]: any };
  /**
   * Pass `true` to use continuous fetch behavior.
   */
  subscribe?: boolean;
  /**
   * A Cube API instance. If not provided will be taken from `CubeProvider`
   */
  // eslint-disable-next-line no-use-before-define
  cubeApi?: CubeApi;
  /**
   * If enabled, all members of the 'number' type will be automatically converted to numerical values on the client side
   */
  castNumerics?: boolean;
  /**
   * Function that receives `ProgressResult` on each `Continue wait` message.
   */
  progressCallback?(result: ProgressResult): void;
  /**
   * AbortSignal to cancel requests
   */
  signal?: AbortSignal;
};

export type DeeplyReadonly<T> = {
  readonly [K in keyof T]: DeeplyReadonly<T[K]>;
};

export type ExtractMembers<T extends DeeplyReadonly<Query>> =
  | (T extends { dimensions: readonly (infer Names)[]; } ? Names : never)
  | (T extends { measures: readonly (infer Names)[]; } ? Names : never)
  | (T extends { timeDimensions: (infer U); } ? ExtractTimeMembers<U> : never);

// If we can't infer any members at all, then return any.
export type SingleQueryRecordType<T extends DeeplyReadonly<Query>> = ExtractMembers<T> extends never
  ? any
  : { [K in string & ExtractMembers<T>]: string | number | boolean | null };

export type QueryArrayRecordType<T extends DeeplyReadonly<Query[]>> =
  T extends readonly [infer First, ...infer Rest]
    ? SingleQueryRecordType<DeeplyReadonly<First>> | QueryArrayRecordType<Rest & DeeplyReadonly<Query[]>>
    : never;

export type QueryRecordType<T extends DeeplyReadonly<Query | Query[]>> =
  T extends DeeplyReadonly<Query[]> ? QueryArrayRecordType<T> :
    T extends DeeplyReadonly<Query> ? SingleQueryRecordType<T> :
      never;

export interface UnsubscribeObj {
  /**
   * Allows to stop requests in-flight in long polling or web socket subscribe loops.
   * It doesn't cancel any submitted requests to the underlying databases.
   */
  unsubscribe(): Promise<void>;
}

/**
 * @deprecated use DryRunResponse
 */
export type TDryRunResponse = {
  queryType: QueryType;
  normalizedQueries: Query[];
  pivotQuery: PivotQuery;
  queryOrder: Array<{ [k: string]: QueryOrder }>;
  transformedQueries: TransformedQuery[];
};

export type DryRunResponse = {
  queryType: QueryType;
  normalizedQueries: Query[];
  pivotQuery: PivotQuery;
  queryOrder: Array<{ [k: string]: QueryOrder }>;
  transformedQueries: TransformedQuery[];
};

export type CubeSqlOptions = LoadMethodOptions & {
  /**
   * Query timeout in milliseconds
   */
  timeout?: number;
};

export type CubeSqlSchemaColumn = {
  name: string;
  columnType: string;
};

export type CubeSqlResult = {
  schema: CubeSqlSchemaColumn[];
  data: (string | number | boolean | null)[][];
};

export type CubeSqlStreamChunk = {
  type: 'schema';
  schema: CubeSqlSchemaColumn[];
} | {
  type: 'data';
  data: (string | number | boolean | null)[];
} | {
  type: 'error';
  error: string;
};

interface BodyResponse {
  error?: string;
  [key: string]: any;
}

let mutexCounter = 0;

const MUTEX_ERROR = 'Mutex has been changed';

function mutexPromise(promise: Promise<any>) {
  return promise
    .then((result) => result)
    .catch((error) => {
      if (error !== MUTEX_ERROR) {
        throw error;
      }
    });
}

export type ResponseFormat = 'compact' | 'default' | undefined;

export type CubeApiOptions = {
  /**
   * URL of your Cube.js Backend. By default, in the development environment it is `http://localhost:4000/cubejs-api/v1`
   */
  apiUrl: string;
  /**
   * Transport implementation to use. [HttpTransport](#http-transport) will be used by default.
   */
  transport?: ITransport<any>;
  method?: TransportOptions['method'];
  headers?: TransportOptions['headers'];
  pollInterval?: number;
  credentials?: TransportOptions['credentials'];
  parseDateMeasures?: boolean;
  resType?: 'default' | 'compact';
  castNumerics?: boolean;
  /**
   * How many network errors would be retried before returning to users. Default to 0.
   */
  networkErrorRetries?: number;
  /**
   * AbortSignal to cancel requests
   */
  signal?: AbortSignal;
  /**
   * Fetch timeout in milliseconds. Would be passed as AbortSignal.timeout()
   */
  fetchTimeout?: number;
};

/**
 * Main class for accessing Cube API
 */
class CubeApi {
  private readonly apiToken: string | (() => Promise<string>) | (CubeApiOptions & any[]) | undefined;

  private readonly apiUrl: string;

  private readonly method: TransportOptions['method'];

  private readonly headers: TransportOptions['headers'];

  private readonly credentials: TransportOptions['credentials'];

  protected readonly transport: ITransport<any>;

  private readonly pollInterval: number;

  private readonly parseDateMeasures: boolean | undefined;

  private readonly castNumerics: boolean;

  private readonly networkErrorRetries: number;

  private updateAuthorizationPromise: Promise<any> | null;

  public constructor(apiToken: string | (() => Promise<string>) | undefined, options: CubeApiOptions);

  public constructor(options: CubeApiOptions);

  /**
   * Creates an instance of the `CubeApi`. The API entry point.
   *
   * ```js
   * import cube from '@cubejs-client/core';
   * const cubeApi = cube(
   *   'CUBE-API-TOKEN',
   *   { apiUrl: 'http://localhost:4000/cubejs-api/v1' }
   * );
   * ```
   *
   * You can also pass an async function or a promise that will resolve to the API token
   *
   * ```js
   * import cube from '@cubejs-client/core';
   * const cubeApi = cube(
   *   async () => await Auth.getJwtToken(),
   *   { apiUrl: 'http://localhost:4000/cubejs-api/v1' }
   * );
   * ```
   */
  public constructor(
    apiToken: string | (() => Promise<string>) | undefined | CubeApiOptions,
    options?: CubeApiOptions
  ) {
    if (apiToken && !Array.isArray(apiToken) && typeof apiToken === 'object') {
      options = apiToken;
      apiToken = undefined;
    }

    if (!options || (!options.transport && !options.apiUrl)) {
      throw new Error('The `apiUrl` option is required');
    }

    this.apiToken = apiToken;
    this.apiUrl = options.apiUrl;
    this.method = options.method;
    this.headers = options.headers || {};
    this.credentials = options.credentials;

    this.transport = options.transport || new HttpTransport({
      authorization: typeof apiToken === 'string' ? apiToken : undefined,
      apiUrl: this.apiUrl,
      method: this.method,
      headers: this.headers,
      credentials: this.credentials,
      fetchTimeout: options.fetchTimeout,
      signal: options.signal
    });

    this.pollInterval = options.pollInterval || 5;
    this.parseDateMeasures = options.parseDateMeasures;
    this.castNumerics = typeof options.castNumerics === 'boolean' ? options.castNumerics : false;
    this.networkErrorRetries = options.networkErrorRetries || 0;

    this.updateAuthorizationPromise = null;
  }

  protected request(method: string, params?: any) {
    return this.transport.request(method, {
      baseRequestId: uuidv4(),
      ...params
    });
  }

  private loadMethod(request: CallableFunction, toResult: CallableFunction, options?: LoadMethodOptions, callback?: CallableFunction) {
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

    const requestPromise = this
      .updateTransportAuthorization()
      .then(() => request());

    let skipAuthorizationUpdate = true;
    let unsubscribed = false;

    const checkMutex = async () => {
      const requestInstance = await requestPromise;

      if (options &&
        options.mutexObj &&
          options.mutexObj[mutexKey] !== mutexValue
      ) {
        unsubscribed = true;
        if (requestInstance.unsubscribe) {
          await requestInstance.unsubscribe();
        }
        throw MUTEX_ERROR;
      }
    };

    let networkRetries = this.networkErrorRetries;

    const loadImpl = async (response: Response | ErrorResponse, next: CallableFunction) => {
      const requestInstance = await requestPromise;

      const subscribeNext = async () => {
        if (options?.subscribe && !unsubscribed) {
          if (requestInstance.unsubscribe) {
            return next();
          } else {
            await new Promise<void>(resolve => setTimeout(() => resolve(), this.pollInterval * 1000));
            return next();
          }
        }
        return null;
      };

      const continueWait = async (wait: boolean = false) => {
        if (!unsubscribed) {
          if (wait) {
            await new Promise<void>(resolve => setTimeout(() => resolve(), this.pollInterval * 1000));
          }
          return next();
        }
        return null;
      };

      if (options?.subscribe && !skipAuthorizationUpdate) {
        await this.updateTransportAuthorization();
      }

      skipAuthorizationUpdate = false;

      if (('status' in response && response.status === 502) ||
        ('error' in response && response.error?.toLowerCase() === 'network error') &&
        --networkRetries >= 0
      ) {
        await checkMutex();
        return continueWait(true);
      }

      // From here we're sure that response is only fetch Response
      response = (response as Response);
      let body: BodyResponse = {};
      let text = '';
      try {
        text = await response.text();
        body = JSON.parse(text);
      } catch (_) {
        body.error = text;
      }

      if (body.error === 'Continue wait') {
        await checkMutex();
        if (options?.progressCallback) {
          options.progressCallback(new ProgressResult(body as ProgressResponse));
        }
        return continueWait();
      }

      if (response.status !== 200) {
        await checkMutex();
        if (!options?.subscribe && requestInstance.unsubscribe) {
          await requestInstance.unsubscribe();
        }

        const error = new RequestError(body.error || (response as any).error || '', body, response.status);
        if (callback) {
          callback(error);
        } else {
          throw error;
        }

        return subscribeNext();
      }
      await checkMutex();
      if (!options?.subscribe && requestInstance.unsubscribe) {
        await requestInstance.unsubscribe();
      }
      const result = toResult(body);
      if (callback) {
        callback(null, result);
      } else {
        return result;
      }

      return subscribeNext();
    };

    const promise = requestPromise.then(requestInstance => mutexPromise(requestInstance.subscribe(loadImpl)));

    if (callback) {
      return {
        unsubscribe: async () => {
          const requestInstance = await requestPromise;

          unsubscribed = true;
          if (requestInstance.unsubscribe) {
            return requestInstance.unsubscribe();
          }
          return null;
        }
      };
    } else {
      return promise;
    }
  }

  private async updateTransportAuthorization() {
    if (this.updateAuthorizationPromise) {
      await this.updateAuthorizationPromise;
      return;
    }

    const tokenFetcher = this.apiToken;

    if (typeof tokenFetcher === 'function') {
      const promise = (async () => {
        try {
          const token = await tokenFetcher();

          if (this.transport.authorization !== token) {
            this.transport.authorization = token;
          }
        } finally {
          this.updateAuthorizationPromise = null;
        }
      })();

      this.updateAuthorizationPromise = promise;
      await promise;
    }
  }

  /**
   * Add system properties to a query object.
   */
  private patchQueryInternal(query: DeeplyReadonly<Query>, responseFormat: ResponseFormat): DeeplyReadonly<Query> {
    if (
      responseFormat === 'compact' &&
      query.responseFormat !== 'compact'
    ) {
      return {
        ...query,
        responseFormat: 'compact',
      };
    } else {
      return query;
    }
  }

  /**
   * Process result fetched from the gateway#load method according
   * to the network protocol.
   */
  protected loadResponseInternal(response: LoadResponse<any>, options: LoadMethodOptions | null = {}): ResultSet<any> {
    if (
      response.results.length
    ) {
      if (options?.castNumerics) {
        response.results.forEach((result) => {
          const numericMembers = Object.entries({
            ...result.annotation.measures,
            ...result.annotation.dimensions,
          }).reduce<string[]>((acc, [k, v]) => {
            if (v.type === 'number') {
              acc.push(k);
            }
            return acc;
          }, []);

          result.data = result.data.map((row) => {
            numericMembers.forEach((key) => {
              if (row[key] != null) {
                row[key] = Number(row[key]);
              }
            });

            return row;
          });
        });
      }

      if (response.results[0].query.responseFormat &&
        response.results[0].query.responseFormat === 'compact') {
        response.results.forEach((result, j) => {
          const data: Record<string, any>[] = [];
          const { dataset, members } = result.data as unknown as { dataset: any[]; members: string[] };
          dataset.forEach((r) => {
            const row: Record<string, any> = {};
            members.forEach((m, i) => {
              row[m] = r[i];
            });
            data.push(row);
          });
          response.results[j].data = data;
        });
      }
    }

    return new ResultSet(response, {
      parseDateMeasures: this.parseDateMeasures
    });
  }

  public load<QueryType extends DeeplyReadonly<Query | Query[]>>(
    query: QueryType,
    options?: LoadMethodOptions,
  ): Promise<ResultSet<QueryRecordType<QueryType>>>;

  public load<QueryType extends DeeplyReadonly<Query | Query[]>>(
    query: QueryType,
    options?: LoadMethodOptions,
    callback?: LoadMethodCallback<ResultSet<QueryRecordType<QueryType>>>,
  ): UnsubscribeObj;

  public load<QueryType extends DeeplyReadonly<Query | Query[]>>(
    query: QueryType,
    options?: LoadMethodOptions,
    callback?: LoadMethodCallback<ResultSet<any>>,
    responseFormat?: string
  ): Promise<ResultSet<QueryRecordType<QueryType>>>;

  /**
   * Fetch data for the passed `query`.
   *
   * ```js
   * import cube from '@cubejs-client/core';
   * import Chart from 'chart.js';
   * import chartjsConfig from './toChartjsData';
   *
   * const cubeApi = cube('CUBEJS_TOKEN');
   *
   * const resultSet = await cubeApi.load({
   *  measures: ['Stories.count'],
   *  timeDimensions: [{
   *    dimension: 'Stories.time',
   *    dateRange: ['2015-01-01', '2015-12-31'],
   *    granularity: 'month'
   *   }]
   * });
   *
   * const context = document.getElementById('myChart');
   * new Chart(context, chartjsConfig(resultSet));
   * ```
   * @param query - [Query object](/product/apis-integrations/rest-api/query-format)
   * @param options
   * @param callback
   * @param responseFormat
   */
  public load<QueryType extends DeeplyReadonly<Query | Query[]>>(query: QueryType, options?: LoadMethodOptions, callback?: CallableFunction, responseFormat: ResponseFormat = 'default') {
    [query, options] = this.prepareQueryOptions(query, options, responseFormat);
    return this.loadMethod(
      () => this.request('load', {
        query,
        queryType: 'multi',
        signal: options?.signal
      }),
      (response: any) => this.loadResponseInternal(response, options),
      options,
      callback
    );
  }

  private prepareQueryOptions<QueryType extends DeeplyReadonly<Query | Query[]>>(query: QueryType, options?: LoadMethodOptions | null, responseFormat: ResponseFormat = 'default'): [query: QueryType, options: LoadMethodOptions] {
    options = {
      castNumerics: this.castNumerics,
      ...options
    };

    if (responseFormat === 'compact') {
      if (Array.isArray(query)) {
        const patched = query.map((q) => this.patchQueryInternal(q, 'compact'));
        return [patched as unknown as QueryType, options];
      } else {
        const patched = this.patchQueryInternal(query as DeeplyReadonly<Query>, 'compact');
        return [patched as QueryType, options];
      }
    }

    return [query, options];
  }

  /**
   * Allows you to fetch data and receive updates over time. See [Real-Time Data Fetch](/product/apis-integrations/rest-api/real-time-data-fetch)
   *
   * ```js
   * // Subscribe to a query's updates
   * const subscription = await cubeApi.subscribe(
   *   {
   *     measures: ['Logs.count'],
   *     timeDimensions: [
   *       {
   *         dimension: 'Logs.time',
   *         granularity: 'hour',
   *         dateRange: 'last 1440 minutes',
   *       },
   *     ],
   *   },
   *   options,
   *   (error, resultSet) => {
   *     if (!error) {
   *       // handle the update
   *     }
   *   }
   * );
   *
   * // Unsubscribe from a query's updates
   * subscription.unsubscribe();
   * ```
   */
  public subscribe<QueryType extends DeeplyReadonly<Query | Query[]>>(
    query: QueryType,
    options: LoadMethodOptions | null,
    callback: LoadMethodCallback<ResultSet<QueryRecordType<QueryType>>>,
    responseFormat: ResponseFormat = 'default'
  ): UnsubscribeObj {
    [query, options] = this.prepareQueryOptions(query, options, responseFormat);
    return this.loadMethod(
      () => this.request('subscribe', {
        query,
        queryType: 'multi',
        signal: options?.signal
      }),
      (response: any) => this.loadResponseInternal(response, options),
      { ...options, subscribe: true },
      callback
    ) as UnsubscribeObj;
  }

  public sql(query: DeeplyReadonly<Query | Query[]>, options?: LoadMethodOptions): Promise<SqlQuery>;

  public sql(query: DeeplyReadonly<Query | Query[]>, options?: LoadMethodOptions, callback?: LoadMethodCallback<SqlQuery>): UnsubscribeObj;

  /**
   * Get generated SQL string for the given `query`.
   */
  public sql(query: DeeplyReadonly<Query | Query[]>, options?: LoadMethodOptions, callback?: LoadMethodCallback<SqlQuery>): Promise<SqlQuery> | UnsubscribeObj {
    return this.loadMethod(
      () => this.request('sql', {
        query,
        signal: options?.signal
      }),
      (response: any) => (Array.isArray(response) ? response.map((body) => new SqlQuery(body)) : new SqlQuery(response)),
      options,
      callback
    );
  }

  public meta(options?: LoadMethodOptions): Promise<Meta>;

  public meta(options?: LoadMethodOptions, callback?: LoadMethodCallback<Meta>): UnsubscribeObj;

  /**
   * Get meta description of cubes available for querying.
   */
  public meta(options?: LoadMethodOptions, callback?: LoadMethodCallback<Meta>): Promise<Meta> | UnsubscribeObj {
    return this.loadMethod(
      () => this.request('meta', {
        signal: options?.signal
      }),
      (body: MetaResponse) => new Meta(body),
      options,
      callback
    );
  }

  public dryRun(query: DeeplyReadonly<Query | Query[]>, options?: LoadMethodOptions): Promise<DryRunResponse>;

  public dryRun(query: DeeplyReadonly<Query | Query[]>, options: LoadMethodOptions, callback?: LoadMethodCallback<DryRunResponse>): UnsubscribeObj;

  /**
   * Get query related meta without query execution
   */
  public dryRun(query: DeeplyReadonly<Query | Query[]>, options?: LoadMethodOptions, callback?: LoadMethodCallback<DryRunResponse>): Promise<DryRunResponse> | UnsubscribeObj {
    return this.loadMethod(
      () => this.request('dry-run', {
        query,
        signal: options?.signal
      }),
      (response: DryRunResponse) => response,
      options,
      callback
    );
  }

  public cubeSql(sqlQuery: string, options?: CubeSqlOptions): Promise<CubeSqlResult>;

  public cubeSql(sqlQuery: string, options?: CubeSqlOptions, callback?: LoadMethodCallback<CubeSqlResult>): UnsubscribeObj;

  /**
   * Execute a Cube SQL query against Cube SQL interface and return the results.
   */
  public cubeSql(sqlQuery: string, options?: CubeSqlOptions, callback?: LoadMethodCallback<CubeSqlResult>): Promise<CubeSqlResult> | UnsubscribeObj {
    return this.loadMethod(
      () => {
        const request = this.request('cubesql', {
          query: sqlQuery,
          method: 'POST',
          signal: options?.signal,
          fetchTimeout: options?.timeout
        });

        return request;
      },
      (response: any) => {
        // TODO: The response is sending both errors and successful results as `error`
        if (!response || !response.error) {
          throw new Error('Invalid response format');
        }

        // Check if this is a timeout or abort error from transport
        if (response.error === 'timeout') {
          const timeoutMs = options?.timeout || 5 * 60 * 1000;
          throw new Error(`CubeSQL query timed out after ${timeoutMs}ms`);
        }

        if (response.error === 'aborted') {
          throw new Error('CubeSQL query was aborted');
        }

        const [schema, ...data] = response.error.split('\n');

        try {
          return {
            schema: JSON.parse(schema).schema,
            data: data
              .filter((d: string) => d.trim().length)
              .map((d: string) => JSON.parse(d).data)
              .reduce((a: any, b: any) => a.concat(b), []),
          };
        } catch (err) {
          throw new Error(response.error);
        }
      },
      options,
      callback
    );
  }

  /**
   * Execute a Cube SQL query against Cube SQL interface and return streaming results as an async generator.
   * The server returns JSONL (JSON Lines) format with schema first, then data rows.
   */
  public async* cubeSqlStream(sqlQuery: string, options?: CubeSqlOptions): AsyncGenerator<CubeSqlStreamChunk> {
    if (!this.transport.requestStream) {
      throw new Error('Transport does not support streaming');
    }

    const streamResponse = this.transport.requestStream('cubesql', {
      method: 'POST',
      signal: options?.signal,
      fetchTimeout: options?.timeout,
      baseRequestId: uuidv4(),
      params: {
        query: sqlQuery
      }
    });

    const decoder = new TextDecoder();
    let buffer = '';

    try {
      const stream = await streamResponse.stream();

      for await (const chunk of stream) {
        buffer += decoder.decode(chunk, { stream: true });

        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          if (line.trim()) {
            try {
              const parsed = JSON.parse(line);

              if (parsed.schema) {
                yield {
                  type: 'schema' as const,
                  schema: parsed.schema
                };
              } else if (parsed.data) {
                yield {
                  type: 'data' as const,
                  data: parsed.data
                };
              } else if (parsed.error) {
                yield {
                  type: 'error' as const,
                  error: parsed.error
                };
              }
            } catch (parseError) {
              yield {
                type: 'error' as const,
                error: `Failed to parse JSON line: ${line}`
              };
            }
          }
        }
      }

      if (buffer.trim()) {
        try {
          const parsed = JSON.parse(buffer);

          if (parsed.schema) {
            yield {
              type: 'schema' as const,
              schema: parsed.schema
            };
          } else if (parsed.data) {
            yield {
              type: 'data' as const,
              data: parsed.data
            };
          } else if (parsed.error) {
            yield {
              type: 'error' as const,
              error: parsed.error
            };
          }
        } catch (parseError) {
          yield {
            type: 'error' as const,
            error: `Failed to parse remaining JSON: ${buffer}`
          };
        }
      }
    } catch (error: any) {
      if (error.name === 'AbortError') {
        throw new Error('aborted');
      }
      throw error;
    } finally {
      if (streamResponse.unsubscribe) {
        await streamResponse.unsubscribe();
      }
    }
  }
}

export default (apiToken: string | (() => Promise<string>), options: CubeApiOptions) => new CubeApi(apiToken, options);

export { CubeApi };
export { default as Meta } from './Meta';
export { default as SqlQuery } from './SqlQuery';
export { default as RequestError } from './RequestError';
export { default as ProgressResult } from './ProgressResult';
export { default as ResultSet } from './ResultSet';
export * from './HttpTransport';
export * from './utils';
export * from './time';
export * from './types';
