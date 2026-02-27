/**
 * @license MIT License
 * @copyright Cube Dev, Inc.
 * @fileoverview Test signal parameter in CubeApi
 */

/* globals describe,test,expect,jest,beforeEach */
/* eslint-disable import/first */

import { CubeApi as CubeApiOriginal, Query } from '../src';
import HttpTransport from '../src/HttpTransport';
import {
  DescriptiveQueryRequest,
  DescriptiveQueryRequestCompact,
  DescriptiveQueryResponse,
  NumericCastedData
} from './helpers';
import ResultSet from '../src/ResultSet';

class CubeApi extends CubeApiOriginal {
  public getTransport(): any {
    return this.transport;
  }

  public makeRequest(method: string, params?: any): any {
    return this.request(method, params);
  }
}

describe('CubeApi Constructor', () => {
  test('throw error if no api url', async () => {
    try {
      const _cubeApi = new CubeApi('token', {} as any);
      throw new Error('Should not get here');
    } catch (e: any) {
      expect(e.message).toBe('The `apiUrl` option is required');
    }
  });
});

describe('CubeApi Load', () => {
  afterEach(() => {
    jest.clearAllMocks();
    jest.restoreAllMocks();
  });

  test('simple query, no options', async () => {
    // Create a spy on the request method
    jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
      subscribe: (cb) => Promise.resolve(cb({
        status: 200,
        text: () => Promise.resolve(JSON.stringify(DescriptiveQueryResponse)),
        json: () => Promise.resolve(DescriptiveQueryResponse)
      } as any,
      async () => undefined as any))
    }));

    const cubeApi = new CubeApi('token', {
      apiUrl: 'http://localhost:4000/cubejs-api/v1',
    });

    const res = await cubeApi.load(DescriptiveQueryRequest as Query);
    expect(res).toBeInstanceOf(ResultSet);
    expect(res.rawData()).toEqual(DescriptiveQueryResponse.results[0].data);
  });

  test('simple query + { mutexKey, castNumerics }', async () => {
    // Create a spy on the request method
    jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
      subscribe: (cb) => Promise.resolve(cb({
        status: 200,
        text: () => Promise.resolve(JSON.stringify(DescriptiveQueryResponse)),
        json: () => Promise.resolve(DescriptiveQueryResponse)
      } as any,
      async () => undefined as any))
    }));

    const cubeApi = new CubeApi({
      apiUrl: 'http://localhost:4000/cubejs-api/v1',
    });

    const res = await cubeApi.load(DescriptiveQueryRequest as Query, { mutexKey: 'mutexKey', castNumerics: true });
    expect(res).toBeInstanceOf(ResultSet);
    expect(res.rawData()).toEqual(NumericCastedData);
  });

  test('simple query + compact response format', async () => {
    // Create a spy on the request method
    jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
      subscribe: (cb) => Promise.resolve(cb({
        status: 200,
        text: () => Promise.resolve(JSON.stringify(DescriptiveQueryResponse)),
        json: () => Promise.resolve(DescriptiveQueryResponse)
      } as any,
      async () => undefined as any))
    }));

    const cubeApi = new CubeApi('token', {
      apiUrl: 'http://localhost:4000/cubejs-api/v1',
    });

    const res = await cubeApi.load(DescriptiveQueryRequestCompact as Query, undefined, undefined, 'compact');
    expect(res).toBeInstanceOf(ResultSet);
    expect(res.rawData()).toEqual(DescriptiveQueryResponse.results[0].data);
  });

  test('2 queries', async () => {
    // Create a spy on the request method
    jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
      subscribe: (cb) => Promise.resolve(cb({
        status: 200,
        text: () => Promise.resolve(JSON.stringify(DescriptiveQueryResponse)),
        json: () => Promise.resolve(DescriptiveQueryResponse)
      } as any,
      async () => undefined as any))
    }));

    const cubeApi = new CubeApi('token', {
      apiUrl: 'http://localhost:4000/cubejs-api/v1',
    });

    const res = await cubeApi.load([DescriptiveQueryRequest as Query, DescriptiveQueryRequest as Query]);
    expect(res).toBeInstanceOf(ResultSet);
    expect(res.rawData()).toEqual(DescriptiveQueryResponse.results[0].data);
  });

  test('simple query + { cache: "no-cache" }', async () => {
    const requestSpy = jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
      subscribe: (cb) => Promise.resolve(cb({
        status: 200,
        text: () => Promise.resolve(JSON.stringify(DescriptiveQueryResponse)),
        json: () => Promise.resolve(DescriptiveQueryResponse)
      } as any,
      async () => undefined as any))
    }));

    const cubeApi = new CubeApi('token', {
      apiUrl: 'http://localhost:4000/cubejs-api/v1',
    });

    const res = await cubeApi.load(DescriptiveQueryRequest as Query, { cache: 'no-cache' });
    expect(res).toBeInstanceOf(ResultSet);
    expect(requestSpy).toHaveBeenCalled();
    expect(requestSpy.mock.calls[0]?.[1]?.cache).toBe('no-cache');
  });

  test('simple query + { cache: "must-revalidate" }', async () => {
    const requestSpy = jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
      subscribe: (cb) => Promise.resolve(cb({
        status: 200,
        text: () => Promise.resolve(JSON.stringify(DescriptiveQueryResponse)),
        json: () => Promise.resolve(DescriptiveQueryResponse)
      } as any,
      async () => undefined as any))
    }));

    const cubeApi = new CubeApi('token', {
      apiUrl: 'http://localhost:4000/cubejs-api/v1',
    });

    const res = await cubeApi.load(DescriptiveQueryRequest as Query, { cache: 'must-revalidate' });
    expect(res).toBeInstanceOf(ResultSet);
    expect(requestSpy).toHaveBeenCalled();
    expect(requestSpy.mock.calls[0]?.[1]?.cache).toBe('must-revalidate');
  });

  test('2 queries + compact response format', async () => {
    // Create a spy on the request method
    jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
      subscribe: (cb) => Promise.resolve(cb({
        status: 200,
        text: () => Promise.resolve(JSON.stringify(DescriptiveQueryResponse)),
        json: () => Promise.resolve(DescriptiveQueryResponse)
      } as any,
      async () => undefined as any))
    }));

    const cubeApi = new CubeApi('token', {
      apiUrl: 'http://localhost:4000/cubejs-api/v1',
    });

    const res = await cubeApi.load([DescriptiveQueryRequestCompact as Query, DescriptiveQueryRequestCompact as Query], undefined, undefined, 'compact');
    expect(res).toBeInstanceOf(ResultSet);
    expect(res.rawData()).toEqual(DescriptiveQueryResponse.results[0].data);
  });
});

describe('CubeApi with Abort Signal', () => {
  afterEach(() => {
    jest.clearAllMocks();
    jest.restoreAllMocks();
  });

  test('should pass signal from constructor to request', async () => {
    const controller = new AbortController();
    const { signal } = controller;

    // Create a spy on the request method
    const requestSpy = jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
      subscribe: (cb) => Promise.resolve(cb({
        status: 200,
        text: () => Promise.resolve('{"results":[]}'),
        json: () => Promise.resolve({ results: [] })
      } as any,
      async () => undefined as any))
    }));

    const cubeApi = new CubeApi('token', {
      apiUrl: 'http://localhost:4000/cubejs-api/v1',
      signal
    });

    // Create a second spy on the load method to verify signal is passed to HttpTransport
    jest.spyOn(cubeApi, 'load');
    await cubeApi.load({
      measures: ['Orders.count']
    });

    // Check if the signal was passed to request method through load
    expect(requestSpy).toHaveBeenCalled();

    // The request method should receive the signal in the call
    // Create a request in the same way as CubeApi.load does
    cubeApi.makeRequest('load', {
      query: { measures: ['Orders.count'] },
      queryType: 'multi'
    });

    // Verify the transport is using the signal
    expect(cubeApi.getTransport().signal).toBe(signal);
  });

  test('should pass signal from options to request', async () => {
    const controller = new AbortController();
    const { signal } = controller;

    // Mock for this specific test
    const requestSpy = jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
      subscribe: (cb) => Promise.resolve(cb({
        status: 200,
        text: () => Promise.resolve('{"results":[]}'),
        json: () => Promise.resolve({ results: [] })
      } as any,
      async () => undefined as any))
    }));

    const cubeApi = new CubeApi('token', {
      apiUrl: 'http://localhost:4000/cubejs-api/v1'
    });

    await cubeApi.load(
      { measures: ['Orders.count'] },
      { signal }
    );

    expect(requestSpy).toHaveBeenCalled();
    expect(requestSpy.mock.calls[0]?.[1]?.signal).toBe(signal);
  });

  test('options signal should override constructor signal', async () => {
    const constructorController = new AbortController();
    const optionsController = new AbortController();

    // Mock for this specific test
    const requestSpy = jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
      subscribe: (cb) => Promise.resolve(cb({
        status: 200,
        text: () => Promise.resolve('{"results":[]}'),
        json: () => Promise.resolve({ results: [] })
      } as any,
      async () => undefined as any))
    }));

    const cubeApi = new CubeApi('token', {
      apiUrl: 'http://localhost:4000/cubejs-api/v1',
      signal: constructorController.signal
    });

    await cubeApi.load(
      { measures: ['Orders.count'] },
      { signal: optionsController.signal }
    );

    expect(requestSpy).toHaveBeenCalled();
    expect(requestSpy.mock.calls[0]?.[1]?.signal).toBe(optionsController.signal);
    expect(requestSpy.mock.calls[0]?.[1]?.signal).not.toBe(constructorController.signal);
  });

  test('should pass signal to meta request', async () => {
    const controller = new AbortController();
    const { signal } = controller;

    // Mock for meta with proper format - include dimensions, segments, and measures with required properties
    const requestSpy = jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
      subscribe: (cb) => Promise.resolve(cb({
        status: 200,
        text: () => Promise.resolve(JSON.stringify({
          cubes: [{
            name: 'Orders',
            title: 'Orders',
            measures: [{
              name: 'count',
              title: 'Count',
              shortTitle: 'Count',
              type: 'number'
            }],
            dimensions: [{
              name: 'status',
              title: 'Status',
              type: 'string'
            }],
            segments: []
          }]
        })),
        json: () => Promise.resolve({
          cubes: [{
            name: 'Orders',
            title: 'Orders',
            measures: [{
              name: 'count',
              title: 'Count',
              shortTitle: 'Count',
              type: 'number'
            }],
            dimensions: [{
              name: 'status',
              title: 'Status',
              type: 'string'
            }],
            segments: []
          }]
        })
      } as any,
      async () => undefined as any))
    }));

    const cubeApi = new CubeApi('token', {
      apiUrl: 'http://localhost:4000/cubejs-api/v1'
    });

    await cubeApi.meta({ signal });

    expect(requestSpy).toHaveBeenCalled();
    expect(requestSpy.mock.calls[0]?.[1]?.signal).toBe(signal);
  });

  test('should pass signal to sql request', async () => {
    const controller = new AbortController();
    const { signal } = controller;

    // Mock for SQL response
    const requestSpy = jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
      subscribe: (cb) => Promise.resolve(cb({
        status: 200,
        text: () => Promise.resolve('{"sql":{"sql":"SELECT * FROM orders"}}'),
        json: () => Promise.resolve({ sql: { sql: 'SELECT * FROM orders' } })
      } as any,
      async () => undefined as any))
    }));

    const cubeApi = new CubeApi('token', {
      apiUrl: 'http://localhost:4000/cubejs-api/v1'
    });

    await cubeApi.sql(
      { measures: ['Orders.count'] },
      { signal }
    );

    expect(requestSpy).toHaveBeenCalled();
    expect(requestSpy.mock.calls[0]?.[1]?.signal).toBe(signal);
  });

  test('should pass signal to dryRun request', async () => {
    const controller = new AbortController();
    const { signal } = controller;

    // Mock for dryRun response
    const requestSpy = jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
      subscribe: (cb) => Promise.resolve(cb({
        status: 200,
        text: () => Promise.resolve('{"queryType":"regular"}'),
        json: () => Promise.resolve({ queryType: 'regular' })
      } as any,
      async () => undefined as any))
    }));

    const cubeApi = new CubeApi('token', {
      apiUrl: 'http://localhost:4000/cubejs-api/v1'
    });

    await cubeApi.dryRun(
      { measures: ['Orders.count'] },
      { signal }
    );

    expect(requestSpy).toHaveBeenCalled();
    expect(requestSpy.mock.calls[0]?.[1]?.signal).toBe(signal);
  });
});

describe('CubeApi cubeSql', () => {
  afterEach(() => {
    jest.clearAllMocks();
    jest.restoreAllMocks();
  });

  const cubeSqlResponseBody = [
    JSON.stringify({
      schema: [
        { name: 'status', column_type: 'String' },
        { name: 'measure(orders_transactions.count)', column_type: 'Int64' }
      ],
      lastRefreshTime: '2026-02-24T00:34:01.594Z'
    }),
    JSON.stringify({ data: [['Cancelled', '27090'], ['Returned', '18232']] }),
    JSON.stringify({ data: [['Shipped', '45102']] }),
  ].join('\n');

  const cubeSqlResponseBodyNoRefreshTime = [
    JSON.stringify({
      schema: [
        { name: 'status', column_type: 'String' },
      ],
    }),
    JSON.stringify({ data: [['Active']] }),
  ].join('\n');

  test('should parse lastRefreshTime from response', async () => {
    jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
      subscribe: (cb) => Promise.resolve(cb({
        status: 200,
        text: () => Promise.resolve(JSON.stringify({ error: cubeSqlResponseBody })),
      } as any,
      async () => undefined as any))
    }));

    const cubeApi = new CubeApi('token', {
      apiUrl: 'http://localhost:4000/cubejs-api/v1',
    });

    const res = await cubeApi.cubeSql('SELECT status, measure(count) FROM orders_transactions');
    expect(res.lastRefreshTime).toBe('2026-02-24T00:34:01.594Z');
    expect(res.schema).toEqual([
      { name: 'status', column_type: 'String' },
      { name: 'measure(orders_transactions.count)', column_type: 'Int64' }
    ]);
    expect(res.data).toEqual([
      ['Cancelled', '27090'],
      ['Returned', '18232'],
      ['Shipped', '45102'],
    ]);
  });

  test('should omit lastRefreshTime when not present in response', async () => {
    jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
      subscribe: (cb) => Promise.resolve(cb({
        status: 200,
        text: () => Promise.resolve(JSON.stringify({ error: cubeSqlResponseBodyNoRefreshTime })),
      } as any,
      async () => undefined as any))
    }));

    const cubeApi = new CubeApi('token', {
      apiUrl: 'http://localhost:4000/cubejs-api/v1',
    });

    const res = await cubeApi.cubeSql('SELECT status FROM users');
    expect(res.lastRefreshTime).toBeUndefined();
    expect(res.schema).toEqual([{ name: 'status', column_type: 'String' }]);
    expect(res.data).toEqual([['Active']]);
  });
});

describe('CubeApi with baseRequestId', () => {
  afterEach(() => {
    jest.clearAllMocks();
    jest.restoreAllMocks();
  });

  test('should pass baseRequestId from options to request', async () => {
    const baseRequestId = 'custom-request-id-123';

    const requestSpy = jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
      subscribe: (cb) => Promise.resolve(cb({
        status: 200,
        text: () => Promise.resolve('{"results":[]}'),
        json: () => Promise.resolve({ results: [] })
      } as any,
      async () => undefined as any))
    }));

    const cubeApi = new CubeApi('token', {
      apiUrl: 'http://localhost:4000/cubejs-api/v1'
    });

    await cubeApi.load(
      { measures: ['Orders.count'] },
      { baseRequestId }
    );

    expect(requestSpy).toHaveBeenCalled();
    expect(requestSpy.mock.calls[0]?.[1]?.baseRequestId).toBe(baseRequestId);
  });

  test('should generate baseRequestId if not provided', async () => {
    const requestSpy = jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
      subscribe: (cb) => Promise.resolve(cb({
        status: 200,
        text: () => Promise.resolve('{"results":[]}'),
        json: () => Promise.resolve({ results: [] })
      } as any,
      async () => undefined as any))
    }));

    const cubeApi = new CubeApi('token', {
      apiUrl: 'http://localhost:4000/cubejs-api/v1'
    });

    await cubeApi.load(
      { measures: ['Orders.count'] }
    );

    expect(requestSpy).toHaveBeenCalled();
    // Should have a baseRequestId (generated via uuidv4)
    expect(requestSpy.mock.calls[0]?.[1]?.baseRequestId).toBeDefined();
    expect(typeof requestSpy.mock.calls[0]?.[1]?.baseRequestId).toBe('string');
  });

  test('should pass baseRequestId to sql request', async () => {
    const baseRequestId = 'sql-request-id-456';

    const requestSpy = jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
      subscribe: (cb) => Promise.resolve(cb({
        status: 200,
        text: () => Promise.resolve('{"sql":{"sql":"SELECT * FROM orders"}}'),
        json: () => Promise.resolve({ sql: { sql: 'SELECT * FROM orders' } })
      } as any,
      async () => undefined as any))
    }));

    const cubeApi = new CubeApi('token', {
      apiUrl: 'http://localhost:4000/cubejs-api/v1'
    });

    await cubeApi.sql(
      { measures: ['Orders.count'] },
      { baseRequestId }
    );

    expect(requestSpy).toHaveBeenCalled();
    expect(requestSpy.mock.calls[0]?.[1]?.baseRequestId).toBe(baseRequestId);
  });

  test('should pass baseRequestId to dryRun request', async () => {
    const baseRequestId = 'dryrun-request-id-789';

    const requestSpy = jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
      subscribe: (cb) => Promise.resolve(cb({
        status: 200,
        text: () => Promise.resolve('{"queryType":"regular"}'),
        json: () => Promise.resolve({ queryType: 'regular' })
      } as any,
      async () => undefined as any))
    }));

    const cubeApi = new CubeApi('token', {
      apiUrl: 'http://localhost:4000/cubejs-api/v1'
    });

    await cubeApi.dryRun(
      { measures: ['Orders.count'] },
      { baseRequestId }
    );

    expect(requestSpy).toHaveBeenCalled();
    expect(requestSpy.mock.calls[0]?.[1]?.baseRequestId).toBe(baseRequestId);
  });

  test('should pass baseRequestId to subscribe request', async () => {
    const baseRequestId = 'subscribe-request-id-abc';

    const requestSpy = jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
      subscribe: (cb) => Promise.resolve(cb({
        status: 200,
        text: () => Promise.resolve('{"results":[]}'),
        json: () => Promise.resolve({ results: [] })
      } as any,
      async () => undefined as any))
    }));

    const cubeApi = new CubeApi('token', {
      apiUrl: 'http://localhost:4000/cubejs-api/v1'
    });

    const subscription = cubeApi.subscribe(
      { measures: ['Orders.count'] },
      { baseRequestId },
      // eslint-disable-next-line @typescript-eslint/no-empty-function
      () => {}
    );

    // Wait for the subscription to be initiated
    await new Promise(resolve => setTimeout(resolve, 0));

    expect(requestSpy).toHaveBeenCalled();
    expect(requestSpy.mock.calls[0]?.[1]?.baseRequestId).toBe(baseRequestId);

    subscription.unsubscribe();
  });

  test('should pass baseRequestId with multiple queries', async () => {
    const baseRequestId = 'multi-query-request-id';

    const requestSpy = jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
      subscribe: (cb) => Promise.resolve(cb({
        status: 200,
        text: () => Promise.resolve(JSON.stringify(DescriptiveQueryResponse)),
        json: () => Promise.resolve(DescriptiveQueryResponse)
      } as any,
      async () => undefined as any))
    }));

    const cubeApi = new CubeApi('token', {
      apiUrl: 'http://localhost:4000/cubejs-api/v1'
    });

    await cubeApi.load(
      [
        { measures: ['Orders.count'] },
        { measures: ['Users.count'] }
      ],
      { baseRequestId }
    );

    expect(requestSpy).toHaveBeenCalled();
    expect(requestSpy.mock.calls[0]?.[1]?.baseRequestId).toBe(baseRequestId);
  });

  test('should pass baseRequestId to meta request', async () => {
    const baseRequestId = 'meta-request-id-def';

    const requestSpy = jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
      subscribe: (cb) => Promise.resolve(cb({
        status: 200,
        text: () => Promise.resolve(JSON.stringify({
          cubes: [{
            name: 'Orders',
            title: 'Orders',
            measures: [{
              name: 'count',
              title: 'Count',
              shortTitle: 'Count',
              type: 'number'
            }],
            dimensions: [{
              name: 'status',
              title: 'Status',
              type: 'string'
            }],
            segments: []
          }]
        })),
        json: () => Promise.resolve({
          cubes: [{
            name: 'Orders',
            title: 'Orders',
            measures: [{
              name: 'count',
              title: 'Count',
              shortTitle: 'Count',
              type: 'number'
            }],
            dimensions: [{
              name: 'status',
              title: 'Status',
              type: 'string'
            }],
            segments: []
          }]
        })
      } as any,
      async () => undefined as any))
    }));

    const cubeApi = new CubeApi('token', {
      apiUrl: 'http://localhost:4000/cubejs-api/v1'
    });

    await cubeApi.meta({ baseRequestId });

    expect(requestSpy).toHaveBeenCalled();
    expect(requestSpy.mock.calls[0]?.[1]?.baseRequestId).toBe(baseRequestId);
  });
});
