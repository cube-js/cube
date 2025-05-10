/* eslint-disable import/first */
/* eslint-disable import/newline-after-import */
/* globals describe,test,expect,jest,afterEach,beforeAll,beforeEach */
import '@babel/runtime/regenerator';
jest.mock('cross-fetch');
import fetch from 'cross-fetch';
import HttpTransport from './HttpTransport';

describe('HttpTransport', () => {
  const apiUrl = 'http://localhost:3000/cubejs-api/v1';
  const query = {
    measures: ['Orders.count'],
    dimensions: ['Users.country']
  };
  const queryUrlEncoded = '%7B%22measures%22%3A%5B%22Orders.count%22%5D%2C%22dimensions%22%3A%5B%22Users.country%22%5D%7D';
  const queryJson = '{"query":{"measures":["Orders.count"],"dimensions":["Users.country"]}}';

  const ids = [];
  for (let i = 0; i < 40; i++) ids.push('a40b2052-4137-11eb-b378-0242ac130002');
  const LargeQuery = {
    measures: ['Orders.count'],
    dimensions: ['Users.country'],
    filters: [
      {
        member: 'Users.id',
        operator: 'equals',
        values: ids
      }
    ]
  };
  const largeQueryJson = `{"query":{"measures":["Orders.count"],"dimensions":["Users.country"],"filters":[{"member":"Users.id","operator":"equals","values":${JSON.stringify(ids)}}]}}`;

  beforeAll(() => {
    fetch.mockReturnValue(Promise.resolve({ ok: true }));
  });

  afterEach(() => {
    fetch.mockClear();
  });

  test('it serializes the query object and sends it in the query string', async () => {
    const transport = new HttpTransport({
      authorization: 'token',
      apiUrl,
    });
    const req = transport.request('load', { query });
    await req.subscribe(() => { });
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(fetch).toHaveBeenCalledWith(`${apiUrl}/load?query=${queryUrlEncoded}`, {
      method: 'GET',
      headers: {
        Authorization: 'token',
      },
      body: null
    });
  });

  test('it passes extra headers and serializes extra params', async () => {
    const extraParams = { foo: 'bar' };
    const serializedExtraParams = encodeURIComponent(JSON.stringify(extraParams));
    const transport = new HttpTransport({
      authorization: 'token',
      apiUrl,
      headers: {
        'X-Extra-Header': '42'
      }
    });
    const req = transport.request('meta', { extraParams });
    await req.subscribe(() => { });
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(fetch).toHaveBeenCalledWith(`${apiUrl}/meta?extraParams=${serializedExtraParams}`, {
      method: 'GET',
      headers: {
        Authorization: 'token',
        'X-Extra-Header': '42'
      },
      body: null
    });
  });

  test('it serializes the query object and sends it in the body', async () => {
    const transport = new HttpTransport({
      authorization: 'token',
      apiUrl,
      method: 'POST'
    });
    const req = transport.request('load', { query });
    await req.subscribe(() => { });
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(fetch).toHaveBeenCalledWith(`${apiUrl}/load`, {
      method: 'POST',
      headers: {
        Authorization: 'token',
        'Content-Type': 'application/json'
      },
      body: queryJson
    });
  });

  test('it use POST over GET if url length is more than 2000 characters', async () => {
    const transport = new HttpTransport({
      authorization: 'token',
      apiUrl
    });
    const req = transport.request('load', { query: LargeQuery });
    await req.subscribe(() => { });
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(fetch).toHaveBeenCalledWith(`${apiUrl}/load`, {
      method: 'POST',
      headers: {
        Authorization: 'token',
        'Content-Type': 'application/json'
      },
      body: largeQueryJson
    });
  });

  // Signal tests from src/tests/HttpTransport.test.js
  describe('Signal functionality', () => {
    beforeEach(() => {
      fetch.mockClear();
      // Default mock implementation for signal tests
      fetch.mockImplementation(() => Promise.resolve({
        json: () => Promise.resolve({ data: 'test data' }),
        ok: true,
        status: 200
      }));
    });

    test('should pass the signal to fetch when provided in constructor', async () => {
      const controller = new AbortController();
      const { signal } = controller;

      const transport = new HttpTransport({
        authorization: 'token',
        apiUrl: 'http://localhost:4000/cubejs-api/v1',
        signal
      });

      const request = transport.request('load', { query: { measures: ['Orders.count'] } });

      // Start the request
      const promise = request.subscribe((result) => result);

      // Wait for fetch to be called
      await Promise.resolve();

      // Ensure fetch was called with the signal
      expect(fetch).toHaveBeenCalledTimes(1);
      expect(fetch.mock.calls[0][1].signal).toBe(signal);

      await promise;
    });

    test('should pass the signal to fetch when provided in request method', async () => {
      const controller = new AbortController();
      const { signal } = controller;

      const transport = new HttpTransport({
        authorization: 'token',
        apiUrl: 'http://localhost:4000/cubejs-api/v1'
      });

      const request = transport.request('load', {
        query: { measures: ['Orders.count'] },
        signal
      });

      // Start the request
      const promise = request.subscribe((result) => result);

      // Wait for fetch to be called
      await Promise.resolve();

      // Ensure fetch was called with the signal
      expect(fetch).toHaveBeenCalledTimes(1);
      expect(fetch.mock.calls[0][1].signal).toBe(signal);

      await promise;
    });

    test('should prioritize request signal over constructor signal', async () => {
      const controller1 = new AbortController();
      const controller2 = new AbortController();

      const transport = new HttpTransport({
        authorization: 'token',
        apiUrl: 'http://localhost:4000/cubejs-api/v1',
        signal: controller1.signal
      });

      const request = transport.request('load', {
        query: { measures: ['Orders.count'] },
        signal: controller2.signal
      });

      // Start the request
      const promise = request.subscribe((result) => result);

      // Wait for fetch to be called
      await Promise.resolve();

      // Ensure fetch was called with the request signal, not the constructor signal
      expect(fetch).toHaveBeenCalledTimes(1);
      expect(fetch.mock.calls[0][1].signal).toBe(controller2.signal);
      expect(fetch.mock.calls[0][1].signal).not.toBe(controller1.signal);

      await promise;
    });

    test('should create AbortSignal.timeout from fetchTimeout if signal not provided', async () => {
      // Mock AbortSignal.timeout
      const originalTimeout = AbortSignal.timeout;
      const mockTimeoutSignal = {};
      AbortSignal.timeout = jest.fn().mockReturnValue(mockTimeoutSignal);

      const transport = new HttpTransport({
        authorization: 'token',
        apiUrl: 'http://localhost:4000/cubejs-api/v1',
        fetchTimeout: 5000
      });

      const request = transport.request('load', {
        query: { measures: ['Orders.count'] }
      });

      // Start the request
      const promise = request.subscribe((result) => result);

      // Wait for fetch to be called
      await Promise.resolve();

      // Ensure fetch was called with the timeout signal
      expect(fetch).toHaveBeenCalledTimes(1);
      expect(fetch.mock.calls[0][1].signal).toBe(mockTimeoutSignal);
      expect(AbortSignal.timeout).toHaveBeenCalledWith(5000);

      // Restore original implementation
      AbortSignal.timeout = originalTimeout;

      await promise;
    });

    test('should handle request abortion', async () => {
      // Create a mock Promise and resolver function to control Promise completion
      let resolveFetch;
      const fetchPromise = new Promise(resolve => {
        resolveFetch = resolve;
      });

      // Mock fetch to return our controlled Promise
      fetch.mockImplementationOnce(() => fetchPromise);

      const controller = new AbortController();
      const { signal } = controller;

      const transport = new HttpTransport({
        authorization: 'token',
        apiUrl: 'http://localhost:4000/cubejs-api/v1'
      });

      const request = transport.request('load', {
        query: { measures: ['Orders.count'] },
        signal
      });

      // Start the request but don't wait for it to complete
      const requestPromise = request.subscribe((result) => result);

      // Wait for fetch to be called
      await Promise.resolve();

      // Ensure fetch was called with the signal
      expect(fetch).toHaveBeenCalledTimes(1);
      expect(fetch.mock.calls[0][1].signal).toBe(signal);

      // Abort the request
      controller.abort();

      // Resolve the fetch Promise, simulating request completion
      resolveFetch({
        json: () => Promise.resolve({ data: 'aborted data' }),
        ok: true,
        status: 200
      });

      // Wait for the request Promise to complete
      await requestPromise;
    }, 10000); // Set 10-second timeout
  });
});
