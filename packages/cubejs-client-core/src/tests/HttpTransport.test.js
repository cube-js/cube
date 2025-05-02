/* globals describe,test,expect,jest,beforeEach */

import 'jest';
import fetch from 'cross-fetch';
import HttpTransport from '../HttpTransport';

// Import the mocked fetch

// Mock cross-fetch
jest.mock('cross-fetch', () => jest.fn().mockImplementation(() => Promise.resolve({
  json: () => Promise.resolve({ data: 'test data' }),
  ok: true,
  status: 200
})));

describe('HttpTransport', () => {
  beforeEach(() => {
    fetch.mockClear();
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
