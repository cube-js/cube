/**
 * @license MIT License
 * @copyright Cube Dev, Inc.
 * @fileoverview Test signal parameter in CubeApi
 */

/* globals describe,test,expect,jest,beforeEach */

import 'jest';
import { CubeApi } from '../index';
import HttpTransport from '../HttpTransport';

describe('CubeApi with Signal', () => {
  beforeEach(() => {
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
      }))
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
    cubeApi.request('load', {
      query: { measures: ['Orders.count'] },
      queryType: 'multi'
    });

    // Verify the transport is using the signal
    expect(cubeApi.transport.signal).toBe(signal);
  });

  test('should pass signal from options to request', async () => {
    const controller = new AbortController();
    const { signal } = controller;

    // Mock for this specific test
    jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
      subscribe: (cb) => Promise.resolve(cb({
        status: 200,
        text: () => Promise.resolve('{"results":[]}'),
        json: () => Promise.resolve({ results: [] })
      }))
    }));

    const cubeApi = new CubeApi('token', {
      apiUrl: 'http://localhost:4000/cubejs-api/v1'
    });

    await cubeApi.load(
      { measures: ['Orders.count'] },
      { signal }
    );

    expect(HttpTransport.prototype.request).toHaveBeenCalled();
    expect(HttpTransport.prototype.request.mock.calls[0][1].signal).toBe(signal);
  });

  test('options signal should override constructor signal', async () => {
    const constructorController = new AbortController();
    const optionsController = new AbortController();

    // Mock for this specific test
    jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
      subscribe: (cb) => Promise.resolve(cb({
        status: 200,
        text: () => Promise.resolve('{"results":[]}'),
        json: () => Promise.resolve({ results: [] })
      }))
    }));

    const cubeApi = new CubeApi('token', {
      apiUrl: 'http://localhost:4000/cubejs-api/v1',
      signal: constructorController.signal
    });

    await cubeApi.load(
      { measures: ['Orders.count'] },
      { signal: optionsController.signal }
    );

    expect(HttpTransport.prototype.request).toHaveBeenCalled();
    expect(HttpTransport.prototype.request.mock.calls[0][1].signal).toBe(optionsController.signal);
    expect(HttpTransport.prototype.request.mock.calls[0][1].signal).not.toBe(constructorController.signal);
  });

  test('should pass signal to meta request', async () => {
    const controller = new AbortController();
    const { signal } = controller;

    // Mock for meta with proper format - include dimensions, segments, and measures with required properties
    jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
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
      }))
    }));

    const cubeApi = new CubeApi('token', {
      apiUrl: 'http://localhost:4000/cubejs-api/v1'
    });

    await cubeApi.meta({ signal });

    expect(HttpTransport.prototype.request).toHaveBeenCalled();
    expect(HttpTransport.prototype.request.mock.calls[0][1].signal).toBe(signal);
  });

  test('should pass signal to sql request', async () => {
    const controller = new AbortController();
    const { signal } = controller;

    // Mock for SQL response
    jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
      subscribe: (cb) => Promise.resolve(cb({
        status: 200,
        text: () => Promise.resolve('{"sql":{"sql":"SELECT * FROM orders"}}'),
        json: () => Promise.resolve({ sql: { sql: 'SELECT * FROM orders' } })
      }))
    }));

    const cubeApi = new CubeApi('token', {
      apiUrl: 'http://localhost:4000/cubejs-api/v1'
    });

    await cubeApi.sql(
      { measures: ['Orders.count'] },
      { signal }
    );

    expect(HttpTransport.prototype.request).toHaveBeenCalled();
    expect(HttpTransport.prototype.request.mock.calls[0][1].signal).toBe(signal);
  });

  test('should pass signal to dryRun request', async () => {
    const controller = new AbortController();
    const { signal } = controller;

    // Mock for dryRun response
    jest.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
      subscribe: (cb) => Promise.resolve(cb({
        status: 200,
        text: () => Promise.resolve('{"queryType":"regular"}'),
        json: () => Promise.resolve({ queryType: 'regular' })
      }))
    }));

    const cubeApi = new CubeApi('token', {
      apiUrl: 'http://localhost:4000/cubejs-api/v1'
    });

    await cubeApi.dryRun(
      { measures: ['Orders.count'] },
      { signal }
    );

    expect(HttpTransport.prototype.request).toHaveBeenCalled();
    expect(HttpTransport.prototype.request.mock.calls[0][1].signal).toBe(signal);
  });
});
