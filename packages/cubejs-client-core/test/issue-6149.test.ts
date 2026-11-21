/**
 * @license MIT License
 * @copyright Cube Dev, Inc.
 * @fileoverview Reproduction for https://github.com/cube-js/cube/issues/6149:
 * a 200 response whose body is not a valid load response (e.g. the body text
 * is "[object Blob]", as reported on Safari) makes `load` throw an unreadable
 * `TypeError` from `loadResponseInternal` (`response.results.length`)
 * instead of a descriptive error.
 */

import { vi } from 'vitest';

import { CubeApi } from '../src/index.js';
import HttpTransport from '../src/HttpTransport.js';

function mockResponse(status: number, text: string) {
  vi.spyOn(HttpTransport.prototype, 'request').mockImplementation(() => ({
    subscribe: (cb) => Promise.resolve(cb({
      status,
      text: () => Promise.resolve(text),
      json: () => Promise.resolve(JSON.parse(text)),
    } as any,
    async () => undefined as any))
  }));
}

describe('CubeApi.load with a 200 response that is not a load response (#6149)', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  const cubeApi = () => new CubeApi('token', {
    apiUrl: 'http://localhost:4000/cubejs-api/v1',
  });

  test('non-JSON body is reported as a descriptive error, not a TypeError', async () => {
    mockResponse(200, '[object Blob]');

    const error = await cubeApi().load({ measures: ['Foo.count'] }).catch((e) => e);

    expect(error).toBeInstanceOf(Error);
    expect(error).not.toBeInstanceOf(TypeError);
    expect(error.message).toContain('[object Blob]');
  });

  test('JSON body without `results` is reported as a descriptive error, not a TypeError', async () => {
    mockResponse(200, JSON.stringify({ foo: 'bar' }));

    const error = await cubeApi().load({ measures: ['Foo.count'] }).catch((e) => e);

    expect(error).toBeInstanceOf(Error);
    expect(error).not.toBeInstanceOf(TypeError);
  });
});
