/**
 * Unit tests for TrinoDriver query execution: backoff, drain, retries,
 * streaming, errors, auth, and result normalization.
 */

/* eslint-disable import/first */
const mockFetch: jest.Mock = jest.fn();
jest.mock('node-fetch', () => ({
  __esModule: true,
  default: (...args: any[]) => mockFetch(...args),
}));

jest.mock('@cubejs-backend/schema-compiler', () => ({
  PrestodbQuery: class PrestodbQuery {},
  TrinoQuery: class TrinoQuery {},
}));

import { TrinoDriver } from '../../src/TrinoDriver';

function mockResponse(body: object, status = 200): any {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: status === 200 ? 'OK' : 'Error',
    json: async () => body,
    text: async () => JSON.stringify(body),
  };
}

function createDriver(overrides: Record<string, any> = {}) {
  return new TrinoDriver({
    host: 'localhost',
    port: 8080,
    catalog: 'test_catalog',
    schema: 'test_schema',
    user: 'test_user',
    pollBackoff: {
      initialInterval: 1,
      incrementStep: 1,
      maxInterval: 5,
      triesBeforeIncrement: 2,
    },
    ...overrides,
  });
}

beforeEach(() => {
  mockFetch.mockReset();
});

describe('normalizeResultOverColumns', () => {
  it('should map row arrays to column-keyed objects', () => {
    const driver = createDriver();
    const columns = [{ name: 'id', type: 'integer' }, { name: 'name', type: 'varchar' }];
    const data = [[1, 'alice'], [2, 'bob']];
    const result = driver.normalizeResultOverColumns(data, columns);

    expect(result).toEqual([
      { id: 1, name: 'alice' },
      { id: 2, name: 'bob' },
    ]);
  });

  it('should handle empty data', () => {
    const driver = createDriver();
    const result = driver.normalizeResultOverColumns([], [{ name: 'id', type: 'integer' }]);
    expect(result).toEqual([]);
  });
});

describe('executeBuffered (queryPromised)', () => {
  it('should return rows from a single-page response', async () => {
    const driver = createDriver();
    const columns = [{ name: 'x', type: 'integer' }];

    mockFetch.mockResolvedValueOnce(
      mockResponse({ columns, data: [[1], [2]], stats: { state: 'FINISHED' } }),
    );

    const result = await driver.queryPromised('SELECT x FROM t', false);
    expect(result).toEqual([{ x: 1 }, { x: 2 }]);
    await driver.release();
  });

  it('should follow nextUri and accumulate rows in arrival order', async () => {
    const driver = createDriver();
    const columns = [{ name: 'v', type: 'integer' }];

    mockFetch.mockResolvedValueOnce(
      mockResponse({ nextUri: 'http://localhost:8080/next/1', columns, stats: { state: 'RUNNING' } }),
    );
    mockFetch.mockResolvedValueOnce(
      mockResponse({ nextUri: 'http://localhost:8080/next/2', data: [[10]], stats: { state: 'RUNNING' } }),
    );
    mockFetch.mockResolvedValueOnce(
      mockResponse({ data: [[20]], stats: { state: 'FINISHED' } }),
    );

    const result = await driver.queryPromised('SELECT v FROM t', false);
    expect(result).toEqual([{ v: 10 }, { v: 20 }]);
    await driver.release();
  });

  it('should apply progressive backoff while waiting for first rows', async () => {
    const driver = createDriver();
    const sleepSpy = jest.spyOn(driver as any, 'sleep').mockResolvedValue(undefined);
    const columns = [{ name: 'a', type: 'integer' }];

    mockFetch.mockResolvedValueOnce(
      mockResponse({ nextUri: 'http://localhost:8080/next/1', stats: { state: 'QUEUED' } }),
    );
    mockFetch.mockResolvedValueOnce(
      mockResponse({ nextUri: 'http://localhost:8080/next/2', stats: { state: 'PLANNING' } }),
    );
    mockFetch.mockResolvedValueOnce(
      mockResponse({ columns, data: [[42]], stats: { state: 'FINISHED' } }),
    );

    await driver.queryPromised('SELECT a FROM t', false);

    expect(sleepSpy).toHaveBeenCalledTimes(2);
    expect(sleepSpy).toHaveBeenNthCalledWith(1, 1);
    expect(sleepSpy).toHaveBeenNthCalledWith(2, 1);
    sleepSpy.mockRestore();
    await driver.release();
  });

  it('should NOT sleep once data is flowing (default drainInterval=0)', async () => {
    const driver = createDriver();
    const sleepSpy = jest.spyOn(driver as any, 'sleep').mockResolvedValue(undefined);
    const columns = [{ name: 'a', type: 'integer' }];

    mockFetch.mockResolvedValueOnce(
      mockResponse({ nextUri: 'http://localhost:8080/next/1', columns, data: [[1]], stats: { state: 'RUNNING' } }),
    );
    mockFetch.mockResolvedValueOnce(
      mockResponse({ columns, data: [[2]], stats: { state: 'FINISHED' } }),
    );

    await driver.queryPromised('SELECT a FROM t', false);
    expect(sleepSpy).not.toHaveBeenCalled();
    sleepSpy.mockRestore();
    await driver.release();
  });

  it('should sleep drainInterval between pages after the first rows', async () => {
    const driver = createDriver({ drainInterval: 7 });
    const sleepSpy = jest.spyOn(driver as any, 'sleep').mockResolvedValue(undefined);
    const columns = [{ name: 'a', type: 'integer' }];

    mockFetch.mockResolvedValueOnce(
      mockResponse({ nextUri: 'http://localhost:8080/next/1', columns, data: [[1]], stats: { state: 'RUNNING' } }),
    );
    mockFetch.mockResolvedValueOnce(
      mockResponse({ columns, data: [[2]], stats: { state: 'FINISHED' } }),
    );

    await driver.queryPromised('SELECT a FROM t', false);
    expect(sleepSpy).toHaveBeenCalledTimes(1);
    expect(sleepSpy).toHaveBeenCalledWith(7);
    sleepSpy.mockRestore();
    await driver.release();
  });

  it('should honor legacy checkInterval for both wait and drain phases', async () => {
    const driver = createDriver({
      pollBackoff: undefined,
      checkInterval: 9,
    });
    const sleepSpy = jest.spyOn(driver as any, 'sleep').mockResolvedValue(undefined);
    const columns = [{ name: 'a', type: 'integer' }];

    mockFetch.mockResolvedValueOnce(
      mockResponse({ nextUri: 'http://localhost:8080/next/1', stats: { state: 'QUEUED' } }),
    );
    mockFetch.mockResolvedValueOnce(
      mockResponse({ nextUri: 'http://localhost:8080/next/2', columns, data: [[1]], stats: { state: 'RUNNING' } }),
    );
    mockFetch.mockResolvedValueOnce(
      mockResponse({ data: [[2]], stats: { state: 'FINISHED' } }),
    );

    await driver.queryPromised('SELECT a FROM t', false);
    expect(sleepSpy.mock.calls.map((c) => c[0])).toEqual([9, 9]);
    sleepSpy.mockRestore();
    await driver.release();
  });

  it('should backoff on empty data array until first rows arrive', async () => {
    const driver = createDriver();
    const sleepSpy = jest.spyOn(driver as any, 'sleep').mockResolvedValue(undefined);
    const columns = [{ name: 'a', type: 'integer' }];

    mockFetch.mockResolvedValueOnce(
      mockResponse({ nextUri: 'http://localhost:8080/next/1', columns, data: [], stats: { state: 'RUNNING' } }),
    );
    mockFetch.mockResolvedValueOnce(
      mockResponse({ data: [[99]], stats: { state: 'FINISHED' } }),
    );

    await driver.queryPromised('SELECT a FROM t', false);
    expect(sleepSpy).toHaveBeenCalledTimes(1);
    expect(sleepSpy).toHaveBeenCalledWith(1);
    sleepSpy.mockRestore();
    await driver.release();
  });

  it('should increase interval only after triesBeforeIncrement holds', async () => {
    const driver = createDriver({
      pollBackoff: {
        initialInterval: 3,
        incrementStep: 3,
        maxInterval: 5,
        triesBeforeIncrement: 2,
      },
    });
    const sleepSpy = jest.spyOn(driver as any, 'sleep').mockResolvedValue(undefined);
    const columns = [{ name: 'a', type: 'integer' }];

    mockFetch.mockResolvedValueOnce(
      mockResponse({ nextUri: 'http://localhost:8080/next/1', stats: { state: 'QUEUED' } }),
    );
    mockFetch.mockResolvedValueOnce(
      mockResponse({ nextUri: 'http://localhost:8080/next/2', stats: { state: 'QUEUED' } }),
    );
    mockFetch.mockResolvedValueOnce(
      mockResponse({ nextUri: 'http://localhost:8080/next/3', stats: { state: 'PLANNING' } }),
    );
    mockFetch.mockResolvedValueOnce(
      mockResponse({ nextUri: 'http://localhost:8080/next/4', stats: { state: 'PLANNING' } }),
    );
    mockFetch.mockResolvedValueOnce(
      mockResponse({ columns, data: [[1]], stats: { state: 'FINISHED' } }),
    );

    await driver.queryPromised('SELECT a FROM t', false);

    expect(sleepSpy.mock.calls.map((c) => c[0])).toEqual([3, 3, 5, 5]);
    sleepSpy.mockRestore();
    await driver.release();
  });

  it('should use TrinoQuery dialect', () => {
    expect(TrinoDriver.dialectClass().name).toBe('TrinoQuery');
  });

  it('should throw on Trino query error', async () => {
    const driver = createDriver();

    mockFetch.mockResolvedValueOnce(
      mockResponse({
        error: { message: 'SYNTAX_ERROR', errorName: 'SYNTAX_ERROR' },
        stats: { state: 'FAILED' },
      }),
    );

    await expect(driver.queryPromised('BAD SQL', false)).rejects.toThrow('Trino query error (SYNTAX_ERROR)');
    await driver.release();
  });
});

describe('fetchNextUri bounded retries', () => {
  it('should retry on 503 and succeed', async () => {
    const driver = createDriver();
    jest.spyOn(driver as any, 'sleep').mockResolvedValue(undefined);

    mockFetch.mockResolvedValueOnce(
      mockResponse({ nextUri: 'http://localhost:8080/next/1', stats: { state: 'RUNNING' } }),
    );
    mockFetch.mockResolvedValueOnce(mockResponse({}, 503));
    mockFetch.mockResolvedValueOnce(
      mockResponse({ data: [[1]], columns: [{ name: 'x', type: 'integer' }], stats: { state: 'FINISHED' } }),
    );

    const result = await driver.queryPromised('SELECT 1', false);
    expect(result).toEqual([{ x: 1 }]);
    expect(mockFetch).toHaveBeenCalledTimes(3);
    await driver.release();
  });

  it('should throw after maxTransientRetries consecutive failures', async () => {
    const driver = createDriver({ maxTransientRetries: 5 });
    jest.spyOn(driver as any, 'sleep').mockResolvedValue(undefined);

    mockFetch.mockResolvedValueOnce(
      mockResponse({ nextUri: 'http://localhost:8080/next/1', stats: { state: 'RUNNING' } }),
    );
    for (let i = 0; i < 6; i++) {
      mockFetch.mockResolvedValueOnce(mockResponse({}, 502));
    }

    await expect(driver.queryPromised('SELECT 1', false)).rejects.toThrow(
      /Trino poll failed after 5 retries/,
    );
    await driver.release();
  });
});

describe('executeStreaming (queryPromised streaming)', () => {
  it('should resolve with rowStream and types when columns arrive', async () => {
    const driver = createDriver();
    const columns = [{ name: 'id', type: 'integer' }, { name: 'val', type: 'varchar' }];

    mockFetch.mockResolvedValueOnce(
      mockResponse({ nextUri: 'http://localhost:8080/next/1', columns, data: [[1, 'a']], stats: { state: 'RUNNING' } }),
    );
    mockFetch.mockResolvedValueOnce(
      mockResponse({ data: [[2, 'b']], stats: { state: 'FINISHED' } }),
    );

    const result = (await driver.queryPromised('SELECT id, val FROM t', true)) as any;
    expect(result.types).toEqual(columns);
    expect(result.rowStream).toBeDefined();

    const rows: any[] = [];
    await new Promise<void>((resolve) => {
      result.rowStream.on('data', (row: any) => rows.push(row));
      result.rowStream.on('end', resolve);
    });

    expect(rows).toEqual([{ id: 1, val: 'a' }, { id: 2, val: 'b' }]);
    await driver.release();
  });

  it('should resolve even when no columns are returned (no-hang guarantee)', async () => {
    const driver = createDriver();

    mockFetch.mockResolvedValueOnce(
      mockResponse({ stats: { state: 'FINISHED' } }),
    );

    const result = (await driver.queryPromised('CREATE TABLE t(x int)', true)) as any;
    expect(result.rowStream).toBeDefined();
    expect(result.types).toEqual([]);
    await driver.release();
  });

  it('should destroy stream on error after promise is resolved', async () => {
    const driver = createDriver();
    const columns = [{ name: 'id', type: 'integer' }];

    mockFetch.mockResolvedValueOnce(
      mockResponse({ nextUri: 'http://localhost:8080/next/1', columns, data: [[1]], stats: { state: 'RUNNING' } }),
    );
    mockFetch.mockRejectedValueOnce(new Error('network failure'));

    const result = (await driver.queryPromised('SELECT id FROM t', true)) as any;

    await new Promise<void>((resolve) => {
      result.rowStream.on('error', (err: Error) => {
        expect(err.message).toBe('network failure');
        resolve();
      });
    });
    await driver.release();
  });

  it('should reject promise on error before columns arrive', async () => {
    const driver = createDriver();

    mockFetch.mockResolvedValueOnce(
      mockResponse({
        error: { message: 'Permission denied', errorName: 'PERMISSION_DENIED' },
        stats: { state: 'FAILED' },
      }),
    );

    await expect(driver.queryPromised('SELECT 1', true)).rejects.toThrow('Trino query error (PERMISSION_DENIED)');
    await driver.release();
  });

  it('should backoff in streaming mode until first rows, then drain', async () => {
    const driver = createDriver();
    const sleepSpy = jest.spyOn(driver as any, 'sleep').mockResolvedValue(undefined);
    const columns = [{ name: 'a', type: 'integer' }];

    mockFetch.mockResolvedValueOnce(
      mockResponse({ nextUri: 'http://localhost:8080/next/1', columns, data: [], stats: { state: 'RUNNING' } }),
    );
    mockFetch.mockResolvedValueOnce(
      mockResponse({ data: [[1]], stats: { state: 'FINISHED' } }),
    );

    const result = (await driver.queryPromised('SELECT a FROM t', true)) as any;

    const rows: any[] = [];
    await new Promise<void>((resolve) => {
      result.rowStream.on('data', (row: any) => rows.push(row));
      result.rowStream.on('end', resolve);
    });

    expect(rows).toEqual([{ a: 1 }]);
    expect(sleepSpy).toHaveBeenCalledTimes(1);
    expect(sleepSpy).toHaveBeenCalledWith(1);
    sleepSpy.mockRestore();
    await driver.release();
  });
});

describe('testConnection', () => {
  it('should succeed on 200 response from /v1/info', async () => {
    const driver = createDriver();
    mockFetch.mockResolvedValueOnce(mockResponse({ starting: false }));

    await expect(driver.testConnection()).resolves.toBeUndefined();
    expect(mockFetch.mock.calls[0][0]).toBe('http://localhost:8080/v1/info');
    await driver.release();
  });

  it('should throw on non-OK response', async () => {
    const driver = createDriver();
    mockFetch.mockResolvedValueOnce(mockResponse({}, 503));

    await expect(driver.testConnection()).rejects.toThrow('Connection test failed');
    await driver.release();
  });
});

describe('configuration', () => {
  it('should build basic auth header', async () => {
    const driver = createDriver({ basic_auth: { user: 'u', password: 'p' } });
    mockFetch.mockResolvedValueOnce(mockResponse({ starting: false }));

    await driver.testConnection();
    const headers = mockFetch.mock.calls[0][1].headers;
    expect(headers.Authorization).toBe(`Basic ${Buffer.from('u:p').toString('base64')}`);
    await driver.release();
  });

  it('should build custom auth header (bearer token)', async () => {
    const driver = createDriver({ custom_auth: 'Bearer tok123' });
    mockFetch.mockResolvedValueOnce(mockResponse({ starting: false }));

    await driver.testConnection();
    const headers = mockFetch.mock.calls[0][1].headers;
    expect(headers.Authorization).toBe('Bearer tok123');
    await driver.release();
  });

  it('should set catalog, schema, and default source headers', async () => {
    const driver = createDriver({ catalog: 'hive', schema: 'default' });
    mockFetch.mockResolvedValueOnce(mockResponse({ starting: false }));

    await driver.testConnection();
    const headers = mockFetch.mock.calls[0][1].headers;
    expect(headers['X-Trino-Catalog']).toBe('hive');
    expect(headers['X-Trino-Schema']).toBe('default');
    expect(headers['X-Trino-Source']).toBe('nodejs-client');
    await driver.release();
  });

  it('should set session header with query timeout', async () => {
    const driver = createDriver({ queryTimeout: 300 });
    const columns = [{ name: 'x', type: 'integer' }];

    mockFetch.mockResolvedValueOnce(
      mockResponse({ columns, data: [[1]], stats: { state: 'FINISHED' } }),
    );

    await driver.queryPromised('SELECT 1', false);
    const headers = mockFetch.mock.calls[0][1].headers;
    expect(headers['X-Trino-Session']).toBe('query_max_run_time=300s');
    await driver.release();
  });

  it('should merge extra session properties with query timeout', async () => {
    const driver = createDriver({
      queryTimeout: 60,
      session: { 'hive.insert_existing_partitions_behavior': 'overwrite' },
    });
    const columns = [{ name: 'x', type: 'integer' }];

    mockFetch.mockResolvedValueOnce(
      mockResponse({ columns, data: [[1]], stats: { state: 'FINISHED' } }),
    );

    await driver.queryPromised('SELECT 1', false);
    const headers = mockFetch.mock.calls[0][1].headers;
    expect(headers['X-Trino-Session']).toBe(
      'query_max_run_time=60s,hive.insert_existing_partitions_behavior=overwrite'
    );
    await driver.release();
  });
});

describe('capabilities', () => {
  it('should report unloadWithoutTempTable', () => {
    const driver = createDriver();
    expect(driver.capabilities()).toEqual({ unloadWithoutTempTable: true });
  });
});
