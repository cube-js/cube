/**
 * Constructor / env / protocol edge cases for TrinoDriver.
 */

const mockFetch: jest.Mock = jest.fn();
jest.mock('node-fetch', () => ({
  __esModule: true,
  default: (...args: any[]) => mockFetch(...args),
}));

jest.mock('@cubejs-backend/schema-compiler', () => ({
  PrestodbQuery: class PrestodbQuery {},
  TrinoQuery: class TrinoQuery {},
}));

/* eslint-disable import/first */
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

const baseConfig = {
  host: 'localhost',
  port: 8080,
  catalog: 'hive',
  schema: 'default',
  user: 'test',
  dataSource: 'default',
};

function createDriver(overrides: Record<string, any> = {}) {
  return new TrinoDriver({
    ...baseConfig,
    ...overrides,
  });
}

const TRINO_ENV_KEYS = [
  'CUBEJS_DB_TRINO_SOURCE',
  'CUBEJS_DB_TRINO_POLL_INITIAL_INTERVAL',
  'CUBEJS_DB_TRINO_POLL_INCREMENT_STEP',
  'CUBEJS_DB_TRINO_POLL_MAX_INTERVAL',
  'CUBEJS_DB_TRINO_POLL_TRIES_BEFORE_INCREMENT',
  'CUBEJS_DB_PRESTO_AUTH_TOKEN',
  'CUBEJS_DB_PASS',
  'CUBEJS_DB_USER',
];

beforeEach(() => {
  mockFetch.mockReset();
  TRINO_ENV_KEYS.forEach((key) => {
    delete process.env[key];
  });
});

afterEach(() => {
  TRINO_ENV_KEYS.forEach((key) => {
    delete process.env[key];
  });
});

describe('constructor validation', () => {
  it('throws when both custom_auth and basic_auth are set', () => {
    expect(() => createDriver({
      custom_auth: 'Bearer tok',
      basic_auth: { user: 'u', password: 'p' },
    })).toThrow('Please do not specify basic_auth and custom_auth at the same time.');
  });

  it('throws when env auth token and password are both set', () => {
    process.env.CUBEJS_DB_PRESTO_AUTH_TOKEN = 'tok';
    process.env.CUBEJS_DB_PASS = 'secret';
    process.env.CUBEJS_DB_USER = 'u';
    expect(() => createDriver()).toThrow(/Both user\/password and auth token/);
  });

  it('throws on negative pollBackoff.initialInterval', () => {
    expect(() => createDriver({
      pollBackoff: { initialInterval: -1 },
    })).toThrow(/pollBackoff.initialInterval must be a non-negative integer/);
  });

  it('throws on NaN drainInterval', () => {
    expect(() => createDriver({ drainInterval: Number.NaN })).toThrow(
      /drainInterval must be a non-negative integer/
    );
  });

  it('throws when initialInterval > maxInterval', () => {
    expect(() => createDriver({
      pollBackoff: { initialInterval: 800, maxInterval: 100 },
    })).toThrow(/initialInterval \(800\) must be <= maxInterval \(100\)/);
  });

  it('throws when triesBeforeIncrement is 0', () => {
    expect(() => createDriver({
      pollBackoff: { triesBeforeIncrement: 0 },
    })).toThrow(/triesBeforeIncrement must be >= 1/);
  });

  it('throws on negative checkInterval', () => {
    expect(() => createDriver({ checkInterval: -5 })).toThrow(
      /pollBackoff.initialInterval must be a non-negative integer/
    );
  });

  it('throws on negative maxTransientRetries', () => {
    expect(() => createDriver({ maxTransientRetries: -1 })).toThrow(
      /maxTransientRetries must be a non-negative integer/
    );
  });

  it('allows incrementStep 0 (constant wait-phase interval)', () => {
    const driver = createDriver({
      pollBackoff: { initialInterval: 10, incrementStep: 0, maxInterval: 10, triesBeforeIncrement: 1 },
    });
    expect((driver as any).pollBackoff.incrementStep).toBe(0);
    driver.release();
  });

  it('allows drainInterval 0 and maxTransientRetries 0', () => {
    const driver = createDriver({ drainInterval: 0, maxTransientRetries: 0 });
    expect((driver as any).drainInterval).toBe(0);
    expect((driver as any).maxTransientRetries).toBe(0);
    driver.release();
  });

  it('uses basic_auth.user as X-Trino-User when user is omitted', async () => {
    const { user, ...rest } = baseConfig;
    const driver = new TrinoDriver({
      ...rest,
      basic_auth: { user: 'presto', password: '' },
    });
    mockFetch.mockResolvedValueOnce(mockResponse({ starting: false }));
    await driver.testConnection();
    expect(mockFetch.mock.calls[0][1].headers['X-Trino-User']).toBe('presto');
    await driver.release();
  });

  it('keeps an explicit user when it differs from basic_auth (impersonation)', async () => {
    const driver = createDriver({
      user: 'alice',
      basic_auth: { user: 'presto', password: '' },
    });
    mockFetch.mockResolvedValueOnce(mockResponse({ starting: false }));
    await driver.testConnection();
    expect(mockFetch.mock.calls[0][1].headers['X-Trino-User']).toBe('alice');
    await driver.release();
  });
});

describe('env-driven configuration', () => {
  it('applies poll env vars when pollBackoff is not passed', () => {
    process.env.CUBEJS_DB_TRINO_POLL_INITIAL_INTERVAL = '20';
    process.env.CUBEJS_DB_TRINO_POLL_INCREMENT_STEP = '10';
    process.env.CUBEJS_DB_TRINO_POLL_MAX_INTERVAL = '80';
    process.env.CUBEJS_DB_TRINO_POLL_TRIES_BEFORE_INCREMENT = '2';

    const driver = createDriver();
    expect((driver as any).pollBackoff).toEqual({
      initialInterval: 20,
      incrementStep: 10,
      maxInterval: 80,
      triesBeforeIncrement: 2,
    });
    driver.release();
  });

  it('lets constructor pollBackoff override env', () => {
    process.env.CUBEJS_DB_TRINO_POLL_INITIAL_INTERVAL = '20';
    const driver = createDriver({
      pollBackoff: { initialInterval: 7, incrementStep: 1, maxInterval: 9, triesBeforeIncrement: 1 },
    });
    expect((driver as any).pollBackoff.initialInterval).toBe(7);
    driver.release();
  });

  it('throws on improper poll env values at construction', () => {
    process.env.CUBEJS_DB_TRINO_POLL_INITIAL_INTERVAL = 'abc';
    expect(() => createDriver()).toThrow(/CUBEJS_DB_TRINO_POLL_INITIAL_INTERVAL/);
  });

  it('falls back to nodejs-client when CUBEJS_DB_TRINO_SOURCE is empty', async () => {
    process.env.CUBEJS_DB_TRINO_SOURCE = '';
    const driver = createDriver();
    mockFetch.mockResolvedValueOnce(mockResponse({ starting: false }));
    await driver.testConnection();
    expect(mockFetch.mock.calls[0][1].headers['X-Trino-Source']).toBe('nodejs-client');
    await driver.release();
  });

  it('sends CUBEJS_DB_TRINO_SOURCE when set', async () => {
    process.env.CUBEJS_DB_TRINO_SOURCE = 'etl-worker';
    const driver = createDriver();
    mockFetch.mockResolvedValueOnce(mockResponse({ starting: false }));
    await driver.testConnection();
    expect(mockFetch.mock.calls[0][1].headers['X-Trino-Source']).toBe('etl-worker');
    await driver.release();
  });
});

describe('protocol edge cases', () => {
  it('does not retry non-transient poll errors (401)', async () => {
    const driver = createDriver({
      pollBackoff: { initialInterval: 1, incrementStep: 1, maxInterval: 1, triesBeforeIncrement: 1 },
    });
    jest.spyOn(driver as any, 'sleep').mockResolvedValue(undefined);

    mockFetch.mockResolvedValueOnce(
      mockResponse({ nextUri: 'http://localhost:8080/next/1', stats: { state: 'RUNNING' } }),
    );
    mockFetch.mockResolvedValueOnce(mockResponse({ message: 'unauthorized' }, 401));

    await expect(driver.queryPromised('SELECT 1', false)).rejects.toThrow(/Trino poll failed \(401\)/);
    expect(mockFetch).toHaveBeenCalledTimes(2);
    await driver.release();
  });

  it('throws on statement POST failure', async () => {
    const driver = createDriver();
    mockFetch.mockResolvedValueOnce(mockResponse({ message: 'bad request' }, 400));
    await expect(driver.queryPromised('SELECT 1', false)).rejects.toThrow(/Trino statement POST failed \(400\)/);
    await driver.release();
  });

  it('accumulates a data page that arrives only after nextUri is gone', async () => {
    const driver = createDriver({
      pollBackoff: { initialInterval: 1, incrementStep: 1, maxInterval: 1, triesBeforeIncrement: 1 },
    });
    jest.spyOn(driver as any, 'sleep').mockResolvedValue(undefined);
    const columns = [{ name: 'v', type: 'integer' }];

    mockFetch.mockResolvedValueOnce(
      mockResponse({ nextUri: 'http://localhost:8080/next/1', columns, stats: { state: 'RUNNING' } }),
    );
    mockFetch.mockResolvedValueOnce(
      mockResponse({ columns, data: [[7]], stats: { state: 'FINISHED' } }),
    );

    const result = await driver.queryPromised('SELECT v FROM t', false);
    expect(result).toEqual([{ v: 7 }]);
    await driver.release();
  });

  it('maps null cells and ignores extra row values past column count', () => {
    const driver = createDriver();
    const result = driver.normalizeResultOverColumns(
      [[1, null, 'x', 'extra']],
      [{ name: 'a', type: 'integer' }, { name: 'b', type: 'varchar' }, { name: 'c', type: 'varchar' }],
    );
    expect(result).toEqual([{ a: 1, b: null, c: 'x' }]);
    driver.release();
  });

  it('prefers pollBackoff over checkInterval when both are set', () => {
    const driver = createDriver({
      checkInterval: 800,
      pollBackoff: { initialInterval: 15, incrementStep: 0, maxInterval: 15, triesBeforeIncrement: 1 },
    });
    expect((driver as any).pollBackoff.initialInterval).toBe(15);
    expect((driver as any).drainInterval).toBe(0);
    driver.release();
  });

  it('uses the https agent when nextUri is https', async () => {
    const driver = createDriver({
      ssl: { rejectUnauthorized: false },
      pollBackoff: { initialInterval: 1, incrementStep: 1, maxInterval: 1, triesBeforeIncrement: 1 },
    });
    jest.spyOn(driver as any, 'sleep').mockResolvedValue(undefined);

    mockFetch.mockResolvedValueOnce(
      mockResponse({
        nextUri: 'https://worker.internal:8443/v1/statement/q/1',
        columns: [{ name: 'x', type: 'integer' }],
        stats: { state: 'RUNNING' },
      }),
    );
    mockFetch.mockResolvedValueOnce(
      mockResponse({ data: [[1]], stats: { state: 'FINISHED' } }),
    );

    await driver.queryPromised('SELECT 1', false);
    const pollOpts = mockFetch.mock.calls[1][1];
    expect(mockFetch.mock.calls[1][0]).toMatch(/^https:/);
    expect(pollOpts.agent).toBe((driver as any).httpsAgent);
    await driver.release();
  });
});
