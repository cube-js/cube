import { getEnv, convertTimeStrToSeconds, convertSizeToBytes } from '../src/env';

test('convertTimeStrToMs', () => {
  expect(convertTimeStrToSeconds('1', 'VARIABLE_ENV')).toBe(1);
  expect(convertTimeStrToSeconds('1s', 'VARIABLE_ENV')).toBe(1);
  expect(convertTimeStrToSeconds('5s', 'VARIABLE_ENV')).toBe(5);
  expect(convertTimeStrToSeconds('1m', 'VARIABLE_ENV')).toBe(1 * 60);
  expect(convertTimeStrToSeconds('10m', 'VARIABLE_ENV')).toBe(10 * 60);
  expect(convertTimeStrToSeconds('1h', 'VARIABLE_ENV')).toBe(60 * 60);
  expect(convertTimeStrToSeconds('2h', 'VARIABLE_ENV')).toBe(2 * 60 * 60);
});

test('convertTimeStrToMs(exception)', () => {
  expect(() => convertTimeStrToSeconds('', 'VARIABLE_ENV')).toThrowError(
    `Value "" is not valid for VARIABLE_ENV. Must be a number in seconds or duration string (1s, 1m, 1h).`
  );
});

test('convertSizeToBytes', () => {
  expect(convertSizeToBytes('1024', 'VARIABLE_ENV')).toBe(1024);
  expect(convertSizeToBytes('1kb', 'VARIABLE_ENV')).toBe(1024);
  expect(convertSizeToBytes('10KB', 'VARIABLE_ENV')).toBe(10 * 1024);
  expect(convertSizeToBytes('1mb', 'VARIABLE_ENV')).toBe(1024 * 1024);
  expect(convertSizeToBytes('50MB', 'VARIABLE_ENV')).toBe(50 * 1024 * 1024);
  expect(convertSizeToBytes('1gb', 'VARIABLE_ENV')).toBe(1024 * 1024 * 1024);
  expect(convertSizeToBytes('2GB', 'VARIABLE_ENV')).toBe(2 * 1024 * 1024 * 1024);
});

test('convertSizeToBytes(exception)', () => {
  expect(() => convertSizeToBytes('', 'VARIABLE_ENV')).toThrowError(
    `Value "" is not valid for VARIABLE_ENV. Must be a number in bytes or size string (1kb, 1mb, 1gb).`
  );
  expect(() => convertSizeToBytes('abc', 'VARIABLE_ENV')).toThrowError(
    `Value "abc" is not valid for VARIABLE_ENV. Must be a number in bytes or size string (1kb, 1mb, 1gb).`
  );
  expect(() => convertSizeToBytes('1tb', 'VARIABLE_ENV')).toThrowError(
    `Value "1tb" is not valid for VARIABLE_ENV. Must be a number in bytes or size string (1kb, 1mb, 1gb).`
  );
});

describe('getEnv', () => {
  test('port(exception)', () => {
    process.env.PORT = '100000000';

    expect(() => getEnv('port')).toThrowError(
      'Value "100000000" is not valid for PORT. Should be lower or equal than 65535.'
    );

    process.env.PORT = '-1000';

    expect(() => getEnv('port')).toThrowError(
      'Value "-1000" is not valid for PORT. Should be a positive integer.'
    );
  });

  test('refreshWorkerMode (from refreshTimer)', () => {
    process.env.NODE_ENV = 'production';
    delete process.env.CUBEJS_SCHEDULED_REFRESH_TIMER;
    expect(getEnv('refreshWorkerMode')).toBe(false);

    process.env.NODE_ENV = 'development';
    delete process.env.CUBEJS_SCHEDULED_REFRESH_TIMER;
    expect(getEnv('refreshWorkerMode')).toBe(true);

    process.env.CUBEJS_SCHEDULED_REFRESH_TIMER = '60';
    expect(getEnv('refreshWorkerMode')).toBe(60);

    process.env.CUBEJS_SCHEDULED_REFRESH_TIMER = '1m';
    expect(getEnv('refreshWorkerMode')).toBe(60);

    process.env.CUBEJS_SCHEDULED_REFRESH_TIMER = 'true';
    expect(getEnv('refreshWorkerMode')).toBe(true);

    process.env.CUBEJS_SCHEDULED_REFRESH_TIMER = 'false';
    expect(getEnv('refreshWorkerMode')).toBe(false);

    process.env.CUBEJS_SCHEDULED_REFRESH_TIMER = 'True';
    expect(getEnv('refreshWorkerMode')).toBe(true);

    process.env.CUBEJS_SCHEDULED_REFRESH_TIMER = 'False';
    expect(getEnv('refreshWorkerMode')).toBe(false);
  });

  test('refreshWorkerMode(exception)', () => {
    process.env.CUBEJS_SCHEDULED_REFRESH_TIMER = '11fffffff';

    expect(() => getEnv('refreshWorkerMode')).toThrowError(
      'Value "11fffffff" is not valid for CUBEJS_SCHEDULED_REFRESH_TIMER. Should be boolean or number (in seconds) or string in time format (1s, 1m, 1h)'
    );
  });

  test('dbPollTimeout', () => {
    process.env.CUBEJS_DB_POLL_TIMEOUT = '1m';
    expect(
      getEnv('dbPollTimeout', { dataSource: 'default' })
    ).toBe(60);
  });

  test('dbPollMaxInterval', () => {
    expect(
      getEnv('dbPollMaxInterval', { dataSource: 'default' })
    ).toBe(5);

    process.env.CUBEJS_DB_POLL_MAX_INTERVAL = '10s';
    expect(
      getEnv('dbPollMaxInterval', { dataSource: 'default' })
    ).toBe(10);
  });

  test('livePreview', () => {
    expect(getEnv('livePreview')).toBe(true);

    process.env.CUBEJS_LIVE_PREVIEW = 'true';
    expect(getEnv('livePreview')).toBe(true);

    process.env.CUBEJS_LIVE_PREVIEW = 'false';
    expect(getEnv('livePreview')).toBe(false);
  });

  test('maxRequestSize', () => {
    delete process.env.CUBEJS_MAX_REQUEST_SIZE;
    expect(getEnv('maxRequestSize')).toBe(50 * 1024 * 1024); // default 50mb

    process.env.CUBEJS_MAX_REQUEST_SIZE = '64mb';
    expect(getEnv('maxRequestSize')).toBe(64 * 1024 * 1024);

    process.env.CUBEJS_MAX_REQUEST_SIZE = '100kb';
    expect(getEnv('maxRequestSize')).toBe(100 * 1024);

    process.env.CUBEJS_MAX_REQUEST_SIZE = '512kb';
    expect(getEnv('maxRequestSize')).toBe(512 * 1024);
  });

  test('maxRequestSize(exception)', () => {
    process.env.CUBEJS_MAX_REQUEST_SIZE = '50kb';
    expect(() => getEnv('maxRequestSize')).toThrowError(
      'Value "50kb" is not valid for CUBEJS_MAX_REQUEST_SIZE. Must be between 100kb and 64mb.'
    );

    process.env.CUBEJS_MAX_REQUEST_SIZE = '100mb';
    expect(() => getEnv('maxRequestSize')).toThrowError(
      'Value "100mb" is not valid for CUBEJS_MAX_REQUEST_SIZE. Must be between 100kb and 64mb.'
    );
  });
});

describe('getEnv(apiSecret / apiSecrets)', () => {
  afterEach(() => {
    delete process.env.CUBEJS_API_SECRET;
    delete process.env.CUBEJS_API_SECRETS;
  });

  test('apiSecret', () => {
    expect(getEnv('apiSecret')).toBeUndefined();

    process.env.CUBEJS_API_SECRET = 'secret';
    expect(getEnv('apiSecret')).toBe('secret');
  });

  test('apiSecrets - unset / empty / blanks resolve to undefined', () => {
    expect(getEnv('apiSecrets')).toBeUndefined();

    process.env.CUBEJS_API_SECRETS = '';
    expect(getEnv('apiSecrets')).toBeUndefined();

    process.env.CUBEJS_API_SECRETS = ',  ,,';
    expect(getEnv('apiSecrets')).toBeUndefined();
  });

  test('apiSecrets - trims, drops empties, deduplicates, preserves order', () => {
    process.env.CUBEJS_API_SECRETS = ' a , b , c ';
    expect(getEnv('apiSecrets')).toEqual(['a', 'b', 'c']);

    process.env.CUBEJS_API_SECRETS = 'a,b,a,c,b';
    expect(getEnv('apiSecrets')).toEqual(['a', 'b', 'c']);

    process.env.CUBEJS_API_SECRETS = 'only';
    expect(getEnv('apiSecrets')).toEqual(['only']);
  });
});

const TRINO_POLL_ENV = {
  trinoPollInitialInterval: 'CUBEJS_DB_TRINO_POLL_INITIAL_INTERVAL',
  trinoPollIncrementStep: 'CUBEJS_DB_TRINO_POLL_INCREMENT_STEP',
  trinoPollMaxInterval: 'CUBEJS_DB_TRINO_POLL_MAX_INTERVAL',
  trinoPollTriesBeforeIncrement: 'CUBEJS_DB_TRINO_POLL_TRIES_BEFORE_INCREMENT',
} as const;

describe('getEnv(trino poll / source)', () => {
  const opts = { dataSource: 'default' as const };

  afterEach(() => {
    delete process.env.CUBEJS_DB_TRINO_SOURCE;
    Object.values(TRINO_POLL_ENV).forEach((key) => {
      delete process.env[key];
    });
  });

  test('defaults', () => {
    expect(getEnv('trinoSource', opts)).toBeUndefined();
    expect(getEnv('trinoPollInitialInterval', opts)).toBe(50);
    expect(getEnv('trinoPollIncrementStep', opts)).toBe(50);
    expect(getEnv('trinoPollMaxInterval', opts)).toBe(500);
    expect(getEnv('trinoPollTriesBeforeIncrement', opts)).toBe(10);
  });

  test('valid overrides', () => {
    process.env.CUBEJS_DB_TRINO_SOURCE = 'my-app';
    process.env.CUBEJS_DB_TRINO_POLL_INITIAL_INTERVAL = '25';
    process.env.CUBEJS_DB_TRINO_POLL_INCREMENT_STEP = '0';
    process.env.CUBEJS_DB_TRINO_POLL_MAX_INTERVAL = '1000';
    process.env.CUBEJS_DB_TRINO_POLL_TRIES_BEFORE_INCREMENT = '3';

    expect(getEnv('trinoSource', opts)).toBe('my-app');
    expect(getEnv('trinoPollInitialInterval', opts)).toBe(25);
    expect(getEnv('trinoPollIncrementStep', opts)).toBe(0);
    expect(getEnv('trinoPollMaxInterval', opts)).toBe(1000);
    expect(getEnv('trinoPollTriesBeforeIncrement', opts)).toBe(3);
  });

  test('empty source is empty string (driver falls back to nodejs-client)', () => {
    process.env.CUBEJS_DB_TRINO_SOURCE = '';
    expect(getEnv('trinoSource', opts)).toBe('');
  });

  test.each(Object.entries(TRINO_POLL_ENV))(
    '%s rejects improper values',
    (getter, envKey) => {
      const read = () => getEnv(getter as keyof typeof TRINO_POLL_ENV, opts);

      process.env[envKey] = '-1';
      expect(read).toThrow(/Must be an integer >= /);

      process.env[envKey] = 'abc';
      expect(read).toThrow(/Must be an integer >= /);

      process.env[envKey] = '';
      expect(read).toThrow(/Must be an integer >= /);

      process.env[envKey] = '1.5';
      expect(read).toThrow(/Must be an integer >= /);

      process.env[envKey] = '50ms';
      expect(read).toThrow(/Must be an integer >= /);

      process.env[envKey] = '1e2';
      expect(read).toThrow(/Must be an integer >= /);
    }
  );

  test('triesBeforeIncrement rejects 0', () => {
    process.env.CUBEJS_DB_TRINO_POLL_TRIES_BEFORE_INCREMENT = '0';
    expect(() => getEnv('trinoPollTriesBeforeIncrement', opts)).toThrow(
      /Must be an integer >= 1/
    );
  });

  test('interval 0 is allowed (poll immediately)', () => {
    process.env.CUBEJS_DB_TRINO_POLL_INITIAL_INTERVAL = '0';
    expect(getEnv('trinoPollInitialInterval', opts)).toBe(0);
  });
});
