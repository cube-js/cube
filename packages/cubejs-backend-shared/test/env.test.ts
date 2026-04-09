import { getEnv, convertTimeStrToSeconds, convertSizeToBytes } from '../src/env';

test('convertTimeStrToMs', () => {
  expect(convertTimeStrToSeconds('1')).toBe(1);
  expect(convertTimeStrToSeconds('1s')).toBe(1);
  expect(convertTimeStrToSeconds('5s')).toBe(5);
  expect(convertTimeStrToSeconds('1m')).toBe(1 * 60);
  expect(convertTimeStrToSeconds('10m')).toBe(10 * 60);
  expect(convertTimeStrToSeconds('1h')).toBe(60 * 60);
  expect(convertTimeStrToSeconds('2h')).toBe(2 * 60 * 60);
});

test('convertTimeStrToMs(exception)', () => {
  expect(() => convertTimeStrToSeconds('')).toThrowError(
    `Value "" is not valid. Must be a number in seconds or duration string (1s, 1m, 1h).`
  );
});

test('convertSizeToBytes', () => {
  expect(convertSizeToBytes('1024')).toBe(1024);
  expect(convertSizeToBytes('1kb')).toBe(1024);
  expect(convertSizeToBytes('10KB')).toBe(10 * 1024);
  expect(convertSizeToBytes('1mb')).toBe(1024 * 1024);
  expect(convertSizeToBytes('50MB')).toBe(50 * 1024 * 1024);
  expect(convertSizeToBytes('1gb')).toBe(1024 * 1024 * 1024);
  expect(convertSizeToBytes('2GB')).toBe(2 * 1024 * 1024 * 1024);
});

test('convertSizeToBytes(exception)', () => {
  expect(() => convertSizeToBytes('')).toThrowError(
    `Value "" is not valid. Must be a number in bytes or size string (1kb, 1mb, 1gb).`
  );
  expect(() => convertSizeToBytes('abc')).toThrowError(
    `Value "abc" is not valid. Must be a number in bytes or size string (1kb, 1mb, 1gb).`
  );
  expect(() => convertSizeToBytes('1tb')).toThrowError(
    `Value "1tb" is not valid. Must be a number in bytes or size string (1kb, 1mb, 1gb).`
  );
});

describe('getEnv', () => {
  test('port(exception)', () => {
    process.env.PORT = '100000000';

    expect(() => getEnv('port')).toThrowError(
      'Should be lower or equal than 65535.'
    );

    process.env.PORT = '-1000';

    expect(() => getEnv('port')).toThrowError(
      'Should be a positive integer.'
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
      'Should be boolean or number (in seconds) or string in time format (1s, 1m, 1h)'
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
      'Must be between 100kb and 64mb.'
    );

    process.env.CUBEJS_MAX_REQUEST_SIZE = '100mb';
    expect(() => getEnv('maxRequestSize')).toThrowError(
      'Must be between 100kb and 64mb.'
    );
  });
});
