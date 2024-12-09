import { getEnv, convertTimeStrToSeconds } from '../src/env';

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
});
