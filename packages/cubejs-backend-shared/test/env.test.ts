import { getEnv, convertTimeStrToMs } from '../src/env';

test('convertTimeStrToMs', () => {
  expect(convertTimeStrToMs('1', 'VARIABLE_ENV')).toBe(1);
  expect(convertTimeStrToMs('1s', 'VARIABLE_ENV')).toBe(1);
  expect(convertTimeStrToMs('5s', 'VARIABLE_ENV')).toBe(5);
  expect(convertTimeStrToMs('1m', 'VARIABLE_ENV')).toBe(1 * 60);
  expect(convertTimeStrToMs('10m', 'VARIABLE_ENV')).toBe(10 * 60);
  expect(convertTimeStrToMs('1h', 'VARIABLE_ENV')).toBe(60 * 60);
  expect(convertTimeStrToMs('2h', 'VARIABLE_ENV')).toBe(2 * 60 * 60);
});

test('convertTimeStrToMs(exception)', () => {
  expect(() => convertTimeStrToMs('', 'VARIABLE_ENV')).toThrowError(
    `VARIABLE_ENV is a time, must be number (in seconds) or string in time format (1s, 1m, 1h)`
  );
});

describe('getEnv', () => {
  test('port(exception)', () => {
    process.env.PORT = '100000000';

    expect(() => getEnv('port')).toThrowError(
      'PORT is a port number, should be lower or equal than 65535'
    );

    process.env.PORT = '-1000';

    expect(() => getEnv('port')).toThrowError(
      'PORT is a port number, should be a positive integer'
    );
  });

  test('refreshTimer', () => {
    expect(getEnv('refreshTimer')).toBe(undefined);

    process.env.CUBEJS_SCHEDULED_REFRESH_TIMER = '60';
    expect(getEnv('refreshTimer')).toBe(60);

    process.env.CUBEJS_SCHEDULED_REFRESH_TIMER = '1m';
    expect(getEnv('refreshTimer')).toBe(60);

    process.env.CUBEJS_SCHEDULED_REFRESH_TIMER = 'true';
    expect(getEnv('refreshTimer')).toBe(true);

    process.env.CUBEJS_SCHEDULED_REFRESH_TIMER = 'false';
    expect(getEnv('refreshTimer')).toBe(false);
  });

  test('refreshTimer(exception)', () => {
    process.env.CUBEJS_SCHEDULED_REFRESH_TIMER = '11fffffff';

    expect(() => getEnv('refreshTimer')).toThrowError(
      'CUBEJS_SCHEDULED_REFRESH_TIMER is not valid, must be boolean or number (in seconds) or string in time format (1s, 1m, 1h)'
    );
  });

  test('dbPollTimeout', () => {
    expect(getEnv('dbPollTimeout')).toBe(15 * 60);

    process.env.CUBEJS_DB_POLL_TIMEOUT = '1m';
    expect(getEnv('dbPollTimeout')).toBe(60);
  });

  test('dbPollMaxInterval', () => {
    expect(getEnv('dbPollMaxInterval')).toBe(5);

    process.env.CUBEJS_DB_POLL_MAX_INTERVAL = '10s';
    expect(getEnv('dbPollMaxInterval')).toBe(10);
  });
});
