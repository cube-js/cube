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
    `Unsupported time format in VARIABLE_ENV`
  );
});

test('getEnv(dbPollTimeout)', () => {
  expect(getEnv('dbPollTimeout')).toBe(15 * 60);

  process.env.CUBEJS_DB_POLL_TIMEOUT = '1m';
  expect(getEnv('dbPollTimeout')).toBe(60);
});

test('getEnv(dbPollMaxInterval)', () => {
  expect(getEnv('dbPollMaxInterval')).toBe(5);

  process.env.CUBEJS_DB_POLL_MAX_INTERVAL = '10s';
  expect(getEnv('dbPollMaxInterval')).toBe(10);
});
