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

  test('refreshKeyLocalTime', () => {
    delete process.env.CUBEJS_REFRESH_KEY_LOCAL_TIME;
    expect(getEnv('refreshKeyLocalTime')).toBe(false);

    process.env.CUBEJS_REFRESH_KEY_LOCAL_TIME = 'true';
    expect(getEnv('refreshKeyLocalTime')).toBe(true);

    process.env.CUBEJS_REFRESH_KEY_LOCAL_TIME = 'false';
    expect(getEnv('refreshKeyLocalTime')).toBe(false);
  });

  test('refreshKeyLocalTime(exception)', () => {
    process.env.CUBEJS_REFRESH_KEY_LOCAL_TIME = 'yes';

    expect(() => getEnv('refreshKeyLocalTime')).toThrowError();

    delete process.env.CUBEJS_REFRESH_KEY_LOCAL_TIME;
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

describe('getEnv(defaultTimezone / scheduledRefreshTimezones)', () => {
  afterEach(() => {
    delete process.env.CUBEJS_DEFAULT_TIMEZONE;
    delete process.env.CUBEJS_SCHEDULED_REFRESH_TIMEZONES;
  });

  // env-var's .default() does not fire for a present but blank value, and it does not trim.
  test('defaultTimezone - unset / blank / padded resolve to UTC', () => {
    delete process.env.CUBEJS_DEFAULT_TIMEZONE;
    expect(getEnv('defaultTimezone')).toBe('UTC');

    process.env.CUBEJS_DEFAULT_TIMEZONE = '';
    expect(getEnv('defaultTimezone')).toBe('UTC');

    process.env.CUBEJS_DEFAULT_TIMEZONE = '   ';
    expect(getEnv('defaultTimezone')).toBe('UTC');

    process.env.CUBEJS_DEFAULT_TIMEZONE = ' UTC ';
    expect(getEnv('defaultTimezone')).toBe('UTC');
  });

  test('defaultTimezone - normalizes to the canonical IANA name', () => {
    process.env.CUBEJS_DEFAULT_TIMEZONE = 'America/Los_Angeles';
    expect(getEnv('defaultTimezone')).toBe('America/Los_Angeles');

    process.env.CUBEJS_DEFAULT_TIMEZONE = 'america/los_angeles';
    expect(getEnv('defaultTimezone')).toBe('America/Los_Angeles');

    process.env.CUBEJS_DEFAULT_TIMEZONE = 'utc';
    expect(getEnv('defaultTimezone')).toBe('UTC');
  });

  test('defaultTimezone(exception)', () => {
    process.env.CUBEJS_DEFAULT_TIMEZONE = 'Europ/Berlin';
    expect(() => getEnv('defaultTimezone')).toThrowError(
      'Value "Europ/Berlin" is not valid for CUBEJS_DEFAULT_TIMEZONE. Must be a valid IANA time zone name, e.g. UTC or America/Los_Angeles.'
    );

    process.env.CUBEJS_DEFAULT_TIMEZONE = '+05:00';
    expect(() => getEnv('defaultTimezone')).toThrowError(
      'Value "+05:00" is not valid for CUBEJS_DEFAULT_TIMEZONE. Must be a valid IANA time zone name, e.g. UTC or America/Los_Angeles.'
    );
  });

  test('scheduledRefreshTimezones - unset resolves to an empty list', () => {
    delete process.env.CUBEJS_SCHEDULED_REFRESH_TIMEZONES;
    expect(getEnv('scheduledRefreshTimezones')).toEqual([]);
  });

  test('scheduledRefreshTimezones - trims and normalizes each entry', () => {
    process.env.CUBEJS_SCHEDULED_REFRESH_TIMEZONES = 'utc, europe/berlin';
    expect(getEnv('scheduledRefreshTimezones')).toEqual(['UTC', 'Europe/Berlin']);

    process.env.CUBEJS_SCHEDULED_REFRESH_TIMEZONES = 'America/New_York';
    expect(getEnv('scheduledRefreshTimezones')).toEqual(['America/New_York']);
  });

  // Trailing/repeated separators are common in .env files and compose YAML.
  test('scheduledRefreshTimezones - ignores empty entries', () => {
    process.env.CUBEJS_SCHEDULED_REFRESH_TIMEZONES = 'UTC,';
    expect(getEnv('scheduledRefreshTimezones')).toEqual(['UTC']);

    process.env.CUBEJS_SCHEDULED_REFRESH_TIMEZONES = 'UTC,,America/Los_Angeles';
    expect(getEnv('scheduledRefreshTimezones')).toEqual(['UTC', 'America/Los_Angeles']);

    process.env.CUBEJS_SCHEDULED_REFRESH_TIMEZONES = ' UTC , ';
    expect(getEnv('scheduledRefreshTimezones')).toEqual(['UTC']);
  });

  test('scheduledRefreshTimezones - blank resolves to an empty list', () => {
    process.env.CUBEJS_SCHEDULED_REFRESH_TIMEZONES = '';
    expect(getEnv('scheduledRefreshTimezones')).toEqual([]);

    process.env.CUBEJS_SCHEDULED_REFRESH_TIMEZONES = '   ';
    expect(getEnv('scheduledRefreshTimezones')).toEqual([]);

    process.env.CUBEJS_SCHEDULED_REFRESH_TIMEZONES = ',';
    expect(getEnv('scheduledRefreshTimezones')).toEqual([]);
  });

  test('scheduledRefreshTimezones(exception)', () => {
    process.env.CUBEJS_SCHEDULED_REFRESH_TIMEZONES = 'UTC,Nope/Zone';
    expect(() => getEnv('scheduledRefreshTimezones')).toThrowError(
      'Value "Nope/Zone" is not valid for CUBEJS_SCHEDULED_REFRESH_TIMEZONES. Must be a comma-separated list of valid IANA time zone names, e.g. UTC,America/Los_Angeles.'
    );
  });
});

describe('getEnv(compilerCacheSize)', () => {
  afterEach(() => {
    delete process.env.CUBEJS_COMPILER_CACHE_SIZE;
  });

  test('defaults to 250', () => {
    expect(getEnv('compilerCacheSize')).toBe(250);
  });

  test('reads CUBEJS_COMPILER_CACHE_SIZE', () => {
    process.env.CUBEJS_COMPILER_CACHE_SIZE = '1000';
    expect(getEnv('compilerCacheSize')).toBe(1000);
  });

  test('throws on zero, so it is never silently coerced to the default', () => {
    process.env.CUBEJS_COMPILER_CACHE_SIZE = '0';
    expect(() => getEnv('compilerCacheSize')).toThrowError(
      'Value "0" is not valid for CUBEJS_COMPILER_CACHE_SIZE. Must be a positive integer. The compiler cache can not be disabled.'
    );
  });

  test.each([
    '-1',
    'abc',
    '1.5',
  ])('throws on the negative or non-integer value %j', (value) => {
    process.env.CUBEJS_COMPILER_CACHE_SIZE = value;
    expect(() => getEnv('compilerCacheSize')).toThrowError(
      /CUBEJS_COMPILER_CACHE_SIZE/
    );
  });
});

const restoreNodeEnv = (value: string | undefined) => {
  if (value === undefined) {
    delete process.env.NODE_ENV;
  } else {
    process.env.NODE_ENV = value;
  }
};

describe('getEnv(devMode)', () => {
  const nodeEnv = process.env.NODE_ENV;

  beforeEach(() => {
    delete process.env.CUBEJS_DEV_MODE;
    delete process.env.NODE_ENV;
  });

  afterAll(() => {
    delete process.env.CUBEJS_DEV_MODE;
    restoreNodeEnv(nodeEnv);
  });

  test('is off when neither CUBEJS_DEV_MODE nor NODE_ENV is set', () => {
    expect(getEnv('devMode')).toBe(false);
  });

  test('ignores NODE_ENV, which is deprecated for this decision', () => {
    process.env.NODE_ENV = 'development';
    expect(getEnv('devMode')).toBe(false);

    process.env.NODE_ENV = 'test';
    expect(getEnv('devMode')).toBe(false);

    process.env.NODE_ENV = 'production';
    expect(getEnv('devMode')).toBe(false);
  });

  test('follows CUBEJS_DEV_MODE whatever NODE_ENV says', () => {
    process.env.CUBEJS_DEV_MODE = 'true';
    expect(getEnv('devMode')).toBe(true);

    process.env.NODE_ENV = 'production';
    expect(getEnv('devMode')).toBe(true);

    process.env.CUBEJS_DEV_MODE = 'false';
    expect(getEnv('devMode')).toBe(false);

    process.env.NODE_ENV = 'development';
    expect(getEnv('devMode')).toBe(false);
  });
});

describe('pinPreAggregationsSchema', () => {
  const saved = process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA;
  let logSpy: jest.SpyInstance;

  // Both the pin and displayCLIWarningOnce latch for the life of the module registry,
  // so each case needs a fresh one
  beforeEach(() => {
    jest.resetModules();
    delete process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA;
    logSpy = jest.spyOn(console, 'log').mockImplementation(() => {
      // swallow
    });
  });

  afterEach(() => {
    logSpy.mockRestore();

    if (saved === undefined) {
      delete process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA;
    } else {
      process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA = saved;
    }
  });

  const pinWarnings = () => logSpy.mock.calls
    .map(([message]) => String(message))
    .filter((message) => message.includes('already pinned'));

  // eslint-disable-next-line global-require
  const freshEnv = () => require('../src/env');

  test('warns when a second instance needs a different schema', () => {
    const env = freshEnv();

    env.pinPreAggregationsSchema('dev_pre_aggregations');
    env.pinPreAggregationsSchema('prod_pre_aggregations');

    // One variable cannot answer for two instances and a driver reads it directly, so
    // the second instance's driver is on the first's schema whatever is done here
    expect(process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA).toEqual('dev_pre_aggregations');
    expect(pinWarnings()).toHaveLength(1);
    expect(pinWarnings()[0]).toContain('prod_pre_aggregations');
  });

  test('stays quiet when the second instance needs the same schema', () => {
    const env = freshEnv();

    env.pinPreAggregationsSchema('prod_pre_aggregations');
    env.pinPreAggregationsSchema('prod_pre_aggregations');

    expect(pinWarnings()).toHaveLength(0);
  });

  test('stays quiet when the user set the variable, and does not overwrite it', () => {
    process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA = 'my_schema';

    const env = freshEnv();

    env.pinPreAggregationsSchema('prod_pre_aggregations');

    // The user chose one schema for the whole process, which is not a conflict
    expect(process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA).toEqual('my_schema');
    expect(pinWarnings()).toHaveLength(0);
    expect(env.userPreAggregationsSchema()).toEqual('my_schema');
  });
});

describe('the NODE_ENV deprecation warning', () => {
  const nodeEnv = process.env.NODE_ENV;
  let logSpy: jest.SpyInstance;

  // The warning is printed at most once per process, so each case needs a fresh
  // module registry to reset displayCLIWarningOnce's bookkeeping
  beforeEach(() => {
    jest.resetModules();
    delete process.env.CUBEJS_DEV_MODE;
    delete process.env.NODE_ENV;
    logSpy = jest.spyOn(console, 'log').mockImplementation(() => {
      // swallow
    });
  });

  afterEach(() => {
    logSpy.mockRestore();
    delete process.env.CUBEJS_DEV_MODE;
    restoreNodeEnv(nodeEnv);
  });

  const nodeEnvWarnings = () => logSpy.mock.calls
    .map(([message]) => String(message))
    .filter((message) => message.includes('NODE_ENV'));

  // eslint-disable-next-line global-require
  const freshGetEnv = () => require('../src/env').getEnv;

  test('is printed once when NODE_ENV is non-production and CUBEJS_DEV_MODE is unset', () => {
    process.env.NODE_ENV = 'development';

    const getEnvFresh = freshGetEnv();
    expect(getEnvFresh('devMode')).toBe(false);
    expect(getEnvFresh('devMode')).toBe(false);

    expect(nodeEnvWarnings()).toHaveLength(1);
    expect(nodeEnvWarnings()[0]).toContain('no longer taken into account');
  });

  test('does not tell an instance that wants development mode off to switch it on', () => {
    process.env.NODE_ENV = 'staging';

    expect(freshGetEnv()('devMode')).toBe(false);

    expect(nodeEnvWarnings()[0]).toContain('otherwise no action is needed');
  });

  test('is suppressed once CUBEJS_DEV_MODE is set, whatever its value', () => {
    process.env.NODE_ENV = 'development';
    process.env.CUBEJS_DEV_MODE = 'false';

    expect(freshGetEnv()('devMode')).toBe(false);

    expect(nodeEnvWarnings()).toHaveLength(0);
  });

  test('is not printed for NODE_ENV=production', () => {
    process.env.NODE_ENV = 'production';

    expect(freshGetEnv()('devMode')).toBe(false);

    expect(nodeEnvWarnings()).toHaveLength(0);
  });

  // An unset NODE_ENV used to mean development mode, so this instance is one the
  // change affects and must not be left without a signal
  test('is printed when NODE_ENV is unset, the case this change flips', () => {
    expect(freshGetEnv()('devMode')).toBe(false);

    expect(nodeEnvWarnings()).toHaveLength(1);
    expect(nodeEnvWarnings()[0]).toContain('including when NODE_ENV was unset');
  });

  // Both dev server paths leave CUBEJS_DEV_MODE unset, and the warning fires on any
  // non-production NODE_ENV including none at all — so without this it would greet
  // every `cubejs dev-server` run telling a dev server to enable development mode
  test('is silenced by markDevModeResolvedByCaller', () => {
    process.env.NODE_ENV = 'development';

    // eslint-disable-next-line global-require
    const env = require('../src/env');
    env.markDevModeResolvedByCaller();

    expect(env.getEnv('devMode')).toBe(false);

    expect(nodeEnvWarnings()).toHaveLength(0);
  });

  test('is not silenced for a process that never called it', () => {
    process.env.NODE_ENV = 'development';

    // A fresh registry, so the latch the case above set cannot leak into this one
    expect(freshGetEnv()('devMode')).toBe(false);

    expect(nodeEnvWarnings()).toHaveLength(1);
  });
});
