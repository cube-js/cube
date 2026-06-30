import { getEnv, keyByDataSource, hasPreAggregationsEnvVars } from '../src/env';

// Clean up any leftover datasources config
delete process.env.CUBEJS_DATASOURCES;

describe('Pre-aggregation env vars (single datasource)', () => {
  afterEach(() => {
    delete process.env.CUBEJS_DB_TYPE;
    delete process.env.CUBEJS_PRE_AGGREGATIONS_DB_TYPE;
    delete process.env.CUBEJS_DB_HOST;
    delete process.env.CUBEJS_PRE_AGGREGATIONS_DB_HOST;
    delete process.env.CUBEJS_DB_USER;
    delete process.env.CUBEJS_PRE_AGGREGATIONS_DB_USER;
    delete process.env.CUBEJS_DB_PASS;
    delete process.env.CUBEJS_PRE_AGGREGATIONS_DB_PASS;
    delete process.env.CUBEJS_DB_SSL;
    delete process.env.CUBEJS_PRE_AGGREGATIONS_DB_SSL;
    delete process.env.CUBEJS_DB_PORT;
    delete process.env.CUBEJS_PRE_AGGREGATIONS_DB_PORT;
  });

  test('preAggregations: true reads PRE_AGGREGATIONS variant', () => {
    process.env.CUBEJS_DB_HOST = 'regular-host';
    process.env.CUBEJS_PRE_AGGREGATIONS_DB_HOST = 'preagg-host';

    expect(getEnv('dbHost', { dataSource: 'default', preAggregations: true }))
      .toEqual('preagg-host');
    expect(getEnv('dbHost', { dataSource: 'default', preAggregations: false }))
      .toEqual('regular-host');
    expect(getEnv('dbHost', { dataSource: 'default' }))
      .toEqual('regular-host');
  });

  test('preAggregations: true returns undefined when PRE_AGGREGATIONS variant not set', () => {
    process.env.CUBEJS_DB_HOST = 'regular-host';

    expect(getEnv('dbHost', { dataSource: 'default', preAggregations: true }))
      .toBeUndefined();
  });

  test('preAggregations: false ignores PRE_AGGREGATIONS variant even when set', () => {
    process.env.CUBEJS_DB_HOST = 'regular-host';
    process.env.CUBEJS_PRE_AGGREGATIONS_DB_HOST = 'preagg-host';

    expect(getEnv('dbHost', { dataSource: 'default', preAggregations: false }))
      .toEqual('regular-host');
  });

  test('works with dbUser/dbPass', () => {
    process.env.CUBEJS_DB_USER = 'regular-user';
    process.env.CUBEJS_PRE_AGGREGATIONS_DB_USER = 'preagg-user';
    process.env.CUBEJS_DB_PASS = 'regular-pass';
    process.env.CUBEJS_PRE_AGGREGATIONS_DB_PASS = 'preagg-pass';

    expect(getEnv('dbUser', { dataSource: 'default', preAggregations: true }))
      .toEqual('preagg-user');
    expect(getEnv('dbPass', { dataSource: 'default', preAggregations: true }))
      .toEqual('preagg-pass');
    expect(getEnv('dbUser', { dataSource: 'default' }))
      .toEqual('regular-user');
    expect(getEnv('dbPass', { dataSource: 'default' }))
      .toEqual('regular-pass');
  });

  test('works with dbSsl (boolean parsing)', () => {
    process.env.CUBEJS_DB_SSL = 'false';
    process.env.CUBEJS_PRE_AGGREGATIONS_DB_SSL = 'true';

    expect(getEnv('dbSsl', { dataSource: 'default', preAggregations: true }))
      .toEqual(true);
    expect(getEnv('dbSsl', { dataSource: 'default' }))
      .toEqual(false);
  });

  test('works with dbPort (int parsing)', () => {
    process.env.CUBEJS_DB_PORT = '5432';
    process.env.CUBEJS_PRE_AGGREGATIONS_DB_PORT = '5433';

    expect(getEnv('dbPort', { dataSource: 'default', preAggregations: true }))
      .toEqual(5433);
    expect(getEnv('dbPort', { dataSource: 'default' }))
      .toEqual(5432);
  });

  test('keyByDataSource with preAggregations flag', () => {
    expect(keyByDataSource('CUBEJS_DB_HOST', 'default', true))
      .toEqual('CUBEJS_PRE_AGGREGATIONS_DB_HOST');
    expect(keyByDataSource('CUBEJS_DB_HOST', 'default', false))
      .toEqual('CUBEJS_DB_HOST');
    expect(keyByDataSource('CUBEJS_DB_HOST', 'default'))
      .toEqual('CUBEJS_DB_HOST');
  });
});

describe('Pre-aggregation env vars (multi datasource)', () => {
  beforeEach(() => {
    process.env.CUBEJS_DATASOURCES = 'default,analytics';
  });

  afterEach(() => {
    delete process.env.CUBEJS_DATASOURCES;
    delete process.env.CUBEJS_DB_HOST;
    delete process.env.CUBEJS_PRE_AGGREGATIONS_DB_HOST;
    delete process.env.CUBEJS_DS_ANALYTICS_DB_HOST;
    delete process.env.CUBEJS_DS_ANALYTICS_PRE_AGGREGATIONS_DB_HOST;
    delete process.env.CUBEJS_DB_TYPE;
    delete process.env.CUBEJS_DS_ANALYTICS_DB_TYPE;
    delete process.env.CUBEJS_DS_ANALYTICS_PRE_AGGREGATIONS_DB_TYPE;
  });

  test('multi-datasource: PRE_AGGREGATIONS variant for named datasource', () => {
    process.env.CUBEJS_DS_ANALYTICS_DB_HOST = 'analytics-host';
    process.env.CUBEJS_DS_ANALYTICS_PRE_AGGREGATIONS_DB_HOST = 'analytics-preagg-host';

    expect(getEnv('dbHost', { dataSource: 'analytics', preAggregations: true }))
      .toEqual('analytics-preagg-host');
    expect(getEnv('dbHost', { dataSource: 'analytics' }))
      .toEqual('analytics-host');
  });

  test('multi-datasource: returns undefined when PRE_AGGREGATIONS variant not set', () => {
    process.env.CUBEJS_DS_ANALYTICS_DB_HOST = 'analytics-host';

    expect(getEnv('dbHost', { dataSource: 'analytics', preAggregations: true }))
      .toBeUndefined();
  });

  test('multi-datasource: default datasource uses CUBEJS_PRE_AGGREGATIONS prefix', () => {
    process.env.CUBEJS_DB_HOST = 'default-host';
    process.env.CUBEJS_PRE_AGGREGATIONS_DB_HOST = 'default-preagg-host';

    expect(getEnv('dbHost', { dataSource: 'default', preAggregations: true }))
      .toEqual('default-preagg-host');
    expect(getEnv('dbHost', { dataSource: 'default' }))
      .toEqual('default-host');
  });


  test('keyByDataSource with preAggregations for named datasource', () => {
    expect(keyByDataSource('CUBEJS_DB_HOST', 'analytics', true))
      .toEqual('CUBEJS_DS_ANALYTICS_PRE_AGGREGATIONS_DB_HOST');
    expect(keyByDataSource('CUBEJS_DB_HOST', 'analytics'))
      .toEqual('CUBEJS_DS_ANALYTICS_DB_HOST');
  });
});

describe('hasPreAggregationsEnvVars', () => {
  afterEach(() => {
    delete process.env.CUBEJS_PRE_AGGREGATIONS_DB_HOST;
    delete process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA;
    delete process.env.CUBEJS_PRE_AGGREGATIONS_BUILDER;
    delete process.env.CUBEJS_PRE_AGGREGATIONS_BACKOFF_MAX_TIME;
    delete process.env.CUBEJS_PRE_AGGREGATIONS_ALLOW_NON_STRICT_DATE_RANGE_MATCH;
    delete process.env.CUBEJS_DS_ANALYTICS_PRE_AGGREGATIONS_DB_HOST;
    delete process.env.CUBEJS_DATASOURCES;
  });

  test('returns false when no PRE_AGGREGATIONS vars set', () => {
    expect(hasPreAggregationsEnvVars('default')).toBe(false);
  });

  test('returns true when a PRE_AGGREGATIONS var is set for default', () => {
    process.env.CUBEJS_PRE_AGGREGATIONS_DB_HOST = 'some-host';
    expect(hasPreAggregationsEnvVars('default')).toBe(true);
  });

  test('returns true when a PRE_AGGREGATIONS var is set for named datasource', () => {
    process.env.CUBEJS_DS_ANALYTICS_PRE_AGGREGATIONS_DB_HOST = 'some-host';
    expect(hasPreAggregationsEnvVars('analytics')).toBe(true);
  });

  test('ignores CUBEJS_PRE_AGGREGATIONS_SCHEMA', () => {
    process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA = 'my_preaggs';
    expect(hasPreAggregationsEnvVars('default')).toBe(false);
  });

  test('ignores CUBEJS_PRE_AGGREGATIONS_BUILDER', () => {
    process.env.CUBEJS_PRE_AGGREGATIONS_BUILDER = 'true';
    expect(hasPreAggregationsEnvVars('default')).toBe(false);
  });

  test('returns false when only non-credential PRE_AGGREGATIONS vars are set', () => {
    process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA = 'my_preaggs';
    process.env.CUBEJS_PRE_AGGREGATIONS_BUILDER = 'true';
    process.env.CUBEJS_PRE_AGGREGATIONS_BACKOFF_MAX_TIME = '600';
    process.env.CUBEJS_PRE_AGGREGATIONS_ALLOW_NON_STRICT_DATE_RANGE_MATCH = 'true';
    expect(hasPreAggregationsEnvVars('default')).toBe(false);
  });

  test('returns true when credential var is set alongside non-credential vars', () => {
    process.env.CUBEJS_PRE_AGGREGATIONS_BUILDER = 'true';
    process.env.CUBEJS_PRE_AGGREGATIONS_DB_HOST = 'some-host';
    expect(hasPreAggregationsEnvVars('default')).toBe(true);
  });

  test('returns false for non-matching datasource', () => {
    process.env.CUBEJS_DS_ANALYTICS_PRE_AGGREGATIONS_DB_HOST = 'some-host';
    expect(hasPreAggregationsEnvVars('default')).toBe(false);
    expect(hasPreAggregationsEnvVars('other')).toBe(false);
  });
});
