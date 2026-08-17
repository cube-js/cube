// eslint-disable-next-line import/no-extraneous-dependencies
import {
  normalizeQuery,
  normalizeQueryPreAggregations,
  normalizeQueryPreAggregationPreview,
  normalizeTimezone,
} from '../src/query';

const baseQuery = {
  measures: ['Foo.count'],
  timezone: 'UTC',
};

describe('responseFormat validation', () => {
  test.each(['default', 'compact', 'columnar'])(
    'accepts responseFormat=%s',
    (responseFormat) => {
      const result = normalizeQuery({ ...baseQuery, responseFormat }, false);
      expect(result.responseFormat).toBe(responseFormat);
    }
  );

  test('rejects unknown responseFormat', () => {
    expect(() => normalizeQuery({ ...baseQuery, responseFormat: 'arrow' }, false)).toThrow(/Invalid query format/);
  });
});

describe('timezone validation', () => {
  test.each(['UTC', 'America/New_York', 'Europe/Berlin', 'Asia/Tokyo'])(
    'accepts valid IANA timezone %s',
    (tz) => {
      const result = normalizeQuery({ ...baseQuery, timezone: tz }, false);
      expect(result.timezone).toBe(tz);
    }
  );

  test.each([
    ['america/new_york', 'America/New_York'],
    ['AMERICA/NEW_YORK', 'America/New_York'],
    ['utc', 'UTC'],
    ['uTc', 'UTC'],
  ])('accepts timezone case-insensitively and normalizes it: %s -> %s', (tz, expected) => {
    const result = normalizeQuery({ ...baseQuery, timezone: tz }, false);
    expect(result.timezone).toBe(expected);
  });

  test.each([
    'Not/AZone',
    '+05:00',
    'foo/bar',
  ])('rejects invalid timezone %j', (tz) => {
    expect(() => normalizeQuery({ ...baseQuery, timezone: tz }, false)).toThrow(/Invalid query format/);
  });

  describe('default timezone fallback', () => {
    afterEach(() => {
      delete process.env.CUBEJS_DEFAULT_TIMEZONE;
    });

    test('falls back to UTC when CUBEJS_DEFAULT_TIMEZONE is unset', () => {
      delete process.env.CUBEJS_DEFAULT_TIMEZONE;

      const { timezone, ...queryWithoutTimezone } = baseQuery;
      const result = normalizeQuery(queryWithoutTimezone, false);
      expect(result.timezone).toBe('UTC');
    });

    test('uses the canonicalized CUBEJS_DEFAULT_TIMEZONE when set', () => {
      process.env.CUBEJS_DEFAULT_TIMEZONE = 'america/new_york';

      const { timezone, ...queryWithoutTimezone } = baseQuery;
      const result = normalizeQuery(queryWithoutTimezone, false);
      expect(result.timezone).toBe('America/New_York');
    });
  });
});

describe('normalizeQueryPreAggregations timezone handling', () => {
  test('normalizes timezone to canonical IANA name', () => {
    const result = normalizeQueryPreAggregations({ timezone: 'america/new_york' }, undefined);
    expect(result.timezones).toEqual(['America/New_York']);
  });

  test('normalizes timezones array to canonical IANA names', () => {
    const result = normalizeQueryPreAggregations({ timezones: ['utc', 'europe/berlin'] }, undefined);
    expect(result.timezones).toEqual(['UTC', 'Europe/Berlin']);
  });

  test('rejects invalid timezone', () => {
    expect(() => normalizeQueryPreAggregations({ timezones: ['Not/AZone'] }, undefined)).toThrow(/Invalid query format/);
  });
});

describe('normalizeQueryPreAggregationPreview timezone handling', () => {
  const previewQuery = {
    preAggregationId: 'cube.preAgg',
    versionEntry: { content_version: 'a', structure_version: 'b' },
  };

  test('normalizes timezone to canonical IANA name', () => {
    const result = normalizeQueryPreAggregationPreview({ ...previewQuery, timezone: 'america/new_york' });
    expect(result.timezone).toBe('America/New_York');
  });

  test('rejects invalid timezone', () => {
    expect(() => normalizeQueryPreAggregationPreview({ ...previewQuery, timezone: 'Not/AZone' })).toThrow(/Invalid query format/);
  });
});

describe('normalizeTimezone helper', () => {
  test.each([
    ['america/new_york', 'America/New_York'],
    ['UTC', 'UTC'],
    ['uTc', 'UTC'],
  ])('normalizes %j -> %j', (tz, expected) => {
    expect(normalizeTimezone(tz)).toBe(expected);
  });

  test.each([undefined, null, ''])('passes through empty value %j', (tz) => {
    expect(normalizeTimezone(tz as any)).toBe(tz);
  });

  test.each([
    'Not/AZone',
    'foo/bar',
  ])('throws on invalid timezone %j', (tz) => {
    expect(() => normalizeTimezone(tz)).toThrow(/valid IANA time zone/);
  });
});
