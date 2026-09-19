// eslint-disable-next-line import/no-extraneous-dependencies
import {
  cubeSqlRequestSchema,
  normalizeQuery,
  normalizeQueryPreAggregations,
  normalizeQueryPreAggregationPreview,
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

describe('cubeSqlRequestSchema', () => {
  const baseBody = { query: 'SELECT 1' };

  test('accepts a body with only the query', () => {
    const { error, value } = cubeSqlRequestSchema.validate(baseBody);
    expect(error).toBeUndefined();
    expect(value).toEqual(baseBody);
  });

  test('accepts every supported field', () => {
    const { error, value } = cubeSqlRequestSchema.validate({
      ...baseBody,
      timezone: 'America/Los_Angeles',
      cache: 'stale-while-revalidate',
      throwContinueWait: true,
    });
    expect(error).toBeUndefined();
    expect(value.cache).toBe('stale-while-revalidate');
    expect(value.throwContinueWait).toBe(true);
  });

  test('requires the query', () => {
    expect(cubeSqlRequestSchema.validate({}).error?.message).toMatch(/"query" is required/);
  });

  test('rejects an unknown field', () => {
    expect(cubeSqlRequestSchema.validate({ ...baseBody, nope: 1 }).error).toBeDefined();
  });

  test('rejects an unknown cache mode', () => {
    expect(cubeSqlRequestSchema.validate({ ...baseBody, cache: 'sometimes' }).error).toBeDefined();
  });

  test.each([
    ['america/new_york', 'America/New_York'],
    ['uTc', 'UTC'],
  ])('normalizes timezone %j -> %j', (tz, expected) => {
    const { error, value } = cubeSqlRequestSchema.validate({ ...baseBody, timezone: tz });
    expect(error).toBeUndefined();
    expect(value.timezone).toBe(expected);
  });

  test.each([
    'Not/AZone',
    'foo/bar',
  ])('rejects invalid timezone %j', (tz) => {
    expect(cubeSqlRequestSchema.validate({ ...baseBody, timezone: tz }).error?.message)
      .toMatch(/valid IANA time zone/);
  });

  test.each([null, '', 123, true])('rejects timezone %j', (tz) => {
    expect(cubeSqlRequestSchema.validate({ ...baseBody, timezone: tz }).error).toBeDefined();
  });
});

describe('limit normalization', () => {
  test('keeps an explicit limit of 0 instead of applying the default limit', () => {
    const result = normalizeQuery({ ...baseQuery, limit: 0 }, false);
    expect(result.limit).toBe(0);
  });

  test('applies the default limit when no limit is given', () => {
    const result = normalizeQuery({ ...baseQuery }, false);
    expect(result.limit).toBeGreaterThan(0);
  });
});
