// eslint-disable-next-line import/no-extraneous-dependencies
import {
  cubeSqlRequestSchema,
  normalizeQuery,
  normalizeQueryPreAggregations,
  normalizeQueryPreAggregationPreview,
  normalizeQueryCancelPreAggregations,
} from '../src/query';
import { ResultType } from '../src/types/enums';

const baseQuery = {
  measures: ['Foo.count'],
  timezone: 'UTC',
};

describe('responseFormat validation', () => {
  test.each(Object.values(ResultType))(
    'accepts responseFormat=%s',
    (responseFormat) => {
      const result = normalizeQuery({ ...baseQuery, responseFormat }, false);
      expect(result.responseFormat).toBe(responseFormat);
    }
  );

  test('rejects unknown responseFormat', () => {
    expect(() => normalizeQuery({ ...baseQuery, responseFormat: 'arrow' as any }, false)).toThrow(/Invalid query format/);
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

      const { timezone: _timezone, ...queryWithoutTimezone } = baseQuery;
      const result = normalizeQuery(queryWithoutTimezone, false);
      expect(result.timezone).toBe('UTC');
    });

    test('uses the canonicalized CUBEJS_DEFAULT_TIMEZONE when set', () => {
      process.env.CUBEJS_DEFAULT_TIMEZONE = 'america/new_york';

      const { timezone: _timezone, ...queryWithoutTimezone } = baseQuery;
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

describe('normalizeQueryPreAggregations', () => {
  test('passes validated fields through', () => {
    const query = {
      metadata: { foo: 'bar' },
      expand: ['partitions'],
      preAggregations: [{
        id: 'cube.preAgg',
        cacheOnly: true,
        metaOnly: false,
        partitions: ['cube_pre_agg_20240101'],
        refreshRange: ['2024-01-01', '2024-01-31'],
      }],
    };

    expect(normalizeQueryPreAggregations(query)).toEqual({
      ...query,
      timezones: ['UTC'],
    });
  });

  test('prefers timezones over timezone', () => {
    const result = normalizeQueryPreAggregations({ timezone: 'Europe/Berlin', timezones: ['America/New_York'] });
    expect(result.timezones).toEqual(['America/New_York']);
  });

  test('falls back to default timezones', () => {
    expect(normalizeQueryPreAggregations({}, { timezones: ['Asia/Tokyo'] }).timezones).toEqual(['Asia/Tokyo']);
    expect(normalizeQueryPreAggregations({}, {}).timezones).toEqual(['UTC']);
  });

  test('returns Joi-converted values instead of the raw input', () => {
    const result = normalizeQueryPreAggregations({
      preAggregations: [{ id: 'cube.preAgg', cacheOnly: 'false', metaOnly: 'true' }],
    });
    expect(result.preAggregations).toEqual([{ id: 'cube.preAgg', cacheOnly: false, metaOnly: true }]);
  });

  test('drops the timezone key from the result', () => {
    expect(normalizeQueryPreAggregations({ timezone: 'UTC' })).not.toHaveProperty('timezone');
  });

  test.each([
    ['unknown top-level key', { nope: 1 }],
    ['unknown pre-aggregation key', { preAggregations: [{ id: 'cube.preAgg', nope: 1 }] }],
    ['missing pre-aggregation id', { preAggregations: [{ cacheOnly: true }] }],
    ['refreshRange of wrong length', { preAggregations: [{ id: 'cube.preAgg', refreshRange: ['2024-01-01'] }] }],
    ['non-boolean cacheOnly', { preAggregations: [{ id: 'cube.preAgg', cacheOnly: 'yes' }] }],
    ['non-object metadata', { metadata: 'foo' }],
    ['non-string expand item', { expand: [1] }],
  ])('rejects %s', (_, query) => {
    expect(() => normalizeQueryPreAggregations(query)).toThrow(/Invalid query format/);
  });
});

describe('normalizeQueryPreAggregationPreview', () => {
  const previewQuery = {
    preAggregationId: 'cube.preAgg',
    timezone: 'UTC',
    versionEntry: {
      content_version: 'a',
      last_updated_at: 1704067200000,
      naming_version: 2,
      structure_version: 'b',
      table_name: 'cube_pre_agg_abc',
      build_range_end: '2024-01-31T23:59:59.999',
    },
  };

  test('passes validated fields through', () => {
    expect(normalizeQueryPreAggregationPreview(previewQuery)).toEqual(previewQuery);
  });

  test('returns Joi-converted values instead of the raw input', () => {
    const result = normalizeQueryPreAggregationPreview({
      ...previewQuery,
      versionEntry: { ...previewQuery.versionEntry, last_updated_at: '1704067200000', naming_version: '2' },
    });
    expect(result.versionEntry.last_updated_at).toBe(1704067200000);
    expect(result.versionEntry.naming_version).toBe(2);
  });

  test.each([
    ['missing preAggregationId', { timezone: 'UTC', versionEntry: {} }],
    ['missing timezone', { preAggregationId: 'cube.preAgg', versionEntry: {} }],
    ['missing versionEntry', { preAggregationId: 'cube.preAgg', timezone: 'UTC' }],
    ['unknown top-level key', { ...previewQuery, nope: 1 }],
    ['unknown versionEntry key', { ...previewQuery, versionEntry: { nope: 1 } }],
    ['non-numeric last_updated_at', { ...previewQuery, versionEntry: { last_updated_at: 'yesterday' } }],
  ])('rejects %s', (_, query) => {
    expect(() => normalizeQueryPreAggregationPreview(query)).toThrow(/Invalid query format/);
  });
});

describe('normalizeQueryCancelPreAggregations', () => {
  test('passes validated fields through', () => {
    const query = { dataSource: 'default', queryKeys: ['key1', 'key2'] };
    expect(normalizeQueryCancelPreAggregations(query)).toEqual(query);
  });

  test('accepts an empty query', () => {
    expect(normalizeQueryCancelPreAggregations({})).toEqual({});
  });

  test.each([
    ['unknown key', { nope: 1 }],
    ['non-string dataSource', { dataSource: 1 }],
    ['non-array queryKeys', { queryKeys: 'key1' }],
    ['non-string queryKeys item', { queryKeys: [1] }],
  ])('rejects %s', (_, query) => {
    expect(() => normalizeQueryCancelPreAggregations(query)).toThrow(/Invalid query format/);
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

describe('member paths through joins', () => {
  // Stands in for the data model: `orders.customer` is a join alias.
  const splitGranularity = (path: string) => {
    const known: Record<string, { dimension: string, granularity: string } | null> = {
      'orders.customer.city': null,
      'orders.created_at.day': { dimension: 'orders.created_at', granularity: 'day' },
      'orders.customer.created_at.month': { dimension: 'orders.customer.created_at', granularity: 'month' },
    };
    return path in known ? known[path] : undefined;
  };

  test('accepts members of every kind named through an alias', () => {
    const result = normalizeQuery({
      measures: ['orders.customer.count'],
      dimensions: ['orders.customer.city'],
      segments: ['orders.customer.berliners'],
      timeDimensions: [{ dimension: 'orders.customer.created_at', granularity: 'day' }],
      filters: [{ member: 'orders.manager.city', operator: 'equals', values: ['Paris'] }],
      order: [['orders.customer.city', 'asc']],
      timezone: 'UTC',
    }, false, undefined, splitGranularity);
    expect(result.dimensions).toEqual(['orders.customer.city']);
    expect(result.timeDimensions).toEqual([
      expect.objectContaining({ dimension: 'orders.customer.created_at', granularity: 'day' }),
    ]);
  });

  test('keeps a three-segment path through an alias a dimension', () => {
    const result = normalizeQuery({
      measures: [],
      dimensions: ['orders.customer.city', 'orders.created_at.day', 'orders.customer.created_at.month'],
      timezone: 'UTC',
    }, false, undefined, splitGranularity);
    expect(result.dimensions).toEqual(['orders.customer.city']);
    expect(result.timeDimensions).toEqual([
      expect.objectContaining({ dimension: 'orders.created_at', granularity: 'day' }),
      expect.objectContaining({ dimension: 'orders.customer.created_at', granularity: 'month' }),
    ]);
  });

  test('reads an unknown three-segment dimension as a granularity', () => {
    const result = normalizeQuery({
      measures: [],
      dimensions: ['Foo.time.day'],
      timezone: 'UTC',
    }, false, undefined, splitGranularity);
    expect(result.dimensions).toEqual([]);
    expect(result.timeDimensions).toEqual([
      expect.objectContaining({ dimension: 'Foo.time', granularity: 'day' }),
    ]);
  });
});

// https://github.com/cube-js/cube/issues/34
describe('timeDimensions.dateRange validation', () => {
  const queryWithDateRange = (dateRange: any) => ({
    ...baseQuery,
    timeDimensions: [{ dimension: 'Foo.time', dateRange }],
  });

  const resolve = (dateRange: any): string[] => normalizeQuery(queryWithDateRange(dateRange), false).timeDimensions?.[0].dateRange as string[];

  test.each([
    [['2020-13-45', '2020-01-01'], /Invalid date "2020-13-45" in date range/],
    [['2020-02-30', '2020-03-01'], /Invalid date "2020-02-30" in date range/],
    [['foo', '2020-01-01'], /Invalid date "foo" in date range/],
    [['2020-01-01', 'bar'], /Invalid date "bar" in date range/],
    [['not-a-date'], /Invalid date "not-a-date" in date range/],
  ])('rejects invalid date in %j', (dateRange, message) => {
    expect(() => resolve(dateRange)).toThrow(message);
  });

  test('rejects a date range whose start is after its end', () => {
    expect(() => resolve(['2020-02-01', '2020-01-01']))
      .toThrow('Invalid date range ["2020-02-01","2020-01-01"]: start date "2020-02-01" is after end date "2020-01-01"');
  });

  test('rejects a reversed relative date range', () => {
    expect(() => resolve('from now to 7 days ago')).toThrow(/is after end date/);
  });

  test.each([
    [[], /must contain at least 1 items/],
    [['2020-01-01', '2020-01-02', '2020-01-03'], /must contain less than or equal to 2 items/],
    ['foo', /Can't parse date: 'foo'/],
    ['last blah', /Can't parse date: 'last blah'/],
  ])('keeps rejecting malformed date range %j', (dateRange, message) => {
    expect(() => resolve(dateRange)).toThrow(message);
  });

  test('accepts a single-day range given as the same date twice', () => {
    expect(resolve(['2020-01-01', '2020-01-01'])).toEqual(['2020-01-01T00:00:00.000', '2020-01-01T23:59:59.999']);
  });

  test('accepts a single-element array as a one-day range', () => {
    expect(resolve(['2020-01-01'])).toEqual(['2020-01-01T00:00:00.000', '2020-01-01T23:59:59.999']);
  });

  test('accepts timestamps with time and timezone offset', () => {
    expect(resolve(['2020-01-01T00:00:00.000Z', '2020-01-02T10:00:00+05:30']))
      .toEqual(['2020-01-01T00:00:00.000', '2020-01-02T04:30:00.000']);
  });

  test.each([
    'last 7 days',
    'from 7 days ago to now',
    'today',
    'this month',
  ])('accepts relative date range %j', (dateRange) => {
    const [start, end] = resolve(dateRange);
    expect(start <= end).toBe(true);
  });
});
