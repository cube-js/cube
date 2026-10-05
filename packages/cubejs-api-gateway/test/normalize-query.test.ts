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
  test.each(['default', 'compact', 'columnar'])(
    'accepts responseFormat=%s',
    (responseFormat) => {
      const result = normalizeQuery({ ...baseQuery, responseFormat: responseFormat as ResultType }, false);
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
