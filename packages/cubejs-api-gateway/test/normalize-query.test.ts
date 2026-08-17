// eslint-disable-next-line import/no-extraneous-dependencies
import { normalizeQuery } from '../src/query';

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
    'Not/AZone',
    '+05:00',
  ])('rejects invalid/injection timezone %j', (tz) => {
    expect(() => normalizeQuery({ ...baseQuery, timezone: tz }, false)).toThrow(/Invalid query format/);
  });
});
