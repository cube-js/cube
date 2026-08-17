import { canonicalTimezone } from '../src/timezone';

describe('canonicalTimezone', () => {
  test.each([
    ['UTC', 'UTC'],
    ['utc', 'UTC'],
    ['uTc', 'UTC'],
    ['America/New_York', 'America/New_York'],
    ['america/new_york', 'America/New_York'],
    ['AMERICA/NEW_YORK', 'America/New_York'],
    ['Etc/GMT+5', 'Etc/GMT+5'],
    ['GMT', 'GMT'],
    ['EST', 'EST'],
  ])('resolves %j to the canonical name %j', (value, expected) => {
    expect(canonicalTimezone(value)).toBe(expected);
  });

  // Pinned deliberately: resolving links to their target would change query cache keys and
  // pre-agg partition names for deployments already using them.
  test.each([
    ['US/Pacific', 'US/Pacific'],
    ['Asia/Calcutta', 'Asia/Calcutta'],
    ['Europe/Kiev', 'Europe/Kiev'],
  ])('keeps link name %j as-is', (value, expected) => {
    expect(canonicalTimezone(value)).toBe(expected);
  });

  test.each([
    'Not/AZone',
    'Europ/Berlin',
    'foo/bar',
    // Offsets are the SQL injection surface.
    '+05:00',
    '+05',
    '05',
    '',
    // No trimming: API payloads must be exact.
    ' UTC',
    'UTC ',
    '  UTC  ',
  ])('returns null for invalid value %j', (value) => {
    expect(canonicalTimezone(value)).toBeNull();
  });

  // The casts are the point: real callers are plain JS, so the signature is not enforced and
  // moment.tz.zone() would throw a TypeError instead of returning null.
  test.each([
    undefined,
    null,
    123,
    0,
    {},
    [],
    true,
  ])('returns null without throwing for non-string %j', (value) => {
    expect(() => canonicalTimezone(value as any)).not.toThrow();
    expect(canonicalTimezone(value as any)).toBeNull();
  });
});
