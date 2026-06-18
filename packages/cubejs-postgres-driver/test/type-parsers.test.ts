import {
  dateTypeParser,
  timestampTypeParser,
  timestampTzTypeParser,
} from '../src/type-parsers';

describe('type parsers', () => {
  test('dateTypeParser (OID 1082)', () => {
    expect(dateTypeParser('2020-01-01')).toBe('2020-01-01T00:00:00.000');
    // Leap date
    expect(dateTypeParser('2020-02-29')).toBe('2020-02-29T00:00:00.000');
  });

  test('timestampTypeParser (OID 1114)', () => {
    // no fractional seconds
    expect(timestampTypeParser('2020-01-01 12:34:56')).toBe('2020-01-01T12:34:56.000');
    // millisecond precision
    expect(timestampTypeParser('2020-01-01 12:34:56.789')).toBe('2020-01-01T12:34:56.789');
    // microsecond precision is truncated to ms
    expect(timestampTypeParser('2020-01-01 12:34:56.123456')).toBe('2020-01-01T12:34:56.123');
    // sub-millisecond precision is padded
    expect(timestampTypeParser('2020-01-01 12:34:56.5')).toBe('2020-01-01T12:34:56.500');
    expect(timestampTypeParser('2020-01-01 12:34:56.05')).toBe('2020-01-01T12:34:56.050');
  });

  test('timestampTzTypeParser (OID 1184)', () => {
    // positive HH-only offset (matches integration assertion)
    expect(timestampTzTypeParser('2020-01-01 00:00:00+02')).toBe('2019-12-31T22:00:00.000');
    // zero offset — fast path (UTC session, every shape Postgres can emit)
    expect(timestampTzTypeParser('2020-01-01 00:00:00+00')).toBe('2020-01-01T00:00:00.000');
    expect(timestampTzTypeParser('2020-01-01 00:00:00-00')).toBe('2020-01-01T00:00:00.000');
    expect(timestampTzTypeParser('2020-01-01 00:00:00+00:00')).toBe('2020-01-01T00:00:00.000');
    expect(timestampTzTypeParser('2020-06-15 08:15:30.250+00')).toBe('2020-06-15T08:15:30.250');
    expect(timestampTzTypeParser('2020-06-15 08:15:30.123456+00')).toBe('2020-06-15T08:15:30.123');
    // negative HH-only offset
    expect(timestampTzTypeParser('2020-01-01 00:00:00-05')).toBe('2020-01-01T05:00:00.000');
    // HH:MM offset crossing day boundary
    expect(timestampTzTypeParser('2020-01-01 23:30:00+05:30')).toBe('2020-01-01T18:00:00.000');
    expect(timestampTzTypeParser('2020-01-01 00:00:00+05:30:15')).toBe('2019-12-31T18:29:45.000');
    // milliseconds plus HH:MM offset
    expect(timestampTzTypeParser('2020-06-15 08:15:30.250+05:45')).toBe('2020-06-15T02:30:30.250');
    // microseconds plus HH offset are truncated to ms
    expect(timestampTzTypeParser('2020-06-15 08:15:30.123456-03')).toBe('2020-06-15T11:15:30.123');
    // Years 100-999 take the fast Date.UTC path; pad4 preserves leading zero.
    expect(timestampTzTypeParser('0500-06-15 12:00:00+00')).toBe('0500-06-15T12:00:00.000');
    // Years 0-99 must NOT trigger Date.UTC's legacy "1900+year" remap
    // (moment parity: `0099-01-01 00:00:00+02` → `0098-12-31T22:00:00.000`,
    // not `1998-12-31T…`).
    expect(timestampTzTypeParser('0099-01-01 00:00:00+00')).toBe('0099-01-01T00:00:00.000');
    expect(timestampTzTypeParser('0099-01-01 00:00:00+02')).toBe('0098-12-31T22:00:00.000');
    expect(timestampTzTypeParser('0001-01-01 02:00:00+05:00')).toBe('0000-12-31T21:00:00.000');
    // Year boundary rollover (forward / backward)
    expect(timestampTzTypeParser('2020-12-31 23:30:00-01')).toBe('2021-01-01T00:30:00.000');
    expect(timestampTzTypeParser('2021-01-01 00:30:00+01')).toBe('2020-12-31T23:30:00.000');
    // Leap-year February edges
    expect(timestampTzTypeParser('2020-02-28 23:30:00-01')).toBe('2020-02-29T00:30:00.000'); // into Feb 29 (leap)
    expect(timestampTzTypeParser('2020-03-01 00:30:00+01')).toBe('2020-02-29T23:30:00.000'); // back to Feb 29
    expect(timestampTzTypeParser('2021-02-28 23:30:00-01')).toBe('2021-03-01T00:30:00.000'); // non-leap skips Feb 29
    // Centennial leap rule: 2000 IS a leap year, 1900 is NOT.
    expect(timestampTzTypeParser('2000-02-28 23:30:00-01')).toBe('2000-02-29T00:30:00.000');
    expect(timestampTzTypeParser('1900-02-28 23:30:00-01')).toBe('1900-03-01T00:30:00.000');
  });
});
