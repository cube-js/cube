import { buildTransform, formatIsoFromMillis } from '../../src/Transform';

describe('formatIsoFromMillis', () => {
  test.each([
    [0, '1970-01-01T00:00:00.000Z'],
    [Date.UTC(2020, 1, 29, 23, 59, 59, 999), '2020-02-29T23:59:59.999Z'],
    [Date.UTC(1960, 0, 2, 3, 4, 5, 123), '1960-01-02T03:04:05.123Z'],
    [-62167219200000, '0000-01-01T00:00:00.000Z'],
    [253402300799999, '9999-12-31T23:59:59.999Z'],
  ])('renders %d as an ISO timestamp', (millis, expected) => {
    expect(formatIsoFromMillis(millis)).toBe(expected);
  });

  test('throws like Date for values outside its range', () => {
    expect(() => formatIsoFromMillis(8.64e15 + 1)).toThrow(RangeError);
    expect(() => formatIsoFromMillis(NaN)).toThrow(RangeError);
  });
});

describe('buildTransform', () => {
  test('rejects a names/types length mismatch', () => {
    expect(() => buildTransform(['a', 'b'], [])).toThrow('names 2 vs types 0');
  });
});
