import { formatIsoFromMillis } from '../../src/Transform';

describe('formatIsoFromMillis', () => {
  const cases = [
    0, -1, 1, 999, 1000, -1000,
    Date.UTC(1960, 0, 2, 3, 4, 5, 123),
    Date.UTC(2020, 1, 29, 23, 59, 59, 999),
    Date.UTC(2000, 2, 1), Date.UTC(1900, 2, 1), Date.UTC(1600, 2, 1),
    Date.UTC(1970, 0, 1), Date.UTC(1969, 11, 31, 23, 59, 59, 999),
    -62167219200000, 253402300799999, // 0000-01-01 and 9999-12-31 bounds
    -62167219200001, 253402300800000, // just outside: Date's extended-year format
    8.64e15, -8.64e15, // Date range limits
  ];

  test.each(cases)('matches Date#toISOString for %d', (millis) => {
    expect(formatIsoFromMillis(millis)).toBe(new Date(millis).toISOString());
  });

  test('matches Date#toISOString on random values', () => {
    let seed = 42;
    const next = () => {
      seed = (seed * 1103515245 + 12345) % 2147483648;
      return seed / 2147483648;
    };

    for (let i = 0; i < 20000; i++) {
      const millis = Math.floor((next() - 0.5) * 2 * 253402300800000);
      expect(formatIsoFromMillis(millis)).toBe(new Date(millis).toISOString());
    }
  });

  test('throws like Date for values outside its range', () => {
    expect(() => formatIsoFromMillis(8.64e15 + 1)).toThrow(RangeError);
    expect(() => formatIsoFromMillis(NaN)).toThrow(RangeError);
  });
});
