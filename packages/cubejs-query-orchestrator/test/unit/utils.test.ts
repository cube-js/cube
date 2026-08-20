import { evaluateLocalRefreshKey, isValidLocalRefreshKey } from '../../src/orchestrator/utils';

describe('evaluateLocalRefreshKey', () => {
  const tenMinutes = { interval: 600, utcOffset: 0, dayOffset: 0 };

  test('returns the same row shape as SELECT FLOOR(...) as refresh_key', () => {
    // 600_000ms == 600s == exactly one 10 minute bucket
    expect(evaluateLocalRefreshKey(tenMinutes, 600_000)).toEqual([{ refresh_key: 1 }]);
    expect(evaluateLocalRefreshKey(tenMinutes, 6_000_000)).toEqual([{ refresh_key: 10 }]);
  });

  test('changes exactly at the interval boundary', () => {
    expect(evaluateLocalRefreshKey(tenMinutes, 599_999)).toEqual([{ refresh_key: 0 }]);
    expect(evaluateLocalRefreshKey(tenMinutes, 600_000)).toEqual([{ refresh_key: 1 }]);
    expect(evaluateLocalRefreshKey(tenMinutes, 1_199_999)).toEqual([{ refresh_key: 1 }]);
    expect(evaluateLocalRefreshKey(tenMinutes, 1_200_000)).toEqual([{ refresh_key: 2 }]);
  });

  test('sub-second precision never changes the result', () => {
    for (const ms of [0, 1, 250, 500, 999]) {
      expect(evaluateLocalRefreshKey(tenMinutes, 600_000 + ms)).toEqual(
        evaluateLocalRefreshKey(tenMinutes, 600_000)
      );
    }
  });

  test('applies a negative utcOffset', () => {
    // America/Los_Angeles in PST: -8 hours
    expect(evaluateLocalRefreshKey({ interval: 3600, utcOffset: -28800, dayOffset: 0 }, 28_800_000))
      .toEqual([{ refresh_key: 0 }]);
  });

  test('applies dayOffset for cron based keys', () => {
    // every '0 10 * * *' => interval 1 day, dayOffset 10 hours
    const daily = { interval: 86400, utcOffset: 0, dayOffset: 36000, cron: true };

    // 09:59:59 UTC on the epoch day is still before the first fire time
    expect(evaluateLocalRefreshKey(daily, 35_999_000)).toEqual([{ refresh_key: -1 }]);
    expect(evaluateLocalRefreshKey(daily, 36_000_000)).toEqual([{ refresh_key: 0 }]);
    // and the day after
    expect(evaluateLocalRefreshKey(daily, 36_000_000 + 86_400_000)).toEqual([{ refresh_key: 1 }]);
  });

  test('defaults to the current clock', () => {
    const before = Math.floor(Date.now() / 1000 / tenMinutes.interval);
    const [{ refresh_key: value }] = evaluateLocalRefreshKey(tenMinutes);
    const after = Math.floor(Date.now() / 1000 / tenMinutes.interval);

    expect(value).toBeGreaterThanOrEqual(before);
    expect(value).toBeLessThanOrEqual(after);
  });
});

describe('isValidLocalRefreshKey', () => {
  test('accepts a well formed descriptor', () => {
    expect(isValidLocalRefreshKey({ interval: 600, utcOffset: 0, dayOffset: 0 })).toBe(true);
    expect(isValidLocalRefreshKey({ interval: 1, utcOffset: -28800, dayOffset: 36000 })).toBe(true);
  });

  test('rejects anything that would produce a garbage key', () => {
    expect(isValidLocalRefreshKey(undefined)).toBe(false);
    expect(isValidLocalRefreshKey({ interval: 0, utcOffset: 0, dayOffset: 0 })).toBe(false);
    expect(isValidLocalRefreshKey({ interval: -600, utcOffset: 0, dayOffset: 0 })).toBe(false);
    expect(isValidLocalRefreshKey({ interval: NaN, utcOffset: 0, dayOffset: 0 })).toBe(false);
    expect(isValidLocalRefreshKey({ interval: Infinity, utcOffset: 0, dayOffset: 0 })).toBe(false);
    expect(isValidLocalRefreshKey({ interval: 600, utcOffset: NaN, dayOffset: 0 })).toBe(false);
    expect(isValidLocalRefreshKey({ interval: 600, utcOffset: 0, dayOffset: NaN })).toBe(false);
  });
});
