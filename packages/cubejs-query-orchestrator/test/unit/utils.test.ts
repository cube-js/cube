import { evaluateLocalRefreshKey, isValidLocalRefreshKey, snapToRenewalThreshold } from '../../src/orchestrator/utils';

describe('evaluateLocalRefreshKey', () => {
  const tenMinutes = { interval: 600, utcOffset: 0, dayOffset: 0 };

  test('returns the same row shape as SELECT FLOOR(...) as refresh_key', () => {
    expect(evaluateLocalRefreshKey(tenMinutes, 600_000)).toEqual([{ refresh_key: '1' }]);
    expect(evaluateLocalRefreshKey(tenMinutes, 6_000_000)).toEqual([{ refresh_key: '10' }]);
  });

  test('changes exactly at the interval boundary', () => {
    expect(evaluateLocalRefreshKey(tenMinutes, 599_999)).toEqual([{ refresh_key: '0' }]);
    expect(evaluateLocalRefreshKey(tenMinutes, 600_000)).toEqual([{ refresh_key: '1' }]);
    expect(evaluateLocalRefreshKey(tenMinutes, 1_199_999)).toEqual([{ refresh_key: '1' }]);
    expect(evaluateLocalRefreshKey(tenMinutes, 1_200_000)).toEqual([{ refresh_key: '2' }]);
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
      .toEqual([{ refresh_key: '0' }]);
  });

  test('applies dayOffset for cron based keys', () => {
    // every '0 10 * * *' => interval 1 day, dayOffset 10 hours
    const daily = { interval: 86400, utcOffset: 0, dayOffset: 36000, cron: true };

    // 09:59:59 UTC on the epoch day is still before the first fire time
    expect(evaluateLocalRefreshKey(daily, 35_999_000)).toEqual([{ refresh_key: '-1' }]);
    expect(evaluateLocalRefreshKey(daily, 36_000_000)).toEqual([{ refresh_key: '0' }]);
    expect(evaluateLocalRefreshKey(daily, 36_000_000 + 86_400_000)).toEqual([{ refresh_key: '1' }]);
  });

  test('defaults to the current clock', () => {
    const before = Math.floor(Date.now() / 1000 / tenMinutes.interval);
    const [{ refresh_key: value }] = evaluateLocalRefreshKey(tenMinutes);
    const after = Math.floor(Date.now() / 1000 / tenMinutes.interval);

    expect(typeof value).toBe('string');
    expect(Number(value)).toBeGreaterThanOrEqual(before);
    expect(Number(value)).toBeLessThanOrEqual(after);
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

describe('snapToRenewalThreshold', () => {
  const day = 24 * 60 * 60;

  test('leaves the clock alone when no threshold is configured', () => {
    for (const threshold of [undefined, 0, -60, NaN, Infinity]) {
      expect(snapToRenewalThreshold(1_234_567, threshold)).toBe(1_234_567);
    }
  });

  test('floors to the threshold boundary', () => {
    expect(snapToRenewalThreshold(0, 120)).toBe(0);
    expect(snapToRenewalThreshold(119_999, 120)).toBe(0);
    expect(snapToRenewalThreshold(120_000, 120)).toBe(120_000);
    expect(snapToRenewalThreshold(239_999, 120)).toBe(120_000);
  });

  test('never goes backwards', () => {
    let previous = -1;

    for (let ms = 0; ms < 600_000; ms += 17_000) {
      const snapped = snapToRenewalThreshold(ms, 120);

      expect(snapped).toBeGreaterThanOrEqual(previous);
      expect(snapped).toBeLessThanOrEqual(ms);
      previous = snapped;
    }
  });

  test('advances a 10 minute key once per daily threshold, staying in its own series', () => {
    const tenMinutes = { interval: 600, utcOffset: 0, dayOffset: 0 };
    const at = (ms: number) => evaluateLocalRefreshKey(tenMinutes, snapToRenewalThreshold(ms, day))[0].refresh_key;

    expect(at(0)).toBe('0');
    expect(at(600_000)).toBe('0');
    expect(at(86_399_000)).toBe('0');
    expect(at(86_400_000)).toBe('144');
    expect(at(86_400_000 + 600_000)).toBe('144');
    expect(at(2 * 86_400_000)).toBe('288');

    for (const ms of [0, 86_400_000, 2 * 86_400_000]) {
      expect(at(ms)).toBe(evaluateLocalRefreshKey(tenMinutes, ms)[0].refresh_key);
    }
  });

  test('keeps the numbering of a cron key', () => {
    // every '0 10 * * *' => interval 1 day, dayOffset 10 hours
    const daily = { interval: day, utcOffset: 0, dayOffset: 36_000 };
    const threshold = 4 * 60 * 60;
    const at = (ms: number) => evaluateLocalRefreshKey(daily, snapToRenewalThreshold(ms, threshold))[0].refresh_key;

    expect(at(36_000_000 - 1)).toBe('-1');
    // The fire at 10:00 is observed at the 12:00 sample, exactly as a 4 hour cache would have
    expect(at(36_000_000)).toBe('-1');
    expect(at(43_200_000)).toBe('0');
    expect(at(36_000_000 + 86_400_000)).toBe('0');
    expect(at(43_200_000 + 86_400_000)).toBe('1');
  });
});
