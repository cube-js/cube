import { localTimestampToUtc } from '../src';

/**
 * Reproduction for https://github.com/cube-js/cube/issues/11945
 *
 * `localTimestampToUtc` takes a local wall-clock string and returns the
 * corresponding UTC instant. Its fast path parses the string as if it were
 * already UTC, looks up the zone offset *at that artificial instant*, and
 * applies it:
 *
 *   const parsedTime = Date.parse(`${timestamp}Z`);
 *   const offset = zone.utcOffset(parsedTime);
 *
 * Near a DST transition the offset at the artificial UTC instant differs from
 * the offset that actually applies to the requested local time, so the result
 * is off by exactly one hour. None of the local times below are ambiguous —
 * each occurs exactly once — so this is not the "which 02:00 did you mean"
 * problem, it is a plain mis-conversion.
 *
 * The same helper backs `BaseQuery.inDbTimeZone` (query date filters) and
 * `PreAggregationPartitionRangeLoader.inDbTimeZone` (pre-aggregation partition
 * ranges), so the bad bounds can also be materialized into rollups.
 */
describe('localTimestampToUtc around DST transitions', () => {
  const format = 'YYYY-MM-DD[T]HH:mm:ss.SSS[Z]';

  describe('Europe/Amsterdam', () => {
    const timezone = 'Europe/Amsterdam';

    // Autumn 2026: the clock goes back at 03:00 local on 2026-10-25,
    // so local 01:00 that day is still CEST (UTC+2).
    it('converts the autumn transition-day 01:00 hour as CEST (UTC+2)', () => {
      expect(localTimestampToUtc(timezone, format, '2026-10-25T01:00:00.000'))
        .toBe('2026-10-24T23:00:00.000Z');
      expect(localTimestampToUtc(timezone, format, '2026-10-25T01:59:59.999'))
        .toBe('2026-10-24T23:59:59.999Z');
    });

    // Spring 2026: the clock goes forward at 02:00 local on 2026-03-29,
    // so local 01:00 that day is still CET (UTC+1).
    it('converts the spring transition-day 01:00 hour as CET (UTC+1)', () => {
      expect(localTimestampToUtc(timezone, format, '2026-03-29T01:00:00.000'))
        .toBe('2026-03-29T00:00:00.000Z');
      expect(localTimestampToUtc(timezone, format, '2026-03-29T01:59:59.999'))
        .toBe('2026-03-29T00:59:59.999Z');
    });

    it('is unaffected away from a transition', () => {
      expect(localTimestampToUtc(timezone, format, '2026-06-15T01:00:00.000'))
        .toBe('2026-06-14T23:00:00.000Z');
      expect(localTimestampToUtc(timezone, format, '2026-01-15T01:00:00.000'))
        .toBe('2026-01-15T00:00:00.000Z');
    });
  });

  // Every DST-observing zone is affected, for a window of local hours
  // proportional to the UTC offset on the transition date.
  describe('other daylight-saving zones', () => {
    it('converts the America/New_York spring transition hour correctly', () => {
      // Clocks go forward at 02:00 local on 2026-03-08; 03:00 local is EDT (UTC-4).
      expect(localTimestampToUtc('America/New_York', format, '2026-03-08T03:00:00.000'))
        .toBe('2026-03-08T07:00:00.000Z');
    });

    it('converts the Pacific/Auckland autumn transition hour correctly', () => {
      // Clocks go back at 03:00 local on 2026-04-05; 2026-04-04T14:00 is NZDT (UTC+13).
      expect(localTimestampToUtc('Pacific/Auckland', format, '2026-04-04T14:00:00.000'))
        .toBe('2026-04-04T01:00:00.000Z');
    });

    it('is unaffected in zones without daylight saving', () => {
      expect(localTimestampToUtc('Asia/Tokyo', format, '2026-03-29T01:00:00.000'))
        .toBe('2026-03-28T16:00:00.000Z');
      expect(localTimestampToUtc('UTC', format, '2026-03-29T01:00:00.000'))
        .toBe('2026-03-29T01:00:00.000Z');
    });
  });
});
