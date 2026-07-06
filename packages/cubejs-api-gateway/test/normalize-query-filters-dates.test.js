/* globals describe,test,expect,jest,beforeEach,afterEach */

import { normalizeQuery, normalizeDateFilterValues, resolveDateRange } from '../src/query';

// Tests for filter-leaf date-range resolution at the gateway. This mirrors
// what `timeDimensions.dateRange` has always done: a single relative string
// (e.g. "last 2 weeks") resolves to an absolute [start, end] pair before the
// schema compiler sees it. The point is reuse — same resolver, same output —
// so an OR group with a relative date filter behaves identically to a query
// using top-level timeDimensions.

const baseQuery = {
  measures: ['Orders.count'],
  timezone: 'UTC',
};

const FIXED_NOW = new Date(Date.UTC(2026, 5, 25, 13, 0, 0, 0));

describe('normalizeDateFilterValues', () => {
  beforeEach(() => {
    jest.spyOn(Date, 'now').mockReturnValue(FIXED_NOW.getTime());
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  test('inDateRange with relative string resolves to absolute [start, end] pair', () => {
    // Why: this is the core invariant — relative strings must be resolved
    // before reaching BaseFilter, which has no relative-date logic of its own.
    const result = normalizeDateFilterValues(
      { member: 'Orders.createdAt', operator: 'inDateRange', values: ['last 2 weeks'] },
      'UTC'
    );

    expect(result.values).toHaveLength(2);
    expect(result.values[0]).toMatch(/^\d{4}-\d{2}-\d{2}T00:00:00\.000$/);
    expect(result.values[1]).toMatch(/^\d{4}-\d{2}-\d{2}T23:59:59\.999$/);
  });

  test('notInDateRange resolves the same way as inDateRange', () => {
    // Why: both range operators must accept relative strings — a user
    // filtering "NOT in last 2 weeks" expects the same boundary handling.
    const result = normalizeDateFilterValues(
      { member: 'Orders.createdAt', operator: 'notInDateRange', values: ['last 2 weeks'] },
      'UTC'
    );

    expect(result.values).toHaveLength(2);
  });

  test('inDateRange with absolute two-element array is passed through unchanged', () => {
    // Why: absolute date pairs are already in the form BaseFilter expects.
    // Mutating them would risk timezone drift or silent reformatting that
    // differs from today's behavior for callers passing absolute ranges.
    const filter = {
      member: 'Orders.createdAt',
      operator: 'inDateRange',
      values: ['2026-01-01', '2026-01-31'],
    };

    const result = normalizeDateFilterValues(filter, 'UTC');

    expect(result.values).toEqual(['2026-01-01', '2026-01-31']);
  });

  test('beforeDate with relative string resolves to start-of-day', () => {
    // Why: mirrors Tesseract's date_single.rs — `beforeDate` compares
    // `< startOfDay`. Using end-of-day here would include the whole day
    // instead of excluding it.
    const result = normalizeDateFilterValues(
      { member: 'Orders.createdAt', operator: 'beforeDate', values: ['yesterday'] },
      'UTC'
    );

    expect(result.values).toHaveLength(1);
    expect(result.values[0]).toMatch(/^\d{4}-\d{2}-\d{2}T00:00:00\.000$/);
  });

  test('afterOrOnDate with relative string resolves to start-of-day', () => {
    // Why: mirrors Tesseract's `>= startOfDay` — the operator's inclusive
    // lower boundary must land at the start of the target day.
    const result = normalizeDateFilterValues(
      { member: 'Orders.createdAt', operator: 'afterOrOnDate', values: ['yesterday'] },
      'UTC'
    );

    expect(result.values).toHaveLength(1);
    expect(result.values[0]).toMatch(/^\d{4}-\d{2}-\d{2}T00:00:00\.000$/);
  });

  test('beforeOrOnDate with relative string resolves to end-of-day', () => {
    // Why: mirrors Tesseract's `<= endOfDay`. Using start-of-day here (as
    // the pre-fix code did) would exclude almost the entire target day.
    const result = normalizeDateFilterValues(
      { member: 'Orders.createdAt', operator: 'beforeOrOnDate', values: ['yesterday'] },
      'UTC'
    );

    expect(result.values).toHaveLength(1);
    expect(result.values[0]).toMatch(/^\d{4}-\d{2}-\d{2}T23:59:59\.999$/);
  });

  test('afterDate with relative string resolves to end-of-day', () => {
    // Why: mirrors Tesseract's `> endOfDay`. Using start-of-day would let
    // "after yesterday" still match rows from within yesterday.
    const result = normalizeDateFilterValues(
      { member: 'Orders.createdAt', operator: 'afterDate', values: ['yesterday'] },
      'UTC'
    );

    expect(result.values).toHaveLength(1);
    expect(result.values[0]).toMatch(/^\d{4}-\d{2}-\d{2}T23:59:59\.999$/);
  });

  test('onTheDate with relative string resolves to a two-value range', () => {
    // Why: onTheDate is broken in both planners today with a single value —
    // legacy `onTheDateWhere` reads values[0]/values[1] and Tesseract maps
    // onTheDate to InDateRange which strictly requires 2 args. Resolving here
    // to [start, end] fixes the operator in both planners simultaneously.
    const result = normalizeDateFilterValues(
      { member: 'Orders.createdAt', operator: 'onTheDate', values: ['yesterday'] },
      'UTC'
    );

    expect(result.values).toHaveLength(2);
    expect(result.values[0]).toMatch(/^\d{4}-\d{2}-\d{2}T00:00:00\.000$/);
    expect(result.values[1]).toMatch(/^\d{4}-\d{2}-\d{2}T23:59:59\.999$/);
  });

  test('single-date operator with absolute bare date is passed through byte-exact', () => {
    // Why: pre-PR the gateway didn't touch filter values, so `beforeOrOnDate:
    // ["2024-01-15"]` reached Tesseract's format_to_date which padded it to
    // end-of-day. Resolving here would rewrite it to T00:00:00.000, silently
    // excluding all of Jan 15 — a data-correctness regression under the
    // default planner. Absolute strings must round-trip unchanged.
    const filter = {
      member: 'Orders.createdAt',
      operator: 'beforeOrOnDate',
      values: ['2024-01-15'],
    };

    const result = normalizeDateFilterValues(filter, 'UTC');

    expect(result.values).toEqual(['2024-01-15']);
  });

  test('single-date operator with absolute timestamp is passed through byte-exact', () => {
    // Why: dateParser truncates every string to day granularity via chrono's
    // startOf('day'). A user passing "2024-01-15T10:30:00.000" would silently
    // lose the time component under the pre-fix path. Absolute timestamps
    // must not be re-parsed.
    const filter = {
      member: 'Orders.createdAt',
      operator: 'afterDate',
      values: ['2024-01-15T10:30:00.000'],
    };

    const result = normalizeDateFilterValues(filter, 'UTC');

    expect(result.values).toEqual(['2024-01-15T10:30:00.000']);
  });

  test('non-string value is passed through unchanged', () => {
    // Why: numeric or boolean values in `values` cannot be date strings, so
    // dispatching them into dateParser would raise an opaque error. The
    // typeof guard is what keeps the helper safe for those callers.
    const filter = { member: 'Orders.count', operator: 'gt', values: [10] };

    const result = normalizeDateFilterValues(filter, 'UTC');

    expect(result).toBe(filter);
  });

  test('non-date operator (equals) is returned unchanged', () => {
    // Why: the helper must not touch unrelated filters.
    const filter = { member: 'Orders.status', operator: 'equals', values: ['active'] };

    const result = normalizeDateFilterValues(filter, 'UTC');

    expect(result).toBe(filter);
  });

  test('group filter (no operator) is returned unchanged', () => {
    // Why: the walker that wires this helper in recurses into OR/AND groups
    // itself; the helper must be a safe no-op on group nodes.
    const groupFilter = { or: [{ member: 'X', operator: 'equals', values: ['1'] }] };

    const result = normalizeDateFilterValues(groupFilter, 'UTC');

    expect(result).toBe(groupFilter);
  });

  test('inDateRange with two relative strings raises UserError at the boundary', () => {
    // Why: pre-fix, `values: ["last week", "this week"]` bypassed the
    // resolver (which only handles single-element values) and failed deep in
    // query execution with an opaque error. Users should get a clear API-
    // boundary error explaining the supported shapes.
    expect(() => normalizeDateFilterValues(
      { member: 'Orders.createdAt', operator: 'inDateRange', values: ['last week', 'this week'] },
      'UTC'
    )).toThrow(/Relative-date strings are only supported/);
  });

  test('inDateRange with mixed relative + absolute strings raises UserError', () => {
    // Why: even one relative element in a multi-value array is unsupported —
    // the resolver has no way to reconcile a relative range with an absolute
    // endpoint. Reject at the boundary.
    expect(() => normalizeDateFilterValues(
      { member: 'Orders.createdAt', operator: 'inDateRange', values: ['last week', '2024-01-31'] },
      'UTC'
    )).toThrow(/Relative-date strings are only supported/);
  });

  test('notInDateRange with two relative strings raises UserError', () => {
    // Why: parity — both range operators must fail the same way for the
    // same shape.
    expect(() => normalizeDateFilterValues(
      { member: 'Orders.createdAt', operator: 'notInDateRange', values: ['last week', 'this week'] },
      'UTC'
    )).toThrow(/Relative-date strings are only supported/);
  });

  test('inDateRange with two absolute strings passes through unchanged', () => {
    // Why: regression guard — the pre-existing absolute two-element form is
    // the canonical shape and must not trigger the new UserError.
    const filter = {
      member: 'Orders.createdAt',
      operator: 'inDateRange',
      values: ['2024-01-01', '2024-01-31'],
    };

    const result = normalizeDateFilterValues(filter, 'UTC');

    expect(result).toBe(filter);
  });

  test('inDateRange with two absolute UTC-suffixed timestamps passes through unchanged', () => {
    // Why: regression guard — the SQL API push-down produces ISO timestamps
    // with a `Z` designator (e.g. "1999-12-30T00:00:00.000Z"). These are
    // absolute values and must not be mistaken for relative-date strings.
    const filter = {
      member: 'Orders.createdAt',
      operator: 'inDateRange',
      values: ['1999-12-30T00:00:00.000Z', '1999-12-30T23:59:59.999Z'],
    };

    const result = normalizeDateFilterValues(filter, 'UTC');

    expect(result).toBe(filter);
  });

  test('inDateRange with two absolute offset timestamps passes through unchanged', () => {
    // Why: numeric UTC offsets are as absolute as `Z` and must be accepted.
    const filter = {
      member: 'Orders.createdAt',
      operator: 'inDateRange',
      values: ['2024-01-01T00:00:00+02:00', '2024-01-31T23:59:59-05:00'],
    };

    const result = normalizeDateFilterValues(filter, 'UTC');

    expect(result).toBe(filter);
  });

  test('invalid relative date string raises UserError', () => {
    // Why: failure must surface at the API boundary with a clear HTTP 400,
    // not as an opaque SQL error after the query reaches the database.
    expect(() => normalizeDateFilterValues(
      { member: 'Orders.createdAt', operator: 'inDateRange', values: ['definitely not a date'] },
      'UTC'
    )).toThrow(/Can't parse date/);
  });
});

describe('normalizeQuery: date-range filter resolution', () => {
  beforeEach(() => {
    jest.spyOn(Date, 'now').mockReturnValue(FIXED_NOW.getTime());
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  test('top-level inDateRange filter with relative string is resolved', () => {
    // Why: even without an OR wrapper, a filter leaf with a relative date
    // value must be resolved at the gateway.
    const result = normalizeQuery({
      ...baseQuery,
      filters: [
        { member: 'Orders.createdAt', operator: 'inDateRange', values: ['last 2 weeks'] },
      ],
    }, false);

    const leaf = result.filters[0];
    expect(leaf.values).toHaveLength(2);
    expect(leaf.values[0]).toMatch(/^\d{4}-\d{2}-\d{2}T00:00:00\.000$/);
    expect(leaf.values[1]).toMatch(/^\d{4}-\d{2}-\d{2}T23:59:59\.999$/);
  });

  test('inDateRange leaf nested inside OR is resolved (the actual feature)', () => {
    // Why: this is the whole point. The recursive walker must reach leaves
    // inside groups and apply the helper there too.
    const result = normalizeQuery({
      ...baseQuery,
      filters: [{
        or: [
          { member: 'Orders.createdAt', operator: 'inDateRange', values: ['last 2 weeks'] },
          { member: 'Orders.status', operator: 'equals', values: ['pending'] },
        ],
      }],
    }, false);

    const [dateLeaf, statusLeaf] = result.filters[0].or;
    expect(dateLeaf.values).toHaveLength(2);
    expect(dateLeaf.values[0]).toMatch(/^\d{4}-\d{2}-\d{2}T/);
    expect(statusLeaf.values).toEqual(['pending']);
  });

  test('inDateRange leaf nested inside AND is resolved', () => {
    // Why: AND must work symmetrically with OR.
    const result = normalizeQuery({
      ...baseQuery,
      filters: [{
        and: [
          { member: 'Orders.createdAt', operator: 'inDateRange', values: ['last 2 weeks'] },
          { member: 'Orders.status', operator: 'equals', values: ['active'] },
        ],
      }],
    }, false);

    expect(result.filters[0].and[0].values).toHaveLength(2);
  });

  test('deeply nested date filter (OR inside AND) is resolved', () => {
    // Why: the walker must recurse to arbitrary depth, not just one level.
    const result = normalizeQuery({
      ...baseQuery,
      filters: [{
        and: [
          { member: 'Orders.status', operator: 'equals', values: ['active'] },
          {
            or: [
              { member: 'Orders.createdAt', operator: 'inDateRange', values: ['last 2 weeks'] },
              { member: 'Orders.priority', operator: 'equals', values: ['high'] },
            ],
          },
        ],
      }],
    }, false);

    const dateLeaf = result.filters[0].and[1].or[0];
    expect(dateLeaf.values).toHaveLength(2);
    expect(dateLeaf.values[0]).toMatch(/^\d{4}-\d{2}-\d{2}T/);
  });

  test('top-level timeDimensions.dateRange still resolves unchanged', () => {
    // Why: invariant — existing queries that only use top-level timeDimensions
    // must produce the same shape they always did. Both paths share
    // resolveDateRange so they cannot diverge.
    const result = normalizeQuery({
      ...baseQuery,
      timeDimensions: [
        { dimension: 'Orders.createdAt', dateRange: 'last 2 weeks' },
      ],
    }, false);

    const td = result.timeDimensions[0];
    expect(td.dateRange).toHaveLength(2);
    expect(td.dateRange[0]).toMatch(/^\d{4}-\d{2}-\d{2}T00:00:00\.000$/);
    expect(td.dateRange[1]).toMatch(/^\d{4}-\d{2}-\d{2}T23:59:59\.999$/);
  });

  test('invalid relative date inside OR raises UserError at gateway', () => {
    // Why: a malformed relative date must fail at the API boundary with a
    // clear message, not deep in the SQL planner.
    expect(() => normalizeQuery({
      ...baseQuery,
      filters: [{
        or: [
          { member: 'Orders.createdAt', operator: 'inDateRange', values: ['definitely not a date'] },
          { member: 'Orders.status', operator: 'equals', values: ['pending'] },
        ],
      }],
    }, false)).toThrow(/Can't parse date/);
  });

  test('timezone from query is honored for relative dates inside OR', () => {
    // Why: the resolver must use the query timezone. We pin this with a
    // day-boundary instant where UTC and LA fall on different calendar days.
    const dayBoundaryNow = new Date(Date.UTC(2026, 5, 25, 2, 0, 0, 0));
    jest.spyOn(Date, 'now').mockReturnValue(dayBoundaryNow.getTime());

    const utc = normalizeQuery({
      ...baseQuery,
      timezone: 'UTC',
      filters: [{ or: [
        { member: 'Orders.createdAt', operator: 'inDateRange', values: ['today'] },
      ]}],
    }, false);

    const la = normalizeQuery({
      ...baseQuery,
      timezone: 'America/Los_Angeles',
      filters: [{ or: [
        { member: 'Orders.createdAt', operator: 'inDateRange', values: ['today'] },
      ]}],
    }, false);

    expect(utc.filters[0].or[0].values[0]).toMatch(/^2026-06-25T/);
    expect(la.filters[0].or[0].values[0]).toMatch(/^2026-06-24T/);
  });

  test('non-date filters inside OR are untouched', () => {
    // Why: regression guard — equals/contains/etc. must not be modified.
    const result = normalizeQuery({
      ...baseQuery,
      filters: [{
        or: [
          { member: 'Orders.status', operator: 'equals', values: ['active'] },
          { member: 'Orders.status', operator: 'equals', values: ['pending'] },
        ],
      }],
    }, false);

    expect(result.filters[0].or[0].values).toEqual(['active']);
    expect(result.filters[0].or[1].values).toEqual(['pending']);
  });
});

// resolveDateRange is the shared resolver used by both timeDimensions.dateRange
// (line 409) and normalizeDateFilterValues. Pinning its behavior here keeps
// the filter path and the timeDimensions path from diverging.
describe('resolveDateRange', () => {
  test('YYYY-MM-DD start expands to start-of-day, end to end-of-day', () => {
    // Why: this formatting invariant is what makes the two paths byte-equal.
    expect(resolveDateRange(['2026-01-01', '2026-01-31'], 'UTC')).toEqual([
      '2026-01-01T00:00:00.000',
      '2026-01-31T23:59:59.999',
    ]);
  });

  test('single-element array expands to [v, v] then normalizes', () => {
    // Why: legacy "single day" shorthand for dateRange.
    expect(resolveDateRange(['2026-01-15'], 'UTC')).toEqual([
      '2026-01-15T00:00:00.000',
      '2026-01-15T23:59:59.999',
    ]);
  });

  test('undefined input returns undefined (granularity-only timeDimensions)', () => {
    // Why: a timeDimension may have only `granularity` and no `dateRange`.
    expect(resolveDateRange(undefined, 'UTC')).toBeUndefined();
  });
});
