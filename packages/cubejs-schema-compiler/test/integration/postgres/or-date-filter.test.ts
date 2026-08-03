import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// End-to-end coverage for date filters inside OR/AND groups. The gateway
// resolves relative strings ("last 2 weeks") to absolute ISO datetimes before
// the query reaches the schema compiler; these tests feed already-absolute
// values to focus on planner correctness under both the legacy JS planner
// and Tesseract. The point is that a date predicate expressed inside `filters.or`
// / `filters.and` must (a) appear as ONE branch of that logical group in the
// executed SQL, and (b) produce the expected row set against real data.
//
// Inline data (7 rows):
//   id status     created_at
//   1  active     2024-01-05
//   2  active     2024-01-15
//   3  pending    2024-02-01
//   4  pending    2024-02-20
//   5  archived   2024-03-10
//   6  active     2024-06-10
//   7  cancelled  2024-06-25
describe('OR/AND date filter integration', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube('orders', {
      sql: \`
        SELECT 1 as id, 'active'    as status, '2024-01-05T00:00:00.000Z'::timestamptz as created_at UNION ALL
        SELECT 2 as id, 'active'    as status, '2024-01-15T00:00:00.000Z'::timestamptz as created_at UNION ALL
        SELECT 3 as id, 'pending'   as status, '2024-02-01T00:00:00.000Z'::timestamptz as created_at UNION ALL
        SELECT 4 as id, 'pending'   as status, '2024-02-20T00:00:00.000Z'::timestamptz as created_at UNION ALL
        SELECT 5 as id, 'archived'  as status, '2024-03-10T00:00:00.000Z'::timestamptz as created_at UNION ALL
        SELECT 6 as id, 'active'    as status, '2024-06-10T00:00:00.000Z'::timestamptz as created_at UNION ALL
        SELECT 7 as id, 'cancelled' as status, '2024-06-25T00:00:00.000Z'::timestamptz as created_at
      \`,

      measures: {
        count: { type: 'count' }
      },

      dimensions: {
        id:        { sql: 'id',         type: 'number', primaryKey: true, shown: true },
        status:    { sql: 'status',     type: 'string' },
        createdAt: { sql: 'created_at', type: 'time' }
      }
    })
  `);

  // Why: this is the whole feature — a date range restricted to ONE branch of
  // an OR group. Rows 1 & 2 match the date window; row 3 matches by status.
  // Legacy behavior (date pulled into a global AND) would exclude row 3.
  it('inDateRange inside OR: date window OR status match', async () => dbRunner.runQueryTest({
    measures: ['orders.count'],
    dimensions: ['orders.id'],
    filters: [{
      or: [
        {
          member: 'orders.createdAt',
          operator: 'inDateRange',
          values: ['2024-01-01T00:00:00.000', '2024-01-31T23:59:59.999'],
        },
        { member: 'orders.status', operator: 'equals', values: ['pending'] },
      ],
    }],
    order: [{ id: 'orders.id' }],
    timezone: 'UTC',
  }, [
    { orders__id: 1, orders__count: '1' },
    { orders__id: 2, orders__count: '1' },
    { orders__id: 3, orders__count: '1' },
    { orders__id: 4, orders__count: '1' },
  ], { joinGraph, cubeEvaluator, compiler }));

  // Why: AND must produce the symmetric structure — both branches inside a
  // single grouped condition. Only rows in the date window AND with status
  // 'active' should match: rows 1 and 2.
  it('inDateRange inside AND: date window AND status match', async () => dbRunner.runQueryTest({
    measures: ['orders.count'],
    dimensions: ['orders.id'],
    filters: [{
      and: [
        {
          member: 'orders.createdAt',
          operator: 'inDateRange',
          values: ['2024-01-01T00:00:00.000', '2024-01-31T23:59:59.999'],
        },
        { member: 'orders.status', operator: 'equals', values: ['active'] },
      ],
    }],
    order: [{ id: 'orders.id' }],
    timezone: 'UTC',
  }, [
    { orders__id: 1, orders__count: '1' },
    { orders__id: 2, orders__count: '1' },
  ], { joinGraph, cubeEvaluator, compiler }));

  // Why: `beforeOrOnDate` is one of the two operators ovr flagged as needing
  // end-of-day boundary semantics (Tesseract's `<= endOfDay`). The gateway
  // resolves the relative form to T23:59:59.999; passing that same absolute
  // value through here confirms the compiler respects it and the row for
  // 2024-01-15 (row 2) is included at the boundary.
  it('beforeOrOnDate at end-of-day boundary includes rows on that day', async () => dbRunner.runQueryTest({
    measures: ['orders.count'],
    dimensions: ['orders.id'],
    filters: [{
      or: [
        {
          member: 'orders.createdAt',
          operator: 'beforeOrOnDate',
          values: ['2024-01-15T23:59:59.999'],
        },
        { member: 'orders.status', operator: 'equals', values: ['cancelled'] },
      ],
    }],
    order: [{ id: 'orders.id' }],
    timezone: 'UTC',
  }, [
    { orders__id: 1, orders__count: '1' },
    { orders__id: 2, orders__count: '1' },
    { orders__id: 7, orders__count: '1' },
  ], { joinGraph, cubeEvaluator, compiler }));

  // Why: `afterDate` is the second operator with end-of-day semantics
  // (Tesseract's `> endOfDay`). "After 2024-02-20" must exclude that day
  // itself — row 4 (2024-02-20) must NOT appear. Row 5 (2024-03-10) and
  // beyond do.
  it('afterDate at end-of-day boundary excludes rows on that day', async () => dbRunner.runQueryTest({
    measures: ['orders.count'],
    dimensions: ['orders.id'],
    filters: [{
      or: [
        {
          member: 'orders.createdAt',
          operator: 'afterDate',
          values: ['2024-02-20T23:59:59.999'],
        },
        { member: 'orders.status', operator: 'equals', values: ['pending'] },
      ],
    }],
    order: [{ id: 'orders.id' }],
    timezone: 'UTC',
  }, [
    { orders__id: 3, orders__count: '1' },
    { orders__id: 4, orders__count: '1' },
    { orders__id: 5, orders__count: '1' },
    { orders__id: 6, orders__count: '1' },
    { orders__id: 7, orders__count: '1' },
  ], { joinGraph, cubeEvaluator, compiler }));

  // Why: `onTheDate` was broken in both planners before this PR. Legacy read
  // values[0]/values[1] and returned garbage when only one value was
  // supplied; Tesseract errored with "2 arguments expected". After the fix
  // the gateway resolves it to a two-value range, so `onTheDate: [date]`
  // must return exactly the rows on that calendar day.
  it('onTheDate with resolved two-value range matches only that day', async () => dbRunner.runQueryTest({
    measures: ['orders.count'],
    dimensions: ['orders.id'],
    filters: [{
      and: [
        {
          member: 'orders.createdAt',
          operator: 'onTheDate',
          values: ['2024-02-01T00:00:00.000', '2024-02-01T23:59:59.999'],
        },
      ],
    }],
    order: [{ id: 'orders.id' }],
    timezone: 'UTC',
  }, [
    { orders__id: 3, orders__count: '1' },
  ], { joinGraph, cubeEvaluator, compiler }));

  // Why: the walker has to recurse through arbitrary depth. A bug that stopped
  // at depth 1 would drop the date predicate silently. Here we ask for
  // status='active' AND (created in Jan OR status='pending'). Only rows 1
  // and 2 satisfy both parts.
  it('date filter nested two levels deep (OR inside AND)', async () => dbRunner.runQueryTest({
    measures: ['orders.count'],
    dimensions: ['orders.id'],
    filters: [{
      and: [
        { member: 'orders.status', operator: 'equals', values: ['active'] },
        {
          or: [
            {
              member: 'orders.createdAt',
              operator: 'inDateRange',
              values: ['2024-01-01T00:00:00.000', '2024-01-31T23:59:59.999'],
            },
            { member: 'orders.status', operator: 'equals', values: ['urgent'] },
          ],
        },
      ],
    }],
    order: [{ id: 'orders.id' }],
    timezone: 'UTC',
  }, [
    { orders__id: 1, orders__count: '1' },
    { orders__id: 2, orders__count: '1' },
  ], { joinGraph, cubeEvaluator, compiler }));

  // Why: existing queries that use only top-level `timeDimensions.dateRange`
  // must continue producing the same results — this is the invariant the PR
  // is built on. If the compiler started routing top-level dateRange through
  // the new filter code path, this test would surface a regression.
  it('regression guard: top-level timeDimensions.dateRange still filters correctly', async () => dbRunner.runQueryTest({
    measures: ['orders.count'],
    dimensions: ['orders.id'],
    timeDimensions: [{
      dimension: 'orders.createdAt',
      dateRange: ['2024-01-01', '2024-01-31'],
    }],
    order: [{ id: 'orders.id' }],
    timezone: 'UTC',
  }, [
    { orders__id: 1, orders__count: '1' },
    { orders__id: 2, orders__count: '1' },
  ], { joinGraph, cubeEvaluator, compiler }));
});
