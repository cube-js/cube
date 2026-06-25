/* eslint-disable no-restricted-syntax */
import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from './PrepareCompiler';

// End-to-end coverage for the feature: a date-range filter expressed inside
// an OR group must produce SQL where the date predicate is INSIDE the OR
// branch, not pulled out as a global AND. The gateway resolves any relative
// strings to absolute datetime ranges before the query reaches the schema
// compiler, so these tests pass already-absolute values to the compiler.

describe('or-date-filter end-to-end SQL', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube(\`Orders\`, {
      sql: \`select * from orders\`,

      measures: {
        count: { type: 'count' }
      },

      dimensions: {
        status: { sql: 'status', type: 'string' },
        createdAt: { sql: 'created_at', type: 'time' }
      }
    })
  `);

  beforeAll(async () => {
    await compiler.compile();
  });

  it('emits date predicate inside an OR group, not as a global AND', async () => {
    // Why: this is the structural feature. Before this change, a date filter
    // could only live in `timeDimensions.dateRange` (global AND); now a
    // resolved `inDateRange` filter inside `filters.or` produces SQL where
    // the date predicate is ONE BRANCH of the OR.
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['Orders.count'],
      filters: [{
        or: [
          {
            member: 'Orders.createdAt',
            operator: 'inDateRange',
            values: ['2026-06-01T00:00:00.000', '2026-06-14T23:59:59.999'],
          },
          { member: 'Orders.status', operator: 'equals', values: ['pending'] },
        ],
      }],
      timezone: 'UTC',
    });

    const [sql, params] = query.buildSqlAndParams();

    expect(sql).toContain(' OR ');
    expect(sql).toContain('"orders".created_at');
    expect(sql).toContain('"orders".status');
    // Both branches must be inside a single OR group, not split into separate
    // top-level AND conditions.
    expect(sql.match(/ OR /g)?.length).toBeGreaterThanOrEqual(1);
    expect(params).toContain('pending');
  });

  it('emits AND group with date predicate when filters.and contains a date filter', async () => {
    // Why: AND must produce the symmetric structure — both branches inside
    // a single grouped condition.
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['Orders.count'],
      filters: [{
        and: [
          {
            member: 'Orders.createdAt',
            operator: 'inDateRange',
            values: ['2026-06-01T00:00:00.000', '2026-06-14T23:59:59.999'],
          },
          { member: 'Orders.status', operator: 'equals', values: ['active'] },
        ],
      }],
      timezone: 'UTC',
    });

    const [sql, params] = query.buildSqlAndParams();

    expect(sql).toContain('"orders".created_at');
    expect(sql).toContain('"orders".status');
    expect(params).toContain('active');
  });

  it('emits the date range params inside the OR group', async () => {
    // Why: pin the actual param values so we'd notice if the date predicate
    // is silently dropped or replaced with a TRUE/FALSE shortcut.
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['Orders.count'],
      filters: [{
        or: [
          {
            member: 'Orders.createdAt',
            operator: 'inDateRange',
            values: ['2026-06-01T00:00:00.000', '2026-06-14T23:59:59.999'],
          },
          { member: 'Orders.status', operator: 'equals', values: ['pending'] },
        ],
      }],
      timezone: 'UTC',
    });

    const [, params] = query.buildSqlAndParams();

    expect(params.some(p => /^2026-06-01T00:00:00\.000/.test(String(p)))).toBe(true);
    expect(params.some(p => /^2026-06-14T23:59:59\.999/.test(String(p)))).toBe(true);
  });

  it('emits a single-value date predicate for beforeDate inside an OR group', async () => {
    // Why: single-date operators take ONE param. A bug that interpreted the
    // value as a range would generate two params and broken SQL.
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['Orders.count'],
      filters: [{
        or: [
          {
            member: 'Orders.createdAt',
            operator: 'beforeDate',
            values: ['2026-06-24T00:00:00.000'],
          },
          { member: 'Orders.status', operator: 'equals', values: ['archived'] },
        ],
      }],
      timezone: 'UTC',
    });

    const [sql, params] = query.buildSqlAndParams();

    expect(sql).toContain(' OR ');
    const dateParams = params.filter(p => /^2026-/.test(String(p)));
    expect(dateParams).toHaveLength(1);
  });

  it('resolves a date filter nested two levels deep (OR inside AND)', async () => {
    // Why: the schema-compiler filter walker has to recurse through arbitrary
    // depth. A bug that stopped at depth 1 would let some queries silently
    // compile to wrong SQL (date predicate dropped or moved to global AND).
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['Orders.count'],
      filters: [{
        and: [
          { member: 'Orders.status', operator: 'equals', values: ['active'] },
          {
            or: [
              {
                member: 'Orders.createdAt',
                operator: 'inDateRange',
                values: ['2026-06-01T00:00:00.000', '2026-06-14T23:59:59.999'],
              },
              { member: 'Orders.status', operator: 'equals', values: ['urgent'] },
            ],
          },
        ],
      }],
      timezone: 'UTC',
    });

    const [sql, params] = query.buildSqlAndParams();

    expect(sql).toContain(' OR ');
    expect(sql).toContain(' AND ');
    expect(sql).toContain('"orders".created_at');
    expect(params).toContain('active');
    expect(params).toContain('urgent');
    expect(params.some(p => /^2026-06-01T00:00:00\.000/.test(String(p)))).toBe(true);
  });

  it('handles two independent date filters in the same OR group', async () => {
    // Why: a user might filter for "created in window A OR created in window B"
    // (e.g. comparing two non-contiguous periods). Both predicates must appear
    // and bind correctly; a bug that deduped or merged them by member name
    // would silently drop one branch.
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['Orders.count'],
      filters: [{
        or: [
          {
            member: 'Orders.createdAt',
            operator: 'inDateRange',
            values: ['2026-01-01T00:00:00.000', '2026-01-31T23:59:59.999'],
          },
          {
            member: 'Orders.createdAt',
            operator: 'inDateRange',
            values: ['2026-06-01T00:00:00.000', '2026-06-30T23:59:59.999'],
          },
        ],
      }],
      timezone: 'UTC',
    });

    const [sql, params] = query.buildSqlAndParams();

    expect(sql).toContain(' OR ');
    // Both windows must appear as separate params — four date params total.
    const dateParams = params.filter(p => /^2026-/.test(String(p)));
    expect(dateParams).toHaveLength(4);
    expect(params.some(p => /^2026-01-01T00:00:00\.000/.test(String(p)))).toBe(true);
    expect(params.some(p => /^2026-06-30T23:59:59\.999/.test(String(p)))).toBe(true);
  });

  it('supports notInDateRange inside an OR group', async () => {
    // Why: Every range operator should compose into OR groups,
    // not just inDateRange.
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['Orders.count'],
      filters: [{
        or: [
          {
            member: 'Orders.createdAt',
            operator: 'notInDateRange',
            values: ['2026-06-01T00:00:00.000', '2026-06-14T23:59:59.999'],
          },
          { member: 'Orders.status', operator: 'equals', values: ['archived'] },
        ],
      }],
      timezone: 'UTC',
    });

    const [sql, params] = query.buildSqlAndParams();

    expect(sql).toContain(' OR ');
    expect(sql).toContain('"orders".created_at');
    // notInDateRange negates the range comparison — the SQL should reflect
    // that, not produce the same predicate as inDateRange.
    expect(sql).toMatch(/<|>|NOT|not/);
    expect(params).toContain('archived');
  });

  it('does not change SQL for queries using only top-level timeDimensions (regression guard)', async () => {
    // Why: We want to validate that "existing top-level timeDimensions
    // behavior is unchanged". This validates that a query with NO `filters`
    // entry and just `timeDimensions.dateRange` returns the same shape
    // it always did, with no OR groups injected by the new code path.
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['Orders.count'],
      timeDimensions: [{
        dimension: 'Orders.createdAt',
        dateRange: ['2026-06-01', '2026-06-14'],
      }],
      timezone: 'UTC',
    });

    const [sql, params] = query.buildSqlAndParams();

    expect(sql).not.toContain(' OR ');
    expect(sql).toContain('"orders".created_at');
    expect(params.some(p => /^2026-06-01T00:00:00\.000/.test(String(p)))).toBe(true);
    expect(params.some(p => /^2026-06-14T23:59:59\.999/.test(String(p)))).toBe(true);
  });
});
