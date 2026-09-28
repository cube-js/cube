import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// Repros for old open GitHub issues, run against Postgres. Each test asserts
// the correct behavior, so it fails until its bug is fixed. Tests for issues
// that no longer reproduce (#7730, #9549) are kept as regression guards.
describe('old bug repros (postgres)', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube('Orders', {
      sql: \`
        SELECT 1 AS id, 100 AS amount, 'new' AS status UNION ALL
        SELECT 2 AS id, 200 AS amount, 'new' AS status UNION ALL
        SELECT 3 AS id, 300 AS amount, 'processed' AS status UNION ALL
        SELECT 4 AS id, 500 AS amount, 'processed' AS status UNION ALL
        SELECT 5 AS id, 600 AS amount, 'shipped' AS status
      \`,
      measures: {
        amount_sum: {
          sql: 'amount',
          type: 'sum',
        },
        cv_amount: {
          sql: \`STDDEV(\${amount_sum}) / NULLIF(AVG(\${amount_sum}), 0)\`,
          type: 'number',
          multi_stage: true,
          add_group_by: [CUBE.id],
        },
        max_amount: {
          sql: \`\${amount_sum}\`,
          type: 'max',
          multi_stage: true,
          add_group_by: [CUBE.id],
        },
      },
      dimensions: {
        id: { sql: 'id', type: 'number', primary_key: true, public: true },
        status: { sql: 'status', type: 'string' },
      },
    });

    cube('sessions', {
      sql: \`
        SELECT 1 AS id, '2024-01-01'::TIMESTAMP AS created_at UNION ALL
        SELECT 2 AS id, '2024-01-03'::TIMESTAMP AS created_at UNION ALL
        SELECT 3 AS id, '2024-01-08'::TIMESTAMP AS created_at UNION ALL
        SELECT 4 AS id, '2024-01-09'::TIMESTAMP AS created_at UNION ALL
        SELECT 5 AS id, '2024-01-10'::TIMESTAMP AS created_at
      \`,
      measures: {
        weeklyCount: {
          sql: 'id',
          type: 'count',
          rollingWindow: {
            leading: '1 week',
            offset: 'start',
          },
        },
      },
      dimensions: {
        id: { sql: 'id', type: 'number', primaryKey: true },
        created_at: { sql: 'created_at', type: 'time' },
      },
      preAggregations: {
        main: {
          measures: [CUBE.weeklyCount],
          timeDimension: CUBE.created_at,
          granularity: 'day',
        },
      },
    });

    cube('revenue', {
      sql: \`
        SELECT 1 AS id, 10 AS amount, '2023-12-31'::TIMESTAMP AS date UNION ALL
        SELECT 2 AS id, 100 AS amount, '2024-01-02'::TIMESTAMP AS date UNION ALL
        SELECT 3 AS id, 1000 AS amount, '2024-12-31'::TIMESTAMP AS date
      \`,
      measures: {
        revenue: { sql: 'amount', type: 'sum' },
        revenue_prior_year: {
          multi_stage: true,
          sql: \`\${revenue}\`,
          type: 'number',
          time_shift: [{ time_dimension: CUBE.date, interval: '1 year', type: 'prior' }],
        },
        revenue_prior_52w: {
          multi_stage: true,
          sql: \`\${revenue}\`,
          type: 'number',
          time_shift: [{ time_dimension: CUBE.date, interval: '52 week', type: 'prior' }],
        },
      },
      dimensions: {
        id: { sql: 'id', type: 'number', primaryKey: true },
        date: { sql: 'date', type: 'time' },
      },
    });
  `);

  const buildQuery = (q: Record<string, unknown>) => new PostgresQuery(
    { joinGraph, cubeEvaluator, compiler },
    { timezone: 'UTC', preAggregationsSchema: '', ...q }
  );

  describe('issue #10864 multi_stage type: number with inline aggregates and a dimension outside add_group_by', () => {
    it('control: a typed multi_stage measure works', async () => {
      await compiler.compile();
      const query = buildQuery({
        measures: ['Orders.max_amount'],
        dimensions: ['Orders.status'],
        order: [{ id: 'Orders.status' }],
      });
      const res = await dbRunner.testQuery(query.buildSqlAndParams());
      expect(res).toEqual([
        { orders__status: 'new', orders__max_amount: '200' },
        { orders__status: 'processed', orders__max_amount: '500' },
        { orders__status: 'shipped', orders__max_amount: '600' },
      ]);
    });

    it('returns one row per status for STDDEV(...) / AVG(...)', async () => {
      await compiler.compile();
      const query = buildQuery({
        measures: ['Orders.cv_amount'],
        dimensions: ['Orders.status'],
        order: [{ id: 'Orders.status' }],
      });
      const res = await dbRunner.testQuery(query.buildSqlAndParams());
      expect(res.map((r: any) => r.orders__status)).toEqual(['new', 'processed', 'shipped']);
      expect(Number(res[0].orders__cv_amount)).toBeCloseTo(0.4714, 3);
      expect(Number(res[1].orders__cv_amount)).toBeCloseTo(0.3536, 3);
      expect(res[2].orders__cv_amount).toBeNull();
    });
  });

  describe('issue #7730 rolling window count with leading + offset: start in a pre-aggregation', () => {
    const q = {
      measures: ['sessions.weeklyCount'],
      timeDimensions: [{
        dimension: 'sessions.created_at',
        granularity: 'day',
        dateRange: ['2024-01-01', '2024-01-03'],
      }],
      order: [{ id: 'sessions.created_at' }],
    };
    it('builds the pre-aggregation and serves the same result as the source', async () => {
      await compiler.compile();
      const sourceQuery = buildQuery({ ...q, preAggregationsSchema: undefined, disableExternalPreAggregations: true });
      const source = await dbRunner.testQuery(sourceQuery.buildSqlAndParams());
      const expected = source.map((r: any) => r.sessions__weekly_count);
      expect(expected.length).toBe(3);

      const query = buildQuery(q);
      const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
      expect(preAggregationsDescription.map((d: any) => d.tableName)).toEqual(['sessions_main']);
      const res = await dbRunner.evaluateQueryWithPreAggregations(query);
      expect(res.map((r: any) => r.sessions__weekly_count)).toEqual(expected);
    });
  });

  describe('issue #9549 time_shift by 1 year with weekly granularity', () => {
    it('`1 year` shifts by calendar year, `52 week` lines up weeks', async () => {
      await compiler.compile();
      const query = buildQuery({
        measures: ['revenue.revenue', 'revenue.revenue_prior_year', 'revenue.revenue_prior_52w'],
        timeDimensions: [{
          dimension: 'revenue.date',
          granularity: 'week',
          dateRange: ['2024-12-30', '2025-01-05'],
        }],
      });
      const res = await dbRunner.testQuery(query.buildSqlAndParams());
      // `1 year` shifts by the calendar year, so the week of 2024-12-30 compares
      // against 2023-12-30..2024-01-05 (110). `52 week` lines up with the week of
      // 2024-01-01 (100).
      expect(res).toEqual([{
        revenue__date_week: '2024-12-30T00:00:00.000Z',
        revenue__revenue: '1000',
        revenue__revenue_prior_year: '110',
        revenue__revenue_prior_52w: '100',
      }]);
    });
  });
});
