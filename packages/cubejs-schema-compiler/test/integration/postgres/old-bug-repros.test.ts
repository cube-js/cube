import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// Repros for old GitHub issues about pre-aggregations. Each test runs the query
// through the pre-aggregation (built as a temp table in Postgres) and asserts the
// result a query to the source data would return.
//
// Note: these tests execute the pre-aggregation SQL on Postgres, not on Cube Store.
// Bugs that live inside Cube Store (e.g. #8580 via its RollingWindowAggregate
// rewrite, #8916 "Sort key size can't be 0") are covered end to end by
// packages/cubejs-testing/test/cli-postgresql-cubestore-old-bug-repros.test.ts.
describe('old pre-aggregation bug repros', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube('order_rolling', {
      sql: \`
        SELECT 10 AS value, '2023-11-01'::TIMESTAMP AS date UNION ALL
        SELECT 10 AS value, '2023-12-01'::TIMESTAMP AS date UNION ALL
        SELECT 10 AS value, '2024-01-01'::TIMESTAMP AS date UNION ALL
        SELECT 30 AS value, '2024-02-01'::TIMESTAMP AS date UNION ALL
        SELECT 50 AS value, '2024-03-01'::TIMESTAMP AS date UNION ALL
        SELECT 80 AS value, '2024-04-01'::TIMESTAMP AS date
      \`,
      dimensions: {
        date: { sql: 'date', type: 'time' },
      },
      measures: {
        current_month_sum: {
          sql: 'value',
          type: 'sum',
          rolling_window: { trailing: '3 month', offset: 'start' },
        },
      },
      preAggregations: {
        main: {
          measures: [CUBE.current_month_sum],
          timeDimension: CUBE.date,
          granularity: 'day',
        },
      },
    });

    cube('Order', {
      sql: \`
        SELECT 1 AS id, '2023-07-01T00:00:00.000Z'::TIMESTAMP AS created_at UNION ALL
        SELECT 2 AS id, '2023-07-05T00:00:00.000Z'::TIMESTAMP AS created_at UNION ALL
        SELECT 3 AS id, '2023-07-11T00:00:00.000Z'::TIMESTAMP AS created_at UNION ALL
        SELECT 4 AS id, '2023-07-15T00:00:00.000Z'::TIMESTAMP AS created_at UNION ALL
        SELECT 5 AS id, '2023-07-21T00:00:00.000Z'::TIMESTAMP AS created_at UNION ALL
        SELECT 6 AS id, '2023-07-25T00:00:00.000Z'::TIMESTAMP AS created_at UNION ALL
        SELECT 7 AS id, '2023-07-29T00:00:00.000Z'::TIMESTAMP AS created_at UNION ALL
        SELECT 8 AS id, '2023-08-02T00:00:00.000Z'::TIMESTAMP AS created_at UNION ALL
        SELECT 9 AS id, '2023-08-06T00:00:00.000Z'::TIMESTAMP AS created_at UNION ALL
        SELECT 10 AS id, '2023-08-12T00:00:00.000Z'::TIMESTAMP AS created_at UNION ALL
        SELECT 11 AS id, '2023-08-16T00:00:00.000Z'::TIMESTAMP AS created_at UNION ALL
        SELECT 12 AS id, '2023-08-22T00:00:00.000Z'::TIMESTAMP AS created_at
      \`,
      measures: {
        rollingCount: {
          sql: 'id',
          type: 'count',
          rollingWindow: { trailing: 'unbounded' },
        },
      },
      dimensions: {
        createdAt: { sql: 'created_at', type: 'time' },
      },
      preAggregations: {
        totalByDay: {
          measures: [CUBE.rollingCount],
          timeDimension: CUBE.createdAt,
          granularity: 'day',
        },
      },
    });

    cube('LE_LegalEntitiesMetadata', {
      sql: \`
        SELECT 'e1' AS legal_entity_id, 'Alice' AS display_name UNION ALL
        SELECT 'e1', 'Bob' UNION ALL
        SELECT 'e2', 'Carol' UNION ALL
        SELECT 'e3', 'Dave' UNION ALL
        SELECT 'e3', 'Eve'
      \`,
      measures: {
        display_names: {
          sql: \`STRING_AGG(\${CUBE.display_name}::TEXT, ', ' ORDER BY \${CUBE.display_name})\`,
          type: 'string',
        },
      },
      dimensions: {
        legal_entity_id: { sql: 'legal_entity_id', type: 'string', primaryKey: true, shown: true },
        display_name: { sql: 'display_name', type: 'string' },
      },
      preAggregations: {
        rollup: {
          dimensions: [CUBE.legal_entity_id],
          measures: [CUBE.display_names],
          indexes: {
            idx: { columns: [CUBE.legal_entity_id] },
          },
        },
      },
    });
  `);

  const buildQuery = (q: Record<string, unknown>) => new PostgresQuery(
    { joinGraph, cubeEvaluator, compiler },
    { timezone: 'UTC', preAggregationsSchema: '', ...q }
  );

  const runWithPreAggregation = async (q: Record<string, unknown>, expectedTable: string) => {
    await compiler.compile();
    const query = buildQuery(q);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    expect(preAggregationsDescription.map((d: any) => d.tableName)).toEqual([expectedTable]);
    return dbRunner.evaluateQueryWithPreAggregations(query);
  };

  describe('issue #8580 rolling window offset: start in pre-aggregations', () => {
    const q = {
      measures: ['order_rolling.current_month_sum'],
      timeDimensions: [{
        dimension: 'order_rolling.date',
        granularity: 'month',
        dateRange: ['2023-11-01', '2024-07-01'],
      }],
      order: [{ id: 'order_rolling.date' }],
      limit: 5000,
    };
    // offset: start => window is the 3 months BEFORE the current month
    const expected = [null, '10', '20', '30', '50', '90', '160', '130', '80'];

    it('pre-aggregation result equals source result', async () => {
      const res = await runWithPreAggregation(q, 'order_rolling_main');
      expect(res.map((r: any) => r.order_rolling__current_month_sum)).toEqual(expected);
      expect(res.map((r: any) => r.order_rolling__date_month)).toEqual([
        '2023-11-01T00:00:00.000Z', '2023-12-01T00:00:00.000Z', '2024-01-01T00:00:00.000Z',
        '2024-02-01T00:00:00.000Z', '2024-03-01T00:00:00.000Z', '2024-04-01T00:00:00.000Z',
        '2024-05-01T00:00:00.000Z', '2024-06-01T00:00:00.000Z', '2024-07-01T00:00:00.000Z',
      ]);
    });
  });

  describe('issue #8745 trailing unbounded rolling window without granularity from pre-aggregation', () => {
    const q = {
      measures: ['Order.rollingCount'],
      timeDimensions: [{
        dimension: 'Order.createdAt',
        dateRange: ['2023-07-11', '2023-07-18'],
      }],
      limit: 5000,
    };

    // Source (no pre-aggregation) returns 4: ids 1..4 have created_at <= 2023-07-18
    it('pre-aggregation result is 4, not NULL', async () => {
      const res = await runWithPreAggregation(q, 'order_total_by_day');
      expect(res).toEqual([{ order__rolling_count: '4' }]);
    });
  });

  describe('issue #9462 string measure served from a pre-aggregation', () => {
    it('keeps STRING_AGG values (no SUM re-aggregation)', async () => {
      await compiler.compile();
      const query = buildQuery({
        dimensions: ['LE_LegalEntitiesMetadata.legal_entity_id'],
        measures: ['LE_LegalEntitiesMetadata.display_names'],
        order: [{ id: 'LE_LegalEntitiesMetadata.legal_entity_id' }],
      });
      const [sql] = query.buildSqlAndParams();
      expect(sql).toContain('l_e__legal_entities_metadata_rollup');
      expect(sql.toLowerCase()).not.toMatch(/sum\([^)]*display_names/);

      const res = await dbRunner.evaluateQueryWithPreAggregations(query);
      expect(res).toEqual([
        { l_e__legal_entities_metadata__legal_entity_id: 'e1', l_e__legal_entities_metadata__display_names: 'Alice, Bob' },
        { l_e__legal_entities_metadata__legal_entity_id: 'e2', l_e__legal_entities_metadata__display_names: 'Carol' },
        { l_e__legal_entities_metadata__legal_entity_id: 'e3', l_e__legal_entities_metadata__display_names: 'Dave, Eve' },
      ]);
    });

    it('does not use the rollup when the string measure would have to be rolled up', async () => {
      await compiler.compile();
      const query = buildQuery({ measures: ['LE_LegalEntitiesMetadata.display_names'] });
      const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
      expect(preAggregationsDescription).toEqual([]);
      const res = await dbRunner.testQuery(query.buildSqlAndParams());
      expect(res).toEqual([{ l_e__legal_entities_metadata__display_names: 'Alice, Bob, Carol, Dave, Eve' }]);
    });
  });
});
