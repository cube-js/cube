/* eslint-disable no-restricted-syntax */
import { AthenaQuery } from '../../src/adapter/AthenaQuery';
import { prepareJsCompiler } from './PrepareCompiler';

describe('AthenaQuery', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube('orders', {
      sql: \`
        SELECT *
        FROM public.orders
      \`,
      dimensions: {
        id: {
          sql: \`id\`,
          type: 'number',
          primaryKey: true
        },
        created_at: {
          sql: \`created_at\`,
          type: 'time',
          granularities: {
            ten_seconds: {
              interval: '10 seconds',
            },
          },
        }
      },
      measures: {
        count: {
          type: 'count',
        }
      },
      preAggregations: {
        by_ten_seconds: {
          measures: [count],
          timeDimension: created_at,
          granularity: 'ten_seconds',
          partitionGranularity: 'day',
        }
      }
    });
  `);

  it('convertTz uses manual offset calculation (inherits from PrestodbQuery)', async () => {
    await compiler.compile();

    const query = new AthenaQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['orders.count'],
      timeDimensions: [{
        dimension: 'orders.created_at',
        granularity: 'month',
        dateRange: ['2026-01-01', '2026-12-31'],
      }],
      timezone: 'Asia/Kolkata',
    });

    const queryAndParams = query.buildSqlAndParams();
    const sql = queryAndParams[0];

    expect(sql).toContain('timezone_hour');
    expect(sql).toContain('timezone_minute');
    expect(sql).not.toMatch(
      /CAST\(\(.*AT TIME ZONE.*\) AS TIMESTAMP\)/
    );
  });

  // Pre-aggregation load SQL for a custom granularity must not expose
  // `timestamp with time zone` columns — Athena/Hive rejects that type when
  // writing to an export bucket. `from_iso8601_timestamp(...)` returns a
  // TZ-aware timestamp and `date_add` inherits the type of its third
  // argument, so the origin must be cast to a plain TIMESTAMP.
  it('custom granularity pre-aggregation loadSql casts dateBin origin to TIMESTAMP', async () => {
    await compiler.compile();

    const query = new AthenaQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['orders.count'],
      timeDimensions: [{
        dimension: 'orders.created_at',
        granularity: 'ten_seconds',
        dateRange: ['2026-04-11', '2026-04-12'],
      }],
      timezone: 'UTC',
    });

    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    expect(preAggregationsDescription.length).toBeGreaterThan(0);

    const loadSql = preAggregationsDescription[0].loadSql[0] as string;

    expect(loadSql).toMatch(/CAST\(from_iso8601_timestamp\('[^']+'\) AS TIMESTAMP\)/);
    expect(loadSql).not.toMatch(/date_add\([^)]*from_iso8601_timestamp\('[^']+'\)\s*\)/);
  });
});
