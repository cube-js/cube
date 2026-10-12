import { prepareYamlCompiler } from './PrepareCompiler';
import { ClickHouseQuery } from '../../src/adapter/ClickHouseQuery';

// https://github.com/cube-js/cube/issues/10326
// ClickHouseQuery.dateTimeCast (no timezone) renders `parseDateTimeBestEffort(?)`, which
// returns a second-precision DateTime. A boundary like `2026-01-20T23:59:59.999` becomes
// `23:59:59`, so DateTime64 rows in the last second of a partition/date range are dropped.
// It should use `parseDateTime64BestEffort` (or an equivalent that keeps milliseconds).
describe('Issue #10326: ClickHouse time filters lose millisecond precision', () => {
  const { compiler, cubeEvaluator, joinGraph } = prepareYamlCompiler(`
cubes:
  - name: events
    sql_table: events
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: ts
        sql: ts
        type: time
    measures:
      - name: count
        type: count
    pre_aggregations:
      - name: main
        measures: [events.count]
        time_dimension: events.ts
        granularity: day
        partition_granularity: day
        build_range_start:
          sql: SELECT '2026-01-20T00:00:00.000Z'
        build_range_end:
          sql: SELECT '2026-01-20T23:59:59.999Z'
        indexes:
          - name: ts_index
            columns: [events.ts]
`);

  beforeAll(async () => {
    await compiler.compile();
  });

  it('keeps milliseconds in pre-aggregation partition bounds', () => {
    const query = new ClickHouseQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['events.count'],
      timeDimensions: [{ dimension: 'events.ts', granularity: 'day', dateRange: ['2026-01-20', '2026-01-20'] }],
      timezone: 'UTC',
    });

    const description: any = query.preAggregations?.preAggregationsDescription();
    const loadSql: string = description[0].loadSql[0];

    expect(loadSql).toMatch(/WHERE/);
    expect(loadSql).not.toMatch(/parseDateTimeBestEffort\(/);
  });

  it('keeps milliseconds in date range filters', () => {
    const query = new ClickHouseQuery({ joinGraph, cubeEvaluator, compiler }, {
      dimensions: ['events.id'],
      timeDimensions: [{ dimension: 'events.ts', dateRange: ['2026-01-20', '2026-01-20'] }],
      timezone: 'UTC',
    });

    const [sql, params] = query.buildSqlAndParams();

    expect(params).toContain('2026-01-20T23:59:59.999Z');
    expect(sql).not.toMatch(/parseDateTimeBestEffort\(/);
  });
});
