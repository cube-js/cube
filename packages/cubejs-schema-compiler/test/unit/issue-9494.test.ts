import { prepareYamlCompiler } from './PrepareCompiler';
import { ClickHouseQuery } from '../../src/adapter/ClickHouseQuery';

// https://github.com/cube-js/cube/issues/9494
// ClickHouse comparison operators return UInt8, not Bool. The rollup load SQL selects boolean
// dimensions and segments as the bare expression, so the pre-aggregation columns are built as
// integers (0/1). Querying the rollup with the segment then fails in Cube Store with
// "Filter predicate must return boolean values, not Int64". The load SQL should convert
// boolean members to Bool (e.g. `toBool(...)` / `CAST(... AS Bool)`).
describe('Issue #9494: ClickHouse boolean dimensions/segments become integers in rollups', () => {
  const { compiler, cubeEvaluator, joinGraph } = prepareYamlCompiler(`
cubes:
  - name: metrics
    sql_table: metrics
    dimensions:
      - name: brand
        sql: brand
        type: string
      - name: is_current_brand
        sql: "brand_id = 17390"
        type: boolean
      - name: created_at
        sql: create_dt
        type: time
    measures:
      - name: count
        type: count
    segments:
      - name: curr_brand
        sql: "{CUBE}.brand_id = 17390"
    pre_aggregations:
      - name: main
        measures: [metrics.count]
        dimensions: [metrics.brand, metrics.is_current_brand]
        segments: [metrics.curr_brand]
        time_dimension: metrics.created_at
        granularity: day
        indexes:
          - name: brand_index
            columns: [metrics.brand]
`);

  beforeAll(async () => {
    await compiler.compile();
  });

  it('casts boolean dimensions and segments to Bool in the rollup load SQL', () => {
    const query = new ClickHouseQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['metrics.count'],
      dimensions: ['metrics.brand'],
      segments: ['metrics.curr_brand'],
      timeDimensions: [{ dimension: 'metrics.created_at', granularity: 'day', dateRange: ['2025-04-01', '2025-04-30'] }],
      timezone: 'UTC',
    });

    const [sql] = query.buildSqlAndParams();
    expect(sql).toMatch(/metrics_main/);

    const description: any = query.preAggregations?.preAggregationsDescription();
    const loadSql: string = description[0].loadSql[0];

    // Columns are comma separated and neither expression contains a comma.
    const boolCast = (alias: string) => new RegExp(`(toBool|CAST)\\([^,]*brand_id = 17390[^,]* \`${alias}\``, 'i');
    expect(loadSql).toMatch(boolCast('metrics__is_current_brand'));
    expect(loadSql).toMatch(boolCast('metrics__curr_brand'));
  });
});
