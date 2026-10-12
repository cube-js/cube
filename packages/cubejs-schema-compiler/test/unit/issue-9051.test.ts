import { prepareYamlCompiler } from './PrepareCompiler';
import { ClickHouseQuery } from '../../src/adapter/ClickHouseQuery';

// https://github.com/cube-js/cube/issues/9051
// A raw `type: time` dimension (no granularity) is wrapped by ClickHouseQuery.convertTz as
// `toTimeZone(toDateTime64(x, 0), tz)`. Scale 0 drops the sub-second part of DateTime64
// columns, so ungrouped log queries lose their millisecond/microsecond ordering.
// convertTzForRawTimeDimension is on by default (CUBESQL_SQL_PUSH_DOWN defaults to true).
describe('Issue #9051: ClickHouse raw time dimensions are truncated to seconds', () => {
  const { compiler, cubeEvaluator, joinGraph } = prepareYamlCompiler(`
cubes:
  - name: logs
    sql_table: logs
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: created
        sql: created
        type: time
    measures:
      - name: count
        type: count
`);

  beforeAll(async () => {
    await compiler.compile();
  });

  it.each(['UTC', 'America/New_York'])('keeps sub-second precision of a raw time dimension (%s)', (timezone) => {
    const query = new ClickHouseQuery({ joinGraph, cubeEvaluator, compiler }, {
      dimensions: ['logs.id', 'logs.created'],
      ungrouped: true,
      timezone,
      convertTzForRawTimeDimension: true,
    });

    const [sql] = query.buildSqlAndParams();

    expect(sql).toMatch(/logs__created/);
    expect(sql).not.toMatch(/toDateTime64\(`logs`\.created, 0\)/);
  });
});
