import { MysqlQuery } from '../../src/adapter/MysqlQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// @link https://github.com/cube-js/cube/issues/10096
// With the default numeric-offset time zone conversion, MySQL pre-aggregation
// load SQL embeds the offset that is current at compile time. The load SQL feeds
// the structure version (table name hash), so it flips at a DST change. A long-running
// refresh worker keeps the old SQL in its query cache while a freshly compiled API
// instance expects the new hash, and queries fail with "No pre-aggregation
// partitions were built yet".
describe('Issue 10096: MySQL pre-aggregation load SQL across a DST change', () => {
  const model = `
cubes:
  - name: statistics
    sql_table: statistics
    measures:
      - name: count
        type: count
    dimensions:
      - name: organizationid
        sql: organizationId
        type: string
      - name: createdat
        sql: createdAt
        type: time
    pre_aggregations:
      - name: day
        dimensions: [organizationid]
        measures: [count]
        time_dimension: createdat
        granularity: day
`;

  // Test global setup enables named time zones; exercise the default (numeric offsets).
  let prevNamedTz: string | undefined;

  beforeAll(() => {
    prevNamedTz = process.env.CUBEJS_DB_MYSQL_USE_NAMED_TIMEZONES;
    process.env.CUBEJS_DB_MYSQL_USE_NAMED_TIMEZONES = 'false';
  });

  afterAll(() => {
    process.env.CUBEJS_DB_MYSQL_USE_NAMED_TIMEZONES = prevNamedTz;
  });

  const loadSqlAt = async (isoNow: string): Promise<string> => {
    const spy = jest.spyOn(Date, 'now').mockReturnValue(new Date(isoNow).getTime());

    try {
      const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(model);
      await compiler.compile();

      const query = new MysqlQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['statistics.count'],
        dimensions: ['statistics.organizationid'],
        timezone: 'Europe/Zurich',
        preAggregationsSchema: '',
      });

      const [description]: any = query.preAggregations?.preAggregationsDescription();
      expect(description.preAggregationId).toEqual('statistics.day');
      return description.loadSql[0];
    } finally {
      spy.mockRestore();
    }
  };

  it('load SQL (and so the structure version) does not change at a DST transition', async () => {
    // Europe/Zurich leaves DST at 2025-10-26T01:00:00Z (+02:00 -> +01:00)
    const beforeDst = await loadSqlAt('2025-10-26T00:30:00Z');
    const afterDst = await loadSqlAt('2025-10-26T01:30:00Z');

    expect(afterDst).toEqual(beforeDst);
  });
});
