import { MysqlQuery } from '../../src/adapter/MysqlQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// #10096: after a DST change (Europe/Zurich, 2025-10-26) queries fail with
// "No pre-aggregation partitions were built yet ... Expected table name patterns".
// With the default CUBEJS_DB_MYSQL_USE_NAMED_TIMEZONES=false, MysqlQuery.convertTz
// embeds the UTC offset that is current at compile time (`moment().tz(tz).format('Z')`)
// into the SQL. The pre-aggregation load SQL feeds the structure version (the
// table name hash), so the expected table name flips at a DST change, while a
// running refresh worker keeps building/serving tables under the old hash.
// (A fixed offset is also wrong for rows on the other side of a DST change.)
describe('Issue #10096 - MySQL pre-aggregation load SQL across a DST change', () => {
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

  // test/global-setup.ts enables named time zones; exercise the default (numeric offsets).
  let prevNamedTz: string | undefined;

  beforeAll(() => {
    prevNamedTz = process.env.CUBEJS_DB_MYSQL_USE_NAMED_TIMEZONES;
    process.env.CUBEJS_DB_MYSQL_USE_NAMED_TIMEZONES = 'false';
  });

  afterAll(() => {
    process.env.CUBEJS_DB_MYSQL_USE_NAMED_TIMEZONES = prevNamedTz;
  });

  const descriptionAt = async (isoNow: string) => {
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
      return description;
    } finally {
      spy.mockRestore();
    }
  };

  it('load SQL does not depend on the current DST offset', async () => {
    // Europe/Zurich leaves DST at 2025-10-26T01:00:00Z (+02:00 -> +01:00).
    const before = await descriptionAt('2025-10-26T00:30:00Z');
    const after = await descriptionAt('2025-10-26T01:30:00Z');

    expect(after.loadSql[0]).toEqual(before.loadSql[0]);
  });
});
