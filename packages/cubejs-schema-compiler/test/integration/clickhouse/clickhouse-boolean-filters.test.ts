import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { ClickHouseQuery } from '../../../src/adapter/ClickHouseQuery';
import { ClickHouseDbRunner } from './ClickHouseDbRunner';

// A ClickHouse predicate evaluates to UInt8, which refuses the string 'true' a boolean filter sends
describe('ClickHouse boolean dimension value filters', () => {
  jest.setTimeout(200000);

  const dbRunner = new ClickHouseDbRunner();

  const noDataSet = async () => Promise.resolve();

  afterAll(async () => {
    await dbRunner.tearDown();
  });

  const compilers = prepareYamlCompiler(`
cubes:
  - name: rows
    sql: >
      SELECT 1 AS id, toUInt8(1) AS flag, true AS flag_bool
      UNION ALL
      SELECT 2 AS id, toUInt8(0) AS flag, false AS flag_bool
      UNION ALL
      SELECT 3 AS id, toUInt8(1) AS flag, true AS flag_bool
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
        public: true
      - name: is_flagged
        sql: "{CUBE}.flag = 1"
        type: boolean
      - name: flag_bool
        sql: "{CUBE}.flag_bool"
        type: boolean
    measures:
      - name: count
        type: count
`);

  beforeAll(async () => {
    await compilers.compiler.compile();
  });

  const idsFor = async (filter: { member: string, operator: string, values: string[] }) => {
    const query = new ClickHouseQuery(compilers, {
      dimensions: ['rows.id'],
      filters: [filter],
      order: [{ id: 'rows.id' }],
      timezone: 'UTC',
    });
    const rows = await dbRunner.testQuery(query.buildSqlAndParams(), noDataSet);
    return rows.map((r: any) => Number(r.rows__id));
  };

  describe.each(['rows.is_flagged', 'rows.flag_bool'])('%s', (member) => {
    it.each([
      ['equals', ['true'], [1, 3]],
      ['equals', ['false'], [2]],
      ['equals', ['1'], [1, 3]],
      ['notEquals', ['true'], [2]],
      ['equals', ['true', 'false'], [1, 2, 3]],
      ['notEquals', ['true', 'false'], []],
    ])('%s %j', async (operator, values, expected) => {
      expect(await idsFor({ member, operator, values })).toEqual(expected);
    });
  });
});
