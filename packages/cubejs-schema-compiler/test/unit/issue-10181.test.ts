import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// https://github.com/cube-js/cube/issues/10181
// An ungrouped query against a cube without a primary key must work when
// allowUngroupedWithoutPrimaryKey is set.
describe('Issue #10181 - ungrouped query on cube without primary key', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: nopk
    sql_table: public.nopk
    dimensions:
      - name: name
        sql: name
        type: string
    measures:
      - name: amount
        sql: amount
        type: sum
`);

  const buildQuery = () => new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
    dimensions: ['nopk.name'],
    measures: ['nopk.amount'],
    ungrouped: true,
    allowUngroupedWithoutPrimaryKey: true,
  });

  beforeAll(async () => {
    await compiler.compile();
  });

  it('builds SQL', () => {
    expect(() => buildQuery().buildSqlAndParams()).not.toThrow();
  });

  it('computes the transformed query used for pre-aggregation matching', () => {
    // CompilerApi.getSql calls this for every REST /load request
    expect(() => buildQuery().preAggregations!.canUseTransformedQuery()).not.toThrow();
  });
});
