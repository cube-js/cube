// Reproduces https://github.com/cube-js/cube/issues/10181: an ungrouped query on a
// cube without a primary key fails with "One or more Primary key is required" even
// when `allowUngroupedWithoutPrimaryKey` is set. The SQL itself builds; the error
// comes from `PreAggregations.transformQueryToCanUseForm`, which the server calls
// for every `/v1/load` request (`includeTransformedQuery`).
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('Ungrouped query on a cube without a primary key', () => {
  jest.setTimeout(200000);

  const compilers = prepareYamlCompiler(`
cubes:
  - name: no_pk
    sql: SELECT 1 AS a, 2 AS b UNION ALL SELECT 3 AS a, 4 AS b
    dimensions:
      - name: a
        sql: a
        type: number
      - name: b
        sql: b
        type: number
`);

  it('runs with allowUngroupedWithoutPrimaryKey', async () => {
    await compilers.compiler.compile();
    const query = new PostgresQuery(compilers, {
      dimensions: ['no_pk.a', 'no_pk.b'],
      ungrouped: true,
      allowUngroupedWithoutPrimaryKey: true,
      order: [{ id: 'no_pk.a' }],
      timezone: 'UTC',
    } as any);

    const canUse = compilers.compiler.withQuery(
      query,
      () => query.preAggregations.canUseTransformedQuery()
    );
    expect(canUse.ungrouped).toBe(true);

    const res = await dbRunner.testQuery(query.buildSqlAndParams());
    expect(res).toEqual([{ no_pk__a: 1, no_pk__b: 2 }, { no_pk__a: 3, no_pk__b: 4 }]);
  });
});
