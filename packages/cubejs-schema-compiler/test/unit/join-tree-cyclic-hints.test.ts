import { PostgresQuery } from '../../src';
import { UserError } from '../../src/compiler/UserError';
import { prepareYamlCompiler } from './PrepareCompiler';
import { createSchemaYaml } from './utils';

class TestPostgresQuery extends PostgresQuery {
  public enrichedJoinHintsFromJoinTree(joinTree, joinHints) {
    return super.enrichedJoinHintsFromJoinTree(joinTree, joinHints);
  }
}

// Regression test for CORE-891: `a` reaches `c` only through `b`, while view `w` joins `b` from
// `c`. Joining both used to add a second edge into `b` (`c -> b`), so the child -> parent walk in
// enrichedJoinHintsFromJoinTree looped forever and died with `RangeError: Invalid array length`.
describe('Join tree with a hint leading back into an already joined cube', () => {
  const schema = createSchemaYaml({
    cubes: [
      {
        name: 'a',
        sql_table: 'a_tbl',
        joins: [{ name: 'b', sql: '{CUBE}.b_id = {b}.id', relationship: 'many_to_one' }],
        measures: [{ name: 'count', type: 'count' }],
        dimensions: [{ name: 'id', sql: 'id', type: 'number', primary_key: true }],
      },
      {
        name: 'b',
        sql_table: 'b_tbl',
        joins: [{ name: 'c', sql: '{CUBE}.c_id = {c}.id', relationship: 'many_to_one' }],
        dimensions: [
          { name: 'id', sql: 'id', type: 'number', primary_key: true },
          { name: 'name', sql: 'name', type: 'string' },
        ],
      },
      {
        name: 'c',
        sql_table: 'c_tbl',
        joins: [{ name: 'b', sql: '{CUBE}.b_id = {b}.id', relationship: 'many_to_one' }],
        dimensions: [
          { name: 'id', sql: 'id', type: 'number', primary_key: true },
          { name: 'name', sql: 'name', type: 'string' },
        ],
      },
    ],
    views: [{
      name: 'w',
      cubes: [
        { join_path: 'c', includes: ['name'], prefix: true },
        { join_path: 'c.b', includes: ['name'], prefix: true },
      ],
    }],
  });

  it('builds a join tree that joins each cube once', async () => {
    const compilers = prepareYamlCompiler(schema);
    await compilers.compiler.compile();

    const join = compilers.joinGraph.buildJoin(['a', 'c', ['c', 'b']]);
    expect(join?.joins.map(j => `${j.from}->${j.to}`)).toEqual(['a->b', 'b->c']);
  });

  it('builds SQL for the query', async () => {
    const compilers = prepareYamlCompiler(schema);
    await compilers.compiler.compile();

    const query = new PostgresQuery(compilers, {
      measures: ['a.count'],
      dimensions: ['c.name', 'w.b_name'],
      timezone: 'UTC',
    });
    const [sql] = query.buildSqlAndParams();
    expect(sql.match(/JOIN\s+b_tbl/g)).toHaveLength(1);
    expect(sql.match(/JOIN\s+c_tbl/g)).toHaveLength(1);
  });

  it('rejects a cyclic join tree with a UserError', async () => {
    const compilers = prepareYamlCompiler(schema);
    await compilers.compiler.compile();

    const query = new TestPostgresQuery(compilers, { measures: ['a.count'], timezone: 'UTC' });
    const joinTree = {
      root: 'a',
      joins: [
        { from: 'a', to: 'b' },
        { from: 'b', to: 'c' },
        { from: 'c', to: 'b' },
      ],
    };
    expect(() => query.enrichedJoinHintsFromJoinTree(joinTree, ['c'])).toThrow(UserError);
  });
});
