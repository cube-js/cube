import { PostgresQuery } from '../../src';
import { prepareYamlCompiler } from './PrepareCompiler';
import { createSchemaYaml } from './utils';

// Regression test: a view that exposes the same cube through more than one join
// path can produce a join tree holding both `outstanding -> rate` and
// `rate -> outstanding`. `enrichedJoinHintsFromJoinTree` walks the to -> from
// map upwards, so without a visited check that walk ping-pongs between the two
// cubes until the path array exceeds the maximum array length and the query
// fails with `RangeError: Invalid array length`.
describe('Join hints from a cyclic join tree', () => {
  const schema = createSchemaYaml({
    cubes: [
      {
        name: 'facility',
        sql_table: 'facility_tbl',
        joins: [{
          name: 'outstanding',
          sql: '{facility}.id = {outstanding}.facility_id',
          relationship: 'one_to_many',
        }],
        measures: [{ name: 'count', type: 'count' }],
        dimensions: [
          { name: 'id', sql: 'id', type: 'number', primary_key: true },
          { name: 'name', sql: 'name', type: 'string' },
        ],
      },
      {
        name: 'outstanding',
        sql_table: 'outstanding_tbl',
        joins: [{
          name: 'rate',
          sql: '{outstanding}.id = {rate}.outstanding_id',
          relationship: 'one_to_many',
        }],
        measures: [],
        dimensions: [
          { name: 'id', sql: 'id', type: 'number', primary_key: true },
          { name: 'facility_id', sql: 'facility_id', type: 'number' },
          { name: 'alias', sql: 'alias', type: 'string' },
        ],
      },
      {
        name: 'rate',
        sql_table: 'rate_tbl',
        joins: [{
          name: 'outstanding',
          sql: '{rate}.outstanding_id = {outstanding}.id',
          relationship: 'many_to_one',
        }],
        measures: [],
        dimensions: [
          { name: 'id', sql: 'id', type: 'number', primary_key: true },
          { name: 'outstanding_id', sql: 'outstanding_id', type: 'number' },
          { name: 'spread', sql: 'spread', type: 'number' },
        ],
      },
    ],
    views: [{
      name: 'v',
      cubes: [
        { join_path: 'facility', includes: ['count', 'name'] },
        { join_path: 'facility.outstanding', includes: ['alias'] },
        { join_path: 'facility.outstanding.rate', includes: ['spread'] },
      ],
    }],
  });

  it('terminates on a join tree that contains a back edge', async () => {
    const compilers = prepareYamlCompiler(schema);
    await compilers.compiler.compile();

    const query = new PostgresQuery(compilers, {
      dimensions: ['v.name', 'v.alias', 'v.spread'],
      timezone: 'UTC',
    });

    // Both directions are declared between `outstanding` and `rate`. The back edge
    // comes last, as it does in a join tree built from a real model.
    const cyclicJoinTree = {
      root: 'facility',
      joins: [
        { from: 'facility', to: 'outstanding' },
        { from: 'outstanding', to: 'rate' },
        { from: 'rate', to: 'outstanding' },
      ],
    };

    const hints = (query as any).enrichedJoinHintsFromJoinTree(cyclicJoinTree, ['outstanding', 'rate']);

    // Without the fix this call never returns. It must also keep reporting the
    // path from the root, so the back edge cannot drop the `facility` prefix.
    expect(hints).toEqual([
      ['facility', 'outstanding'],
      ['facility', 'outstanding', 'rate'],
    ]);
  });

  it('resolves cubes named after Object.prototype keys', async () => {
    const compilers = prepareYamlCompiler(schema);
    await compilers.compiler.compile();

    const query = new PostgresQuery(compilers, {
      dimensions: ['v.name'],
      timezone: 'UTC',
    });

    // `identifierRegex` allows these as cube names, so the joins lookup must not
    // pick up anything inherited from Object.prototype.
    const joinTree = {
      root: 'toString',
      joins: [
        { from: 'toString', to: 'constructor' },
        { from: 'constructor', to: 'orders' },
      ],
    };

    const hints = (query as any).enrichedJoinHintsFromJoinTree(joinTree, ['toString', 'orders']);

    expect(hints).toEqual([
      'toString',
      ['toString', 'constructor', 'orders'],
    ]);
  });
});
