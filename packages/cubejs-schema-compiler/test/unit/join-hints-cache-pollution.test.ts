import { PostgresQuery } from '../../src';
import { prepareYamlCompiler } from './PrepareCompiler';
import { createSchemaYaml } from './utils';

// Regression test: cross-query pollution of compilerCache join hints
// through a view. All view members report the view as their cube, so the
// member-cubes cache namespacing added in #10084 cannot distinguish two
// different queries against the same view. If a member's SQL uses
// FILTER_PARAMS, collecting its join hints traverses ALL members of the
// current query (via allBackAliasMembersExceptSegments), leaking their join
// paths into the compilerCache entry of that member. A later query reusing
// the poisoned entry gets an extra join.
describe('Join hints compilerCache pollution through views', () => {
  const schema = createSchemaYaml({
    cubes: [
      {
        name: 'orders',
        sql_table: 'orders_tbl',
        joins: [{
          name: 'layout',
          sql: '1 = 1',
          relationship: 'one_to_one',
        }],
        measures: [{ name: 'count', type: 'count' }],
        dimensions: [
          { name: 'id', sql: 'id', type: 'number', primary_key: true },
          { name: 'category', sql: 'category', type: 'string' },
          {
            name: 'flag',
            sql: 'CASE WHEN {FILTER_PARAMS.orders.category.filter(\'category\')} THEN 1 ELSE 0 END',
            type: 'number',
          },
        ],
      },
      {
        name: 'layout',
        sql_table: 'layout_tbl',
        measures: [],
        dimensions: [
          { name: 'id', sql: 'id', type: 'number', primary_key: true },
          { name: 'bucket', sql: 'bucket', type: 'string' },
        ],
      },
    ],
    views: [{
      name: 'v',
      cubes: [
        { join_path: 'orders', includes: ['count', 'id', 'category', 'flag'] },
        { join_path: 'orders.layout', includes: ['bucket'] },
      ],
    }],
  });

  it('does not leak join hints from one query to another via compilerCache', async () => {
    const compilers = prepareYamlCompiler(schema);
    await compilers.compiler.compile();

    // Query 1 (poisoner): legitimately joins `layout` because it filters on v.bucket
    const poisoner = new PostgresQuery(compilers, {
      dimensions: ['v.flag'],
      filters: [{ member: 'v.bucket', operator: 'equals', values: ['x'] }],
      timezone: 'UTC',
    });
    const poisonerSql = poisoner.buildSqlAndParams()[0];
    expect(poisonerSql).toContain('layout_tbl');

    // Query 2 (victim): does not reference layout at all — must not join it
    const victim = new PostgresQuery(compilers, {
      dimensions: ['v.flag'],
      filters: [{ member: 'v.category', operator: 'equals', values: ['y'] }],
      timezone: 'UTC',
    });
    const victimSql = victim.buildSqlAndParams()[0];
    expect(victimSql).not.toContain('layout_tbl');
  });

  it('reverse order: query needing the join still gets it after a cached query without it', async () => {
    const compilers = prepareYamlCompiler(schema);
    await compilers.compiler.compile();

    const victim = new PostgresQuery(compilers, {
      dimensions: ['v.flag'],
      filters: [{ member: 'v.category', operator: 'equals', values: ['y'] }],
      timezone: 'UTC',
    });
    expect(victim.buildSqlAndParams()[0]).not.toContain('layout_tbl');

    const poisoner = new PostgresQuery(compilers, {
      dimensions: ['v.flag'],
      filters: [{ member: 'v.bucket', operator: 'equals', values: ['x'] }],
      timezone: 'UTC',
    });
    expect(poisoner.buildSqlAndParams()[0]).toContain('layout_tbl');
  });

  it('control: victim query alone does not join layout', async () => {
    // Fresh compilers → fresh compilerCache → correct SQL
    const compilers = prepareYamlCompiler(schema);
    await compilers.compiler.compile();

    const victim = new PostgresQuery(compilers, {
      dimensions: ['v.flag'],
      filters: [{ member: 'v.category', operator: 'equals', values: ['y'] }],
      timezone: 'UTC',
    });
    const victimSql = victim.buildSqlAndParams()[0];
    expect(victimSql).not.toContain('layout_tbl');
  });
});
