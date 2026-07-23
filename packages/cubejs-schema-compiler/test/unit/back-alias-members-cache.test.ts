import { PostgresQuery } from '../../src';
import { prepareYamlCompiler } from './PrepareCompiler';
import { createSchemaYaml } from './utils';

const aliasedViewSchema = createSchemaYaml({
  cubes: [{
    name: 'orders',
    sql_table: 'orders',
    measures: [{ name: 'count', type: 'count' }],
    dimensions: [
      { name: 'id', sql: 'id', type: 'number', primary_key: true },
      { name: 'status', sql: 'status', type: 'string' },
    ],
  }],
  views: [{
    name: 'orders_view',
    cubes: [{
      join_path: 'orders',
      includes: [
        { name: 'count', alias: 'total_orders' },
        { name: 'status', alias: 'state' },
      ],
    }],
  }],
});

function queryOptions() {
  return {
    measures: ['orders_view.total_orders'],
    dimensions: ['orders_view.state'],
    filters: [{
      member: 'orders_view.state',
      operator: 'equals',
      values: ['completed'],
    }],
    timezone: 'UTC',
  };
}

describe('allBackAliasMembersExceptSegments cache', () => {
  const compilers = prepareYamlCompiler(aliasedViewSchema);

  beforeAll(async () => {
    await compilers.compiler.compile();
  });

  it('memoizes the completed query alias map', () => {
    const query = new PostgresQuery(compilers, queryOptions());
    const expected = query.backAliasMembers(query.flattenAllMembers(true));
    const backAliasMembers = jest.spyOn(query, 'backAliasMembers');

    const first = (query as any).cachedAllBackAliasMembersExceptSegments();
    const second = (query as any).cachedAllBackAliasMembersExceptSegments();

    expect(first).toEqual(expected);
    expect(Object.keys(first).length).toBeGreaterThan(0);
    expect(Object.isFrozen(first)).toBe(true);
    expect(second).toBe(first);
    expect(backAliasMembers).toHaveBeenCalledTimes(1);
  });

  it('keeps the public alias-map result fresh and mutable', () => {
    const query = new PostgresQuery(compilers, queryOptions());

    const first = query.allBackAliasMembersExceptSegments();
    const second = query.allBackAliasMembersExceptSegments();
    const [alias] = Object.keys(first);
    const expected = second[alias];

    expect(alias).toBeDefined();
    expect(first).not.toBe(second);
    expect(Object.isFrozen(first)).toBe(false);

    first[alias] = 'mutated.alias';

    expect(second[alias]).toBe(expected);
    expect(query.allBackAliasMembersExceptSegments()[alias]).toBe(expected);
  });

  it('keeps the cache isolated to each query instance', () => {
    const firstQuery = new PostgresQuery(compilers, queryOptions());
    const secondQuery = new PostgresQuery(compilers, queryOptions());
    const firstBackAliasMembers = jest.spyOn(firstQuery, 'backAliasMembers');
    const secondBackAliasMembers = jest.spyOn(secondQuery, 'backAliasMembers');

    const first = (firstQuery as any).cachedAllBackAliasMembersExceptSegments();
    const second = (secondQuery as any).cachedAllBackAliasMembersExceptSegments();

    expect(first).toEqual(second);
    expect(first).not.toBe(second);
    expect((firstQuery as any).cachedAllBackAliasMembersExceptSegments()).toBe(first);
    expect((secondQuery as any).cachedAllBackAliasMembersExceptSegments()).toBe(second);
    expect(firstBackAliasMembers).toHaveBeenCalledTimes(1);
    expect(secondBackAliasMembers).toHaveBeenCalledTimes(1);
  });

  it('does not cache results collected before the join graph is ready', () => {
    const query = new PostgresQuery(compilers, queryOptions());
    const backAliasMembers = jest.spyOn(query, 'backAliasMembers');
    const { joinGraphPaths } = query;

    query.joinGraphPaths = undefined as any;
    const firstIntermediate = (query as any).cachedAllBackAliasMembersExceptSegments();
    const secondIntermediate = (query as any).cachedAllBackAliasMembersExceptSegments();

    expect(secondIntermediate).toEqual(firstIntermediate);
    expect(secondIntermediate).not.toBe(firstIntermediate);

    query.joinGraphPaths = joinGraphPaths;
    const completed = (query as any).cachedAllBackAliasMembersExceptSegments();

    expect((query as any).cachedAllBackAliasMembersExceptSegments()).toBe(completed);
    expect(backAliasMembers).toHaveBeenCalledTimes(3);
  });

  it('does not cache nested alias-gathering results', () => {
    const query = new PostgresQuery(compilers, queryOptions());
    const backAliasMembers = jest.spyOn(query, 'backAliasMembers');

    const firstNested = query.evaluateSymbolSqlWithContext(
      () => (query as any).cachedAllBackAliasMembersExceptSegments(),
      { aliasGathering: true }
    );
    const secondNested = query.evaluateSymbolSqlWithContext(
      () => (query as any).cachedAllBackAliasMembersExceptSegments(),
      { aliasGathering: true }
    );

    expect(secondNested).toEqual(firstNested);
    expect(secondNested).not.toBe(firstNested);

    const completed = (query as any).cachedAllBackAliasMembersExceptSegments();
    expect((query as any).cachedAllBackAliasMembersExceptSegments()).toBe(completed);

    const nestedAfterCache = query.evaluateSymbolSqlWithContext(
      () => (query as any).cachedAllBackAliasMembersExceptSegments(),
      { aliasGathering: true }
    );

    expect(nestedAfterCache).toEqual(completed);
    expect(nestedAfterCache).not.toBe(completed);
    expect((query as any).cachedAllBackAliasMembersExceptSegments()).toBe(completed);
    expect(backAliasMembers).toHaveBeenCalledTimes(4);
  });

  it('only stores successful alias collection results', () => {
    const query = new PostgresQuery(compilers, queryOptions());
    const originalBackAliasMembers = query.backAliasMembers.bind(query);
    const backAliasMembers = jest.spyOn(query, 'backAliasMembers')
      .mockImplementationOnce(() => {
        throw new Error('alias collection failed');
      })
      .mockImplementation(originalBackAliasMembers);

    expect(() => (query as any).cachedAllBackAliasMembersExceptSegments())
      .toThrow('alias collection failed');

    const completed = (query as any).cachedAllBackAliasMembersExceptSegments();
    expect((query as any).cachedAllBackAliasMembersExceptSegments()).toBe(completed);
    expect(backAliasMembers).toHaveBeenCalledTimes(2);
  });
});

describe('FILTER_PARAMS alias collection', () => {
  it('reuses one alias traversal without changing generated SQL', async () => {
    const dimensionCount = 20;
    const filterParamCount = 50;
    const dimensions = Array.from({ length: dimensionCount }, (_, index) => ({
      name: `dimension_${index}`,
      sql: `dimension_${index}`,
      type: 'string',
    }));
    const filterParams = Array.from(
      { length: filterParamCount },
      (_, index) => `{FILTER_PARAMS.orders.dimension_${index % dimensionCount}.filter('dimension_${index % dimensionCount}')}`
    );
    const schema = createSchemaYaml({
      cubes: [{
        name: 'orders',
        sql: `SELECT * FROM orders WHERE ${filterParams.join(' AND ')}`,
        measures: [{ name: 'count', type: 'count' }],
        dimensions,
      }],
      views: [{
        name: 'orders_view',
        cubes: [{
          join_path: 'orders',
          includes: [
            { name: 'count', alias: 'total_orders' },
            ...dimensions.map(({ name }, index) => ({
              name,
              alias: `aliased_dimension_${index}`,
            })),
          ],
        }],
      }],
    });
    const uncachedCompilers = prepareYamlCompiler(schema);
    const cachedCompilers = prepareYamlCompiler(schema);
    await Promise.all([
      uncachedCompilers.compiler.compile(),
      cachedCompilers.compiler.compile(),
    ]);
    const options = {
      measures: ['orders_view.total_orders'],
      dimensions: dimensions.map((_, index) => `orders_view.aliased_dimension_${index}`),
      filters: [{
        member: 'orders_view.aliased_dimension_0',
        operator: 'equals',
        values: ['completed'],
      }],
      timezone: 'UTC',
      useNativeSqlPlanner: false,
    };

    const uncachedQuery = new PostgresQuery(uncachedCompilers, options);
    const uncachedBackAliasMembers = jest.spyOn(uncachedQuery, 'backAliasMembers');
    uncachedQuery.allBackAliasMembersExceptSegments = function uncachedAliases() {
      return this.backAliasMembers(this.flattenAllMembers(true));
    };
    const expected = uncachedQuery.buildSqlAndParams();

    const cachedQuery = new PostgresQuery(cachedCompilers, options);
    const cachedAllBackAliases = jest.spyOn(cachedQuery as any, 'cachedAllBackAliasMembersExceptSegments');
    const backAliasMembers = jest.spyOn(cachedQuery, 'backAliasMembers');
    const actual = cachedQuery.buildSqlAndParams();
    const aliases = (cachedQuery as any).cachedAllBackAliasMembersExceptSegments();

    expect(actual).toEqual(expected);
    expect(Object.keys(aliases).length).toBeGreaterThan(0);
    expect(cachedAllBackAliases).toHaveBeenCalled();
    expect(backAliasMembers.mock.calls.length)
      .toBeLessThan(uncachedBackAliasMembers.mock.calls.length);
  });
});
