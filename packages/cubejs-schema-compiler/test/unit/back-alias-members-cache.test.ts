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

    const first = query.allBackAliasMembersExceptSegments();
    const second = query.allBackAliasMembersExceptSegments();

    expect(first).toEqual(expected);
    expect(Object.keys(first).length).toBeGreaterThan(0);
    expect(Object.isFrozen(first)).toBe(true);
    expect(second).toBe(first);
    expect(backAliasMembers).toHaveBeenCalledTimes(1);
  });

  it('keeps the cache isolated to each query instance', () => {
    const firstQuery = new PostgresQuery(compilers, queryOptions());
    const secondQuery = new PostgresQuery(compilers, queryOptions());
    const firstBackAliasMembers = jest.spyOn(firstQuery, 'backAliasMembers');
    const secondBackAliasMembers = jest.spyOn(secondQuery, 'backAliasMembers');

    const first = firstQuery.allBackAliasMembersExceptSegments();
    const second = secondQuery.allBackAliasMembersExceptSegments();

    expect(first).toEqual(second);
    expect(first).not.toBe(second);
    expect(firstQuery.allBackAliasMembersExceptSegments()).toBe(first);
    expect(secondQuery.allBackAliasMembersExceptSegments()).toBe(second);
    expect(firstBackAliasMembers).toHaveBeenCalledTimes(1);
    expect(secondBackAliasMembers).toHaveBeenCalledTimes(1);
  });

  it('does not cache results collected before the join graph is ready', () => {
    const query = new PostgresQuery(compilers, queryOptions());
    const backAliasMembers = jest.spyOn(query, 'backAliasMembers');
    const { joinGraphPaths } = query;

    query.joinGraphPaths = undefined as any;
    query.allBackAliasMembersExceptSegments();
    expect(query.allBackAliasMembersExceptSegmentsCache).toBeUndefined();

    query.joinGraphPaths = joinGraphPaths;
    const completed = query.allBackAliasMembersExceptSegments();

    expect(query.allBackAliasMembersExceptSegments()).toBe(completed);
    expect(backAliasMembers).toHaveBeenCalledTimes(2);
  });

  it('does not cache nested alias-gathering results', () => {
    const query = new PostgresQuery(compilers, queryOptions());
    const backAliasMembers = jest.spyOn(query, 'backAliasMembers');

    query.evaluateSymbolSqlWithContext(
      () => query.allBackAliasMembersExceptSegments(),
      { aliasGathering: true }
    );
    query.evaluateSymbolSqlWithContext(
      () => query.allBackAliasMembersExceptSegments(),
      { aliasGathering: true }
    );

    expect(query.allBackAliasMembersExceptSegmentsCache).toBeUndefined();

    const completed = query.allBackAliasMembersExceptSegments();
    expect(query.allBackAliasMembersExceptSegments()).toBe(completed);

    const nestedAfterCache = query.evaluateSymbolSqlWithContext(
      () => query.allBackAliasMembersExceptSegments(),
      { aliasGathering: true }
    );

    expect(nestedAfterCache).toEqual(completed);
    expect(nestedAfterCache).not.toBe(completed);
    expect(query.allBackAliasMembersExceptSegmentsCache).toBe(completed);
    expect(query.allBackAliasMembersExceptSegments()).toBe(completed);
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

    expect(() => query.allBackAliasMembersExceptSegments())
      .toThrow('alias collection failed');
    expect(query.allBackAliasMembersExceptSegmentsCache).toBeUndefined();

    const completed = query.allBackAliasMembersExceptSegments();
    expect(query.allBackAliasMembersExceptSegments()).toBe(completed);
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
    const compilers = prepareYamlCompiler(createSchemaYaml({
      cubes: [{
        name: 'orders',
        sql: `SELECT * FROM orders WHERE ${filterParams.join(' AND ')}`,
        measures: [{ name: 'count', type: 'count' }],
        dimensions,
      }],
    }));
    await compilers.compiler.compile();
    const options = {
      measures: ['orders.count'],
      dimensions: dimensions.map(({ name }) => `orders.${name}`),
      filters: [{
        member: 'orders.dimension_0',
        operator: 'equals',
        values: ['completed'],
      }],
      timezone: 'UTC',
    };

    const uncachedQuery = new PostgresQuery(compilers, options);
    uncachedQuery.allBackAliasMembersExceptSegments = function uncachedAliases() {
      return this.backAliasMembers(this.flattenAllMembers(true));
    };
    const expectedSql = uncachedQuery.cubeSql('orders');

    const cachedQuery = new PostgresQuery(compilers, options);
    const allBackAliases = jest.spyOn(cachedQuery, 'allBackAliasMembersExceptSegments');
    const backAliasMembers = jest.spyOn(cachedQuery, 'backAliasMembers');
    const actualSql = cachedQuery.cubeSql('orders');

    expect(actualSql).toBe(expectedSql);
    expect(allBackAliases).toHaveBeenCalledTimes(filterParamCount);
    expect(backAliasMembers).toHaveBeenCalledTimes(1);
  });
});
