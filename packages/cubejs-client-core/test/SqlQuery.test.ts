/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview SqlQuery class unit tests.
 */

/* globals describe,it,expect */

import SqlQuery, { SqlQueryTuple, SqlData, SqlQueryWrapper } from '../src/SqlQuery';

describe('SqlQuery', () => {
  const mockCacheKeyQueriesTuple: SqlQueryTuple = [
    'SELECT FLOOR((-25200 + EXTRACT(EPOCH FROM NOW())) / 600) as refresh_key',
    [],
    {
      external: false,
      renewalThreshold: 60
    }
  ];

  const mockSqlTuple: SqlQueryTuple = [
    'SELECT count(*) "base_orders__count" FROM base_orders WHERE base_orders.continent = ?',
    ['Europe'],
  ];

  const mockSqlData: SqlData = {
    aliasNameToMember: { base_orders__count: 'base_orders.count' },
    cacheKeyQueries: [mockCacheKeyQueriesTuple],
    dataSource: 'default',
    external: false,
    sql: mockSqlTuple,
    preAggregations: [],
    rollupMatchResults: [],
  };

  const mockWrapper: SqlQueryWrapper = {
    sql: mockSqlData,
  };

  it('should construct without error', () => {
    expect(() => new SqlQuery(mockWrapper)).not.toThrow();
  });

  it('rawQuery should return the original SqlData', () => {
    const query = new SqlQuery(mockWrapper);
    expect(query.rawQuery()).toEqual(mockSqlData);
  });

  it('sql should return the first element (SQL string) from the sql tuple', () => {
    const query = new SqlQuery(mockWrapper);
    expect(query.sql()).toBe('SELECT count(*) "base_orders__count" FROM base_orders WHERE base_orders.continent = ?');
  });
});
