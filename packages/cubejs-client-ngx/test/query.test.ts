import { Query } from '../src/query-builder/query';

import { createMeta } from './meta-fixture';

describe('Query', () => {
  let query: Query;

  beforeEach(() => {
    query = new Query(createMeta());
  });

  test('starts empty', () => {
    expect(query.asCubeQuery()).toEqual({});
    expect(query.isPresent()).toBe(false);
  });

  describe('members', () => {
    test('add/replace/set', () => {
      query.measures.add('Orders.count');
      expect(query.asCubeQuery().measures).toEqual(['Orders.count']);
      expect(query.isPresent()).toBe(true);

      query.measures.replace('Orders.count', 'Orders.totalAmount');
      expect(query.asCubeQuery().measures).toEqual(['Orders.totalAmount']);

      query.measures.set(['Orders.count', 'Orders.totalAmount']);
      expect(query.asCubeQuery().measures).toEqual([
        'Orders.count',
        'Orders.totalAmount',
      ]);
    });

    test('remove by name and by index', () => {
      query.dimensions.set(['Orders.status', 'Orders.createdAt']);

      query.dimensions.remove('Orders.status');
      expect(query.asCubeQuery().dimensions).toEqual(['Orders.createdAt']);

      query.dimensions.set(['Orders.status', 'Orders.createdAt']);
      query.dimensions.remove(0);
      expect(query.asCubeQuery().dimensions).toEqual(['Orders.createdAt']);
    });

    test('asArray resolves members against meta', () => {
      query.measures.add('Orders.count');

      expect(query.measures.asArray()).toEqual([
        expect.objectContaining({
          name: 'Orders.count',
          title: 'Orders Count',
        }),
      ]);
    });
  });

  describe('time dimensions', () => {
    test('add, granularity and date range', () => {
      query.timeDimensions.add('Orders.createdAt');
      expect(query.asCubeQuery().timeDimensions).toEqual([
        { dimension: 'Orders.createdAt' },
      ]);

      query.timeDimensions.setGranularity('Orders.createdAt', 'day');
      expect(query.timeDimensions.granularity).toBe('day');

      query.timeDimensions.setDateRange(0, 'last 30 days');
      expect(query.asCubeQuery().timeDimensions).toEqual([
        {
          dimension: 'Orders.createdAt',
          granularity: 'day',
          dateRange: 'last 30 days',
        },
      ]);

      query.timeDimensions.remove('Orders.createdAt');
      expect(query.asCubeQuery().timeDimensions).toEqual([]);
    });
  });

  describe('filters', () => {
    test('add, update, replace and remove', () => {
      query.filters.add({
        member: 'Orders.status',
        operator: 'equals',
        values: ['completed'],
      });
      expect(query.asCubeQuery().filters).toEqual([
        { member: 'Orders.status', operator: 'equals', values: ['completed'] },
      ]);

      query.filters.update('Orders.status', { values: ['shipped'] });
      expect(query.asCubeQuery().filters).toEqual([
        { member: 'Orders.status', operator: 'equals', values: ['shipped'] },
      ]);

      query.filters.replace('Orders.status', 'Orders.count');
      expect(query.asCubeQuery().filters).toEqual([
        { member: 'Orders.count', operator: 'equals', values: ['shipped'] },
      ]);

      query.filters.remove('Orders.count');
      expect(query.asCubeQuery().filters).toEqual([]);
    });
  });

  describe('order', () => {
    test('member order is reflected in the query', () => {
      query.measures.add('Orders.count');

      query.order.setMemberOrder('Orders.count', 'desc');
      expect(query.asCubeQuery().order).toEqual({ 'Orders.count': 'desc' });
      expect(query.order.of('Orders.count')).toBe('desc');
      expect(query.order.asObject()).toEqual({ 'Orders.count': 'desc' });

      query.order.setMemberOrder('Orders.count', 'none');
      expect(query.asCubeQuery().order).toEqual({});
      expect(query.order.of('Orders.count')).toBe('none');
    });
  });

  test('setLimit', () => {
    query.setLimit(100);
    expect(query.asCubeQuery().limit).toBe(100);
  });

  test('onBeforeChange callback rewrites the new query', () => {
    const onBeforeChange = jest.fn((newQuery) => ({ ...newQuery, limit: 10 }));
    const q = new Query(createMeta(), onBeforeChange);

    q.setQuery({ measures: ['Orders.count'] });

    expect(onBeforeChange).toHaveBeenCalled();
    expect(q.asCubeQuery()).toEqual({ measures: ['Orders.count'], limit: 10 });
  });
});
