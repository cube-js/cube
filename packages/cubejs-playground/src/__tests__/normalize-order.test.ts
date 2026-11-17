import { normalizeQueryOrder } from '../QueryBuilderV2/utils/normalize-order';

describe('normalizeQueryOrder', () => {
  it('converts object order to array form, preserving member order', () => {
    const query = { measures: ['Orders.count'], order: { 'Orders.count': 'desc', 'Orders.status': 'asc' } };

    expect(normalizeQueryOrder(query).order).toEqual([
      ['Orders.count', 'desc'],
      ['Orders.status', 'asc'],
    ]);
    // input untouched
    expect(query.order).toEqual({ 'Orders.count': 'desc', 'Orders.status': 'asc' });
  });

  it('leaves array order and missing order unchanged', () => {
    const arr = { order: [['A.b', 'asc']] };
    const none = { measures: ['A.b'] };

    expect(normalizeQueryOrder(arr)).toBe(arr);
    expect(normalizeQueryOrder(none)).toBe(none);
  });
});
