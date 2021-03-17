const { defaultOrder } = require('../utils');

describe('order', () => {
  test('default order', () => {
    const query = {
      measures: ['Orders.count'],
      timeDimensions: [
        {
          dimension: 'Orders.createdAt',
          granularity: 'day',
        },
      ],
    };
    expect(defaultOrder(query)).toStrictEqual({
      'Orders.createdAt': 'asc',
    });
  });
});
