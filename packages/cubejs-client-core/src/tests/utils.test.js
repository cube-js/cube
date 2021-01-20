const { defaultOrder } = require('../utils');

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
    'Orders.createdAtf': 'asc',
  });
});
