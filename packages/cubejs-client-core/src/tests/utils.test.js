const { defaultOrder, orderMembersToOrder } = require('../utils');

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

  test('order members to order', () => {
    expect(orderMembersToOrder([{ id: 'Orders.count', order: 'asc' }], [])).toEqual({
      'Orders.count': 'asc',
    });

    expect(
      orderMembersToOrder(
        [
          {
            id: 'Orders.count',
            order: 'asc',
          },
        ],
        [
          {
            id: 'Orders.count',
            order: 'asc',
          },
          {
            id: 'Orders.number',
            order: 'desc',
          },
        ]
      )
    ).toEqual({
      'Orders.count': 'asc',
    });

    expect(
      orderMembersToOrder(
        [
          {
            id: 'Orders.count',
            order: 'none',
          },
        ],
        [
          {
            id: 'Orders.count',
            order: 'asc',
          },
          {
            id: 'Orders.number',
            order: 'desc',
          },
        ]
      )
    ).toEqual({});

    const current = [
      {
        id: 'Orders.count',
        order: 'desc',
      },
    ];
    const prev = [
      {
        id: 'Orders.count',
        order: 'asc',
      },
      {
        id: 'Orders.number',
        order: 'desc',
      },
    ];

    expect(orderMembersToOrder(current, prev)).toEqual({
      'Orders.count': 'desc',
    });

    console.log(' --> ', JSON.stringify( orderMembersToOrder(current, prev, 'array')))
    expect(orderMembersToOrder(current, prev, 'array')).toStrictEqual([['Orders.count', 'desc']]);
    
  });
});
