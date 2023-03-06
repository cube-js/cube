cube('Orders', {
  sql: `
    select 1 as id, 100 as amount, 'new' status, TIMESTAMP '2022-12-05 06:00:00' created_at
    UNION ALL
    select 2 as id, 200 as amount, 'new' status, TIMESTAMP '2022-12-05 06:15:00' created_at
    UNION ALL
    select 3 as id, 300 as amount, 'processed' status, TIMESTAMP '2022-12-05 08:00:00' created_at
    UNION ALL
    select 4 as id, 500 as amount, 'processed' status, TIMESTAMP '2022-12-05 09:00:00' created_at
    UNION ALL
    select 5 as id, 600 as amount, 'shipped' status, TIMESTAMP '2022-12-05 10:00:00' created_at
    UNION ALL
    select 6 as id, 700 as amount, 'cancelled_by_customer' status, TIMESTAMP '2022-12-05 11:00:00' created_at
    `,
  measures: {
    count: {
      type: 'count',
    },
    totalAmount: {
      sql: 'amount',
      type: 'sum',
    },
  },
  dimensions: {
    status: {
      sql: 'status',
      type: 'string',
    },
    createdAt: {
      sql: 'created_at',
      type: 'time',
    },
  },
  preAggregations: {
    orderStatus: {
      measures: [CUBE.count, CUBE.totalAmount],
      dimensions: [CUBE.status],
      refreshKey: {
        every: '1 second',
      }
    },
  },
});
