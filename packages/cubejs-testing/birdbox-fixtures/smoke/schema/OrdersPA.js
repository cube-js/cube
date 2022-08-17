cube(`OrdersPA`, {
  sql: `
    select 1 as id, 100 as amount, 'new' status
    UNION ALL
    select 2 as id, 200 as amount, 'new' status
    UNION ALL
    select 3 as id, 300 as amount, 'processed' status
    UNION ALL
    select 4 as id, 500 as amount, 'processed' status
    UNION ALL
    select 5 as id, 600 as amount, 'shipped' status
  `,

  preAggregations: {
    ordersByStatus: {
      measures: [CUBE.count, CUBE.totalAmount],
      dimensions: [CUBE.status],
      refreshKey: {
        every: `1 hour`,
      }
    },
  },

  measures: {
    count: {
      type: `count`,
    },

    totalAmount: {
      sql: `amount`,
      type: `sum`,
    },
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
    },

    status: {
      sql: `status`,
      type: `string`,
    },
  },
});
