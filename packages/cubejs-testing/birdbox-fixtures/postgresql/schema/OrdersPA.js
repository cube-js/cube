cube(`OrdersPA`, {
  sql: `
    select 1 as id, 100 as amount, 'new' as status2, 1 as id2, 100 as amount2, 'new' as status2
    UNION ALL
    select 2 as id, 200 as amount, 'new' status, 2 as id2, 200 as amount2, 'new' as status2
    UNION ALL
    select 3 as id, 300 as amount, 'processed' as status, 3 as id2, 300 as amount2, 'processed' as status2
    UNION ALL
    select 4 as id, 500 as amount, 'processed' as status, 4 as id2, 500 as amount2, 'processed' as status2
    UNION ALL
    select 5 as id, 600 as amount, 'shipped' as status, 5 as id2, 600 as amount2, 'shipped' as status2
  `,

  preAggregations: {
    orderStatus: {
      measures: [CUBE.amount, CUBE.amount2],
      dimensions: [CUBE.id, CUBE.status, CUBE.id2, CUBE.status2],
      indexes: {
        categoryIndex: {
          columns: [CUBE.status, CUBE.status2],
        },
      },
      refreshKey: {
        every: `1 hour`,
      }
    },
  },

  measures: {
    amount: {
      sql: `amount`,
      type: `sum`,
    },
    amount2: {
      sql: `amount2`,
      type: `sum`,
    },
  },
  dimensions: {
    id: {
      sql: 'id',
      type: `string`,
    },
    status: {
      sql: `status`,
      type: `string`,
    },
    id2: {
      sql: 'id2',
      type: `string`,
    },
    status2: {
      sql: `status`,
      type: `string`,
    },
  },
});
