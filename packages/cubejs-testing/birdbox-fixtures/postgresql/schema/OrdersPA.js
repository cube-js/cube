cube(`OrdersPA`, {
  sql: `
    select 1 as id2, 100.1 as amount2, 'new' as status2, 1 as id, 100.1 as amount, 'new' as status
    UNION ALL
    select 2 as id2, 200.2 as amount2, 'new' as status2, 2 as id, 200.2 as amount, 'new' as status
    UNION ALL
    select 3 as id2, 300.3 as amount2, 'processed' as status2, 3 as id, 300.3 as amount, 'processed' as status
    UNION ALL
    select 4 as id2, 500.5 as amount2, 'processed' as status2, 4 as id, 500.5 as amount, 'processed' as status
    UNION ALL
    select 5 as id2, 600.6 as amount2, 'shipped' as status2, 5 as id, 600.6 as amount, 'shipped' as status
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
      sql: `id`,
      type: `number`,
    },
    status: {
      sql: `status`,
      type: `string`,
    },
    id2: {
      sql: `id2`,
      type: `number`,
    },
    status2: {
      sql: `status`,
      type: `string`,
    },
  },
});
