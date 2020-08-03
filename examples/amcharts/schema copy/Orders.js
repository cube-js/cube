cube(`Orders`, {
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

  measures: {
    count: {
      type: `count`
    },

    totalAmount: {
      sql: `amount`,
      type: `sum`
    }
  },

  dimensions: {
    status: {
      sql: `status`,
      type: `string`
    }
  }
});
