cube(`Orders`, {
  sql: `
  select 1 as id, 100 as amount, 100.15 as decimal_amount, CAST(100.5 AS FLOAT) as float_amount, 'new' status
  UNION ALL
  select 2 as id, 200 as amount, 100.25 as decimal_amount, CAST(200.5 AS FLOAT) as float_amount, 'new' status
  UNION ALL
  select 3 as id, 300 as amount, 100.35 as decimal_amount, CAST(300.5 AS FLOAT) as float_amount, 'processed' status
  UNION ALL
  select 4 as id, 500 as amount, 100.45 as decimal_amount, CAST(500.5 AS FLOAT) as float_amount, 'processed' status
  UNION ALL
  select 5 as id, 600 as amount, 100.55 as decimal_amount, CAST(600.5 AS FLOAT) as float_amount, 'shipped' status
  `,
  measures: {
    count: {
      type: `count`,
    },
    totalAmount: {
      sql: `amount`,
      type: `sum`,
    },
    totalDecimalAmount: {
      sql: `decimal_amount`,
      type: `sum`,
    },
    totalFloatAmount: {
      sql: `float_amount`,
      type: `sum`,
    },
    toRemove: {
      type: `count`,
    },
  },
  dimensions: {
    status: {
      sql: `status`,
      type: `string`,
    },
  },

  preAggregations: {
    amount: {
      measures: [totalAmount, totalFloatAmount, totalDecimalAmount],
      dimensions: [status],
    }
  }
});
