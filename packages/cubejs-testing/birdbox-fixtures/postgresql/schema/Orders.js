cube(`Orders`, {
  sql: `
  select 1 as id, 100 as amount, 'new' status, '2024-01-01'::timestamptz created_at
  UNION ALL
  select 2 as id, 200 as amount, 'new' status, '2024-01-02'::timestamptz created_at
  UNION ALL
  select 3 as id, 300 as amount, 'processed' status, '2024-01-03'::timestamptz created_at
  UNION ALL
  select 4 as id, 500 as amount, 'processed' status, '2024-01-04'::timestamptz created_at
  UNION ALL
  select 5 as id, 600 as amount, 'shipped' status, '2024-01-05'::timestamptz created_at
  `,
  measures: {
    count: {
      type: `count`,
    },
    totalAmount: {
      sql: `amount`,
      type: `sum`,
    },
    toRemove: {
      type: `count`,
    },
    numberTotal: {
      sql: `${totalAmount}`,
      type: `number`
    },
    amountRank: {
      multi_stage: true,
      type: `rank`,
      order_by: [{
        sql: `${totalAmount}`,
        dir: 'asc'
      }],
      reduce_by: [status],
    },
    amountReducedByStatus: {
      multi_stage: true,
      type: `sum`,
      sql: `${totalAmount}`,
      reduce_by: [status],
    },
    statusPercentageOfTotal: {
      multi_stage: true,
      sql: `${totalAmount} / NULLIF(${amountReducedByStatus}, 0)`,
      type: `number`,
    },
    amountRankView: {
      multi_stage: true,
      type: `number`,
      sql: `${amountRank}`,
    },
    amountRankDateMax: {
      multi_stage: true,
      sql: `${createdAt}`,
      type: `max`,
      filters: [{
        sql: `${amountRank} = 1`
      }]
    },
    amountRankDate: {
      multi_stage: true,
      sql: `${amountRankDateMax}`,
      type: `time`,
    },
    countAndTotalAmount: {
      type: "string",
      sql: `CONCAT(${count}, ' / ', ${totalAmount})`,
    },
    createdAtMax: {
      type: `max`,
      sql: `created_at`,
    },
    createdAtMaxProxy: {
      type: "time",
      sql: `${createdAtMax}`,
    },
  },
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
      public: true,
    },

    status: {
      sql: `status`,
      type: `string`,
    },

    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  },
});

view(`OrdersView`, {
  cubes: [{
    joinPath: Orders,
    includes: `*`,
    excludes: [`toRemove`]
  }]
});
