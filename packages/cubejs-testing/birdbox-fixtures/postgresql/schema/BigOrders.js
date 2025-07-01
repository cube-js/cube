cube(`BigOrders`, {
  sql: `
  select * from (
  select 1 as id, 100 as amount, 'new' status, '2024-01-01'::timestamptz created_at
  UNION ALL
  select 2 as id, 200 as amount, 'new' status, '2024-01-02'::timestamptz created_at
  UNION ALL
  select 3 as id, 300 as amount, 'processed' status, '2024-01-03'::timestamptz created_at
  UNION ALL
  select 4 as id, 500 as amount, 'processed' status, '2024-01-04'::timestamptz created_at
  UNION ALL
  select 5 as id, 600 as amount, 'shipped' status, '2024-01-05'::timestamptz created_at
  ) data
  CROSS JOIN GENERATE_SERIES(1, 20000) value
  `,
  measures: {
    totalAmount: {
      sql: `amount`,
      type: `sum`,
    },
    toRemove: {
      type: `count`,
    },
  },
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
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
