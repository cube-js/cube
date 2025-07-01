cube(`Orders`, {
  sql: `
  select id, amount, status, created_at, is_paid from (
    select 1 as id, 100 as amount, 'new' status, TIMESTAMPTZ '2020-01-01 00:00:00' created_at, TRUE as is_paid
    UNION ALL
    select 2 as id, 200 as amount, 'new' status, TIMESTAMPTZ '2020-01-02 00:00:00' created_at, TRUE as is_paid
    UNION ALL
    select 3 as id, 300 as amount, 'processed' status, TIMESTAMPTZ '2020-01-03 00:00:00' created_at, TRUE as is_paid
    UNION ALL
    select 4 as id, 500 as amount, 'processed' status, TIMESTAMPTZ '2020-01-04 00:00:00' created_at, FALSE as is_paid
    UNION ALL
    select 5 as id, 600 as amount, 'shipped' status, TIMESTAMPTZ '2020-01-05 00:00:00' created_at, FALSE as is_paid
  )
  `,
  measures: {
    count: {
      type: `count`,
    },
    totalAmount: {
      sql: `amount`,
      type: `sum`,
    },
    avgAmount: {
      sql: `amount`,
      type: `avg`,
    },
    statusList: {
      sql: `status`,
      type: `string`,
    },
    lastStatus: {
      sql: `MAX(status)`,
      type: `string`,
    },
    hasUnpaidOrders: {
      sql: `BOOL_OR(NOT is_paid)`,
      type: `boolean`,
    },
    maxCreatedAt: {
      sql: `MAX(created_at)`,
      type: `time`,
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
    amount: {
      sql: `amount`,
      type: `number`,
    },
    isPaid: {
      sql: `is_paid`,
      type: `boolean`,
    },
    createdAt: {
      sql: `created_at`,
      type: `time`,
    },
  },
});
