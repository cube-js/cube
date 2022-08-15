cube('Orders', {
  sql: `
  select 1 as id, 100 as amount, '2017-01-01 09:34:21'::timestamp as created_at, 'new' status
  UNION ALL
  select 2 as id, 200 as amount, '2017-02-01 19:34:21'::timestamp as created_at, 'new' status
  UNION ALL
  select 3 as id, 300 as amount, '2017-01-01 19:34:21'::timestamp as created_at, 'processed' status
  UNION ALL
  select 4 as id, 500 as amount, '2017-01-01 19:34:21'::timestamp as created_at, 'processed' status
  UNION ALL
  select 5 as id, 600 as amount, '2017-01-01 19:34:21'::timestamp as created_at, 'shipped' status
  `,
  measures: {
    count: {
      type: 'count',
    },
    totalAmount: {
      sql: 'amount',
      type: 'sum',
    },
    toRemove: {
      type: 'count',
    },
  },
  dimensions: {
    status: {
      sql: 'status',
      type: 'string',
    },
    createdAt: {
      sql: 'created_at',
      type: 'time'
    }
  },
});
