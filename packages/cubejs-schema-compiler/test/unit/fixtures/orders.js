cube('orders', {
  sql_table: 'public.orders',

  dimensions: {
    id: {
      sql: 'id',
      type: 'number',
      primary_key: true,
    },

    status: {
      sql: 'status',
      type: 'string',
    },

    created_at: {
      sql: 'created_at',
      type: 'time',
    },

    completed_at: {
      sql: 'completed_at',
      type: 'time',
    },
  },

  measures: {
    count: {
      type: 'count',
    },
  },

  hierarchies: {
    hello: {
      title: 'World',
      levels: [status],
    },
  },
});
