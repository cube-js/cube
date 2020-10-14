cube(`OrdersOffline`, {
  sql: `SELECT * FROM public.orders_offline`,

  measures: {
    count: {
      type: `count`,
    },
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
    },

    createdAt: {
      sql: `created_at`,
      type: `time`,
    },
  },
});
