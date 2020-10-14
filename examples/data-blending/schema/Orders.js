cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

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

    status: {
      sql: `status`,
      type: `string`,
    },

    createdAt: {
      sql: `created_at`,
      type: `time`,
    },
  },
});
