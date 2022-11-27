cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  dimensions: {
    userId: {
      sql: `user_id`,
      type: `string`,
    },

    status: {
      sql: `status`,
      type: `string`
    },

    completedAt: {
      sql: `completed_at`,
      type: `time`
    },
  }
});
