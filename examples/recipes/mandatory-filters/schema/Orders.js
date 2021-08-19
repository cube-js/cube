cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  dimensions: {
    status: {
      sql: `status`,
      type: `string`
    },
    
    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  }
});
