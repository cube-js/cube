cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  dimensions: {
    status: {
      sql: `status`,
      type: `string`
    },
    
    number: {
      sql: `number`,
      type: `number`
    },
    
    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  }
});
