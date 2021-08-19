cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,
  
  measures: {
    count: {
      type: `count`,
    }
  },
  
  dimensions: {
    status: {
      sql: `status`,
      type: `string`
    }
  }
});
