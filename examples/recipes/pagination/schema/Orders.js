cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  measures: {
    count: {
      type: `count`
    }
  },
  
  dimensions: {
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
