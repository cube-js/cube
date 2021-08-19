cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  measures: {
    count: {
      type: `count`,
      drillMembers: [id, createdAt]
    }
  },
  
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
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
