cube(`Users`, {
  sql: `SELECT * FROM public.users`,
  
  measures: {
    count: {
      type: `count`
    }
  },
  
  dimensions: {
    firstName: {
      sql: `first_name`,
      type: `string`
    },
    
    lastName: {
      sql: `last_name`,
      type: `string`
    },
    
    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  }
});
