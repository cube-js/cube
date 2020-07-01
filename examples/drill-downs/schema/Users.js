cube(`Users`, {
  sql: `SELECT * FROM public.users`,
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: `count`,
      drillMembers: [id, city, createdAt]
    }
  },
  
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },
    
    company: {
      sql: `company`,
      type: `string`
    },
    
    gender: {
      sql: `gender`,
      type: `string`
    },
    
    city: {
      sql: `city`,
      type: `string`
    },

    firstName: {
      sql: `first_name`,
      type: `string`
    },

    city: {
      sql: `city`,
      type: `string`
    },
    
    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  }
});
