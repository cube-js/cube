cube(`Suppliers`, {
  sql: `SELECT * FROM public.suppliers`,
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: `count`,
      drillMembers: [id, createdAt]
    }
  },
  
  dimensions: {
    address: {
      sql: `address`,
      type: `string`
    },
    
    company: {
      sql: `company`,
      type: `string`
    },
    
    email: {
      sql: `email`,
      type: `string`
    },
    
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },
    
    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  }
});
