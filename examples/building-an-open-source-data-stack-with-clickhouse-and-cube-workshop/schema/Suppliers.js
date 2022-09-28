// replace with ontime schema for MySQL once we get the endpoint
cube(`Suppliers`, {
  sql: `SELECT * FROM public.suppliers`,
  dataSource: `mysql`,
  
  preAggregations: {
    // Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started  
  },
  
  joins: {
    
  },
  
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
      primaryKey: true,
      shown: true
    },
    
    address: {
      sql: `address`,
      type: `string`
    },
    
    email: {
      sql: `email`,
      type: `string`
    },
    
    company: {
      sql: `company`,
      type: `string`
    },
    
    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  }
});
