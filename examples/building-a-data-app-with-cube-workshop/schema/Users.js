cube(`Users`, {
  sql: `SELECT * FROM public.users`,
  
  preAggregations: {
    // Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started  
  },
  
  joins: {
    /**
     * Demo 1
     * Joins
     * A "User" has many "Orders"
     */
    // Orders: {
    //   sql: `${CUBE}.id = ${Orders}.user_id`,
    //   relationship: `hasMany`
    // },
  },
  
  measures: {
    count: {
      type: `count`,
      drillMembers: [id, city, firstName, lastName, createdAt]
    }
  },
  
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },
    
    city: {
      sql: `city`,
      type: `string`
    },
    
    company: {
      sql: `company`,
      type: `string`
    },
    
    gender: {
      sql: `gender`,
      type: `string`
    },
    
    firstName: {
      sql: `first_name`,
      type: `string`
    },
    
    lastName: {
      sql: `last_name`,
      type: `string`
    },
    
    state: {
      sql: `state`,
      type: `string`
    },
    
    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  }
});
