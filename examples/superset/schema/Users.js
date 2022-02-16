cube(`Users`, {
  sql: `SELECT * FROM public.users`,
  
  // Copy me ↓
  preAggregations: {
    main: {
      measures: [ CUBE.count ],
      dimensions: [ CUBE.city, CUBE.gender ]
    }
  },
  // Copy me ↑
  
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
    
    state: {
      sql: `state`,
      type: `string`
    },
    
    company: {
      sql: `company`,
      type: `string`
    },
    
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },
    
    lastName: {
      sql: `last_name`,
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
    
    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  },
  
  dataSource: `default`
});
