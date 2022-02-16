cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,
  
  // Copy me ↓
  preAggregations: {
    main: {
      measures: [ CUBE.count ],
      dimensions: [ CUBE.status ],
      timeDimension: CUBE.createdAt,
      granularity: 'day'
    }
  },
  // Copy me ↑

  measures: {
    count: {
      type: `count`
    },
    
    number: {
      sql: `number`,
      type: `sum`
    }
  },
  
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },
    
    status: {
      sql: `status`,
      type: `string`
    },
    
    createdAt: {
      sql: `created_at`,
      type: `time`
    },
    
    completedAt: {
      sql: `completed_at`,
      type: `time`
    }
  },
  
  dataSource: `default`
});
