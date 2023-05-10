cube(`Nation`, {
  sql: `SELECT * FROM public.nation`,
  
  preAggregations: {
    // Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started  
  },
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: `count`,
      drillMembers: [nName]
    }
  },
  
  dimensions: {
    nName: {
      sql: `n_name`,
      type: `string`
    },
    
    nComment: {
      sql: `n_comment`,
      type: `string`
    }
  },
  
  dataSource: `default`
});
