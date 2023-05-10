cube(`Region`, {
  sql: `SELECT * FROM public.region`,
  
  preAggregations: {
    // Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started  
  },
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: `count`,
      drillMembers: [rName]
    }
  },
  
  dimensions: {
    rName: {
      sql: `r_name`,
      type: `string`
    },
    
    rComment: {
      sql: `r_comment`,
      type: `string`
    }
  },
  
  dataSource: `default`
});
