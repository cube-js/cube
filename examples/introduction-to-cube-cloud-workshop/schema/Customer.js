cube(`Customer`, {
  sql: `SELECT * FROM public.customer`,
  
  preAggregations: {
    // Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started  
  },
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: `count`,
      drillMembers: [cName]
    }
  },
  
  dimensions: {
    cPhone: {
      sql: `c_phone`,
      type: `string`
    },
    
    cName: {
      sql: `c_name`,
      type: `string`
    },
    
    cAddress: {
      sql: `c_address`,
      type: `string`
    },
    
    cMktsegment: {
      sql: `c_mktsegment`,
      type: `string`
    },
    
    cComment: {
      sql: `c_comment`,
      type: `string`
    },
    
    cAcctbal: {
      sql: `c_acctbal`,
      type: `string`
    }
  },
  
  dataSource: `default`
});
